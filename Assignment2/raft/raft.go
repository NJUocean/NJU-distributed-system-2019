package raft

import "sync"
import "labrpc"
import "bytes"
import "encoding/gob"
import "time"
import "math/rand"

/********************const and struct********************/
const (
	HEARTBEAT = 50
	LEADER = iota
	CANDIDATE
	FOLLOWER
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	LogIndex   int
	LogTerm    int
	LogCommand interface{}
}

type Raft struct {
	mu            sync.Mutex
	peers         []*labrpc.ClientEnd
	persister     *Persister
	me            int // index into peers[]

	//在所有服务器上持久存在
	state         int
	voteCount     int
	currentTerm   int 
	votedFor      int  //default：-1
	log           []LogEntry //第一个log为空，下标从1开始

	//在所有服务器上不稳定存在
	commitIndex   int
	lastApplied   int

	//在leader上不稳定存在
	nextIndex     []int  //记录每个服务器待发送的日志index(default:leader最后一条log的index+1
	matchIndex    []int //记录每个服务器已复制最新日志的index(default：0

	//通信通道
	commitCh      chan bool
	appendCh      chan bool
	voteCh        chan bool
	leaderCh     	chan bool
	applyCh       chan ApplyMsg	

	//心跳间隔
	heartbeatInterval	time.Duration
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex  int
	LastLogTerm int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int  
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int //当添加日志发生冲突时，follower返回的修正后的待传日志index
}

/********************辅助函数********************/
//外界传入日志
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := 1
	isLeader := false	
	term,isLeader = rf.GetState()
	if isLeader{
		index = rf.getLastLogIndex() + 1
		entry := LogEntry{
			index,
    		term,
    		command,
		}
		rf.log = append(rf.log, entry)
		rf.persist()
	}
	return index, term, isLeader
}

func (rf *Raft) GetState() (int, bool) {
	term := rf.currentTerm
	isleader := false
	if rf.state == LEADER{
		isleader = true
	}
	return term, isleader
}
//获取随机时间
func(rf *Raft) randomElectionTime() time.Duration {
	return time.Millisecond*time.Duration(2 * HEARTBEAT + rand.Intn(HEARTBEAT))
}

//状态转移，并进行初始化，默认进行persist
func (rf *Raft) updateTo(state int)  {
	if state == FOLLOWER {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.voteCount = 0
	} else if state == CANDIDATE {
		rf.state = CANDIDATE
	} else if state == LEADER {
		rf.state = LEADER
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers{
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
			rf.matchIndex[i] = 0
		}
	}
	rf.persist()
}

func (rf *Raft) getPrevLogIndex(idx int) int {
	return rf.nextIndex[idx] - 1
}

func (rf *Raft) getPrevLogTerm(idx int) int {
	prevLogIndex := rf.getPrevLogIndex(idx)
	return rf.log[prevLogIndex].LogTerm
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log) - 1].LogIndex
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log) - 1].LogTerm
}
//通道通信
func dropAndSet(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

/********************主逻辑********************/

func Make(peers []*labrpc.ClientEnd, me int,persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0})
	rf.currentTerm = 0
	rf.commitCh = make(chan bool, 1)
	rf.appendCh = make(chan bool, 1)
	rf.voteCh = make(chan bool, 1)
	rf.leaderCh = make(chan bool, 1)
	rf.applyCh = applyCh
	rf.heartbeatInterval = time.Duration(HEARTBEAT) * time.Millisecond//初始化固定的heartbetInterval

	rf.readPersist(persister.ReadRaftState())
	go rf.Activate() //处理election heartbeat/appendentries
	go rf.Commit(rf.applyCh) //处理commit
	return rf
}

func (rf *Raft) Activate(){
	for {
		switch rf.state {
		case LEADER:
			rf.startAppend()
			time.Sleep(rf.heartbeatInterval)
		case FOLLOWER:
			select {
			case <-time.After(rf.randomElectionTime()):
				rf.mu.Lock()
				rf.updateTo(CANDIDATE)
				rf.mu.Unlock()
			case <-rf.appendCh:
			case <-rf.voteCh:				
			}
		case CANDIDATE:
			rf.mu.Lock()
			rf.currentTerm++     
			rf.votedFor = rf.me  
			rf.voteCount = 1     
			rf.persist() 
			rf.mu.Unlock()
			rf.startElection() 
			select {
			case <-time.After(rf.randomElectionTime()):
			case <-rf.appendCh:
				rf.mu.Lock()
				rf.updateTo(FOLLOWER)
				rf.mu.Unlock()
			case <-rf.leaderCh:
				rf.mu.Lock()
				rf.updateTo(LEADER)
				rf.mu.Unlock()
			}
		}
	}
}
//收到commit信息后加锁pply所有新的日志a
func (rf *Raft) Commit(applyCh chan ApplyMsg){
	for {
		select {
		case <-rf.commitCh:
			rf.mu.Lock()
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{Index: i, Command: rf.log[i].LogCommand}
				applyCh <- msg			
			}
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()
		}
	}
}

/********************Election********************/

func (rf *Raft) startElection() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getLastLogIndex(),
		rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == CANDIDATE {
			go func(i int) {	
				reply := &RequestVoteReply{}			
				rf.sendRequestVote(i, args, reply)
			}(i)
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.state == CANDIDATE {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.updateTo(FOLLOWER)
		}
		if reply.VoteGranted {
			rf.voteCount++
			if rf.state == CANDIDATE && rf.voteCount > len(rf.peers) / 2 {
				//rf.updateTo(FOLLOWER)
				//rf.persist()
				dropAndSet(rf.leaderCh)
			}
		}
	}
	return ok
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer dropAndSet(rf.voteCh)
	defer rf.mu.Unlock()

	//比较候选人任期是否过时
	if  args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.VoteGranted =false
		return
	}

	vote := false

	//比较日志新旧，只有候选人日志够新才可能给他投票
	if args.LastLogTerm > rf.getLastLogTerm() || 
		(args.LastLogTerm == rf.getLastLogTerm() && 
			args.LastLogIndex >= rf.getLastLogIndex()){
				vote = true
	}
	
	//如果候选人任期比当前节点新，则当前节点转为跟随者，若还能投票则投
	if  args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.updateTo(FOLLOWER)
		if vote{
			rf.votedFor = args.CandidateId
		}
		reply.Term = args.Term
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		return
	}

	//如果候选人任期和当前节点一样
	if  args.Term == rf.currentTerm {
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId)&& vote {			
				rf.votedFor = args.CandidateId
		}
		reply.Term = args.Term
		reply.VoteGranted = (rf.votedFor == args.CandidateId)

		return
	}

}

/********************AppendEntries********************/
func (rf *Raft) startAppend() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	flag:=false

	//比较是否有新的日志被复制到大多数节点
	for i := rf.commitIndex + 1; i <= rf.getLastLogIndex(); i++ {
		sum := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].LogTerm == rf.currentTerm {
				sum++
			}
		}
		if sum > len(rf.peers)/2 {
			rf.commitIndex = i
			flag = true
		}
	}
	if flag {
		dropAndSet(rf.commitCh)
	}
	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER {

			if rf.nextIndex[i] > 0 {
			go func(i int) {
					next := rf.nextIndex[i]
					entries := make([]LogEntry, 0)
					entries = append(entries,rf.log[next:]...)

					args := AppendEntriesArgs{
						rf.currentTerm,
						rf.me,
						rf.getPrevLogIndex(i),
						rf.getPrevLogTerm(i),
						entries,
						rf.commitIndex,
					}
					reply := &AppendEntriesReply{}
					rf.sendAppendEntries(i, args, reply)

				}(i)
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.state == LEADER {
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.matchIndex[server] = args.Entries[len(args.Entries) - 1].LogIndex
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.updateTo(FOLLOWER)
				return ok
			}
			rf.nextIndex[server] = reply.NextIndex
		}
	}
	return ok
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	

	reply.Success = false


	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		return
	}else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.updateTo(FOLLOWER)
	}
	dropAndSet(rf.appendCh)
	reply.Term = args.Term

	//首先确保args.prevLogIndex不大rf.log最后一个log的index，这样才可以比较prev是否相同
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextIndex = rf.getLastLogIndex() + 1
		return
	}else if args.PrevLogIndex > 0 {
		if args.PrevLogTerm != rf.log[args.PrevLogIndex].LogTerm{
			//当index能对齐后，逐次排除rf.logterm和args.PrevLogTerm不同的log，每一次ApeendEntries调用排除一个term
			for i:=args.PrevLogIndex-1;i>=0;i--{
				if rf.log[i].LogTerm != rf.log[args.PrevLogIndex].LogTerm{
					reply.NextIndex = i+1
					break
				}
			}
			return
		}
	}

	//程序能运行到这，说明arg.PrevLogTerm,arg.PrevLogIndex都和rf.log最后一个log的term和index对齐，可以append
	if args.PrevLogIndex >= 0 {
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log,args.Entries...)
		reply.Success = true
	}

	//如果leader的commiindex大于自己的，则将commitindex设置为min（args.leadercommit，rf.commitindex),并进行commit
	if args.LeaderCommit > rf.commitIndex{
		if args.LeaderCommit > rf.getLastLogIndex(){
			rf.commitIndex = rf.getLastLogIndex()
		}else{
			rf.commitIndex = args.LeaderCommit
		}
		dropAndSet(rf.commitCh)
	}

}






