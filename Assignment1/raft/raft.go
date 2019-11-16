package raft

import "sync"
import "labrpc"
import "bytes"
import "encoding/gob"
import "time"
import "math/rand"
import "fmt"

const(
	HEARTBEAT = 50
	Follower = iota
	Candidate
	Leader
)

type Entry struct {
    Command interface{}
    Term    int
    Index 	int
}

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}



type Raft struct {
	mu        	sync.Mutex
	peers     	[]*labrpc.ClientEnd
	persister 	*Persister
	me 			int // index into peers[]
	
	state       int
	voteCount   int

	//persistent
	votedFor	int
	currentTerm	int
	log 		[]Entry//first index is 1

	//volatile
	commitIndex	int//已知的最大的已经被提交的日志条目的索引值
	lastApplied	int//最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）

	//leader only
	nextIndex	[]int//对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	matchIndex	[]int//对于每一个服务器，已经复制给他的日志的最高索引值

	voteCh		chan bool//投票信息接受通道
	appendCh	chan bool//管理信息接收通道
	applyCh     chan ApplyMsg//这应该是外部传入指令的通道
	electionTimer*		time.Timer//election计时器
	electionTimeout		time.Duration
	heartbeatInterval	time.Duration


}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.voteCount = 0

	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{})

	rf.commitIndex = 0
	rf.lastApplied = 0
	
	//rf.voteCount = 0
	rf.voteCh = make(chan bool, 1)
	rf.appendCh = make(chan bool, 1)
	rf.applyCh = applyCh

	rf.randomElectionTime()//生成随机的electionTime
	rf.heartbeatInterval = time.Duration(HEARTBEAT) * time.Millisecond//初始化固定的heartbetInterval

	rf.readPersist(persister.ReadRaftState())

	go rf.activate()

	return rf
}
func(rf *Raft) randomElectionTime(){
	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	//rf.electionTimeout = time.Millisecond*time.Duration(r.Int63n(200)+300)
	rf.electionTimeout = time.Millisecond*time.Duration(2 * HEARTBEAT + rand.Intn(HEARTBEAT))
	rf.electionTimer = time.NewTimer(rf.electionTimeout)
}

func (rf *Raft) updateTo(state int)  {
	if rf.state == state {
		return
	}
	if state == Follower {
		rf.state = Follower
		rf.votedFor = -1
		rf.voteCount = 0
	} else if state == Candidate {
		rf.state = Candidate
	} else if state == Leader {
		rf.state = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
		}
	}
}

func (rf *Raft) activate( ){
	for {
		rf.mu.Lock()
		st := rf.state
		rf.mu.Unlock()
		switch st {
		case Follower:
			select{
			case <-rf.appendCh:
				rf.electionTimer.Reset(rf.electionTimeout)
			case <-rf.voteCh:
				rf.electionTimer.Reset(rf.electionTimeout)
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				rf.updateTo(Candidate)
				rf.startElection()
				rf.mu.Unlock()
			}
		case Candidate:
			select {
			case <-rf.appendCh:
				rf.electionTimer.Reset(rf.electionTimeout)
				rf.mu.Lock()
				rf.updateTo(Follower)
				rf.mu.Unlock()
			case <-rf.voteCh:
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				rf.startElection()
				rf.mu.Unlock()
			}
		case Leader:
			rf.startAppend()
			time.Sleep(rf.heartbeatInterval)
		}
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm += 1
	rf.votedFor = rf.me		//vote for self
	rf.voteCount = 1
	rf.randomElectionTime()

	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getLastLogIndex(),
		rf.getLastLogTerm(),
	}	

	reply := &RequestVoteReply{}
	
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(server int) {
			//	fmt.Printf("raft%v is sending RequestVote RPC to raft%v\n", rf.me, server)
				if rf.state == Candidate && rf.sendRequestVote(server, args, reply){
					if reply.VoteGranted {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						rf.voteCount += 1
						if rf.voteCount > len(rf.peers)/2{
							rf.updateTo(Leader)
							return
						}
					} else if reply.Term > rf.currentTerm{
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.updateTo(Follower)
						rf.mu.Unlock()
						return						
					}
				} 
			}(i)
		}
	}
}

func(rf *Raft) startAppend(){

	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
	}
	reply := AppendEntriesReply{}

	for i, _ := range rf.peers {
				if i != rf.me {
					go func(server int) {
						rf.sendAppendEntries(server, args, &reply)
						if reply.Term > rf.currentTerm {
							rf.mu.Lock()
							rf.currentTerm = reply.Term
							rf.updateTo(Follower)
							rf.mu.Unlock()
						}
					}(i)
				}
			}

}

func (rf *Raft) getPrevLogIndex(idx int) int {
	return rf.nextIndex[idx] - 1
}

func (rf *Raft) getPrevLogTerm(idx int) int {
	prevLogIndex := rf.getPrevLogIndex(idx)
	if prevLogIndex == 0 {
		return -1
	} else {
		return rf.log[prevLogIndex].Term
	}
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	lastLogIndex := rf.getLastLogIndex()
	if lastLogIndex == 0 {
		return -1
	} else {
		return rf.log[lastLogIndex].Term
	}
}

type AppendEntriesArgs struct {
	// Your data here.
	Term 			int
	LeaderId		int
	//PreLogIndex		int //新的日志条目之前的索引值
	//PreLogTerm		int //prevLogIndex 条目的任期号
	//Entries 		[]Entry
	//LeaderCommit	int //领导人已经提交的日志的索引值
}

type AppendEntriesReply struct {
	Term 		int
	Success 	bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer dropAndSet(rf.appendCh)

	if args.Term < rf.currentTerm{
		reply.Success = false
		reply.Term = rf.currentTerm
	} else if args.Term > rf.currentTerm{
		rf.updateTo(Follower)
		rf.currentTerm = args.Term
		reply.Term = args.Term
		reply.Success = true
	} else{
		reply.Success = true
	}	

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type RequestVoteArgs struct {
	// Your data here.
	Term 			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

type RequestVoteReply struct {
	// Your data here.
	Term 		int
	VoteGranted	bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.persist()
	defer dropAndSet(rf.voteCh)
	defer rf.mu.Unlock()

	//比较候选人任期是否过时
	if  args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.VoteGranted =false
		return
	}

	//vote := false
	vote := true

	//比较日志新旧，只有候选人日志够新才可能给他投票
	//if args.LastLogTerm > rf.log[len(rf.log)-1].Term || 
	//	(args.LastLogTerm == rf.log[len(rf.log)-1].Term && 
	//		args.LastLogIndex > rf.log[len(rf.log)-1].Index){
	//			vote = true
	//}
	
	//如果候选人任期比当前节点新，则当前节点转为跟随者，若还能投票则投
	if  args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.updateTo(Follower)
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

	fmt.Printf("fail")
}


func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


func dropAndSet(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	term = rf.currentTerm
	if rf.state == Leader{
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true


	return index, term, isLeader
}

func (rf *Raft) Kill() {
	// Your code here, if desired.
}
