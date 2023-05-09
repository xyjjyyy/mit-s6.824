package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

const (
	heartBeatTimeOut = 200
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role Role

	log []Entry

	leaderId    int
	votedFor    int
	votes       int // 选票
	currentTerm int

	nextIndex  []int
	matchIndex []int

	commitIndex int
	lastApplied int

	applyMsg chan ApplyMsg // 提交信息

	done           chan struct{} // 选举成功信号
	heartBeatCount int
	heartBeat      chan struct{}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs  structure.
type RequestVoteArgs struct {
	Term         int // 任期
	CandidateId  int // 候选ID
	LastLogIndex int // 候选人的最新一条日志条目的索引值
	LastLogTerm  int // 候选人的最新一条日志条目的任期号
}

// RequestVoteReply  structure.
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool // 是否同意投票给候选者
	Term        int  // 任期
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term > rf.currentTerm ||
		(args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) {
		rf.currentTerm = args.Term
	} else {
		return
	}

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	if args.LastLogTerm > lastLogTerm || (args.LastLogIndex >= lastLogIndex && args.LastLogTerm == lastLogTerm) {
		rf.role = Follower
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.heartBeat <- struct{}{}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []Entry // copy log entries
	LeaderCommit int     // leader's commitIndex
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries send heartBeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	defer rf.mu.Unlock()

	log.Printf("peers [%d] term [%d] : receive from [%d], term :[%d]\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	log.Printf("rf:%#v \n args: %#v\n", rf.log, args)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false

		return
	}

	rf.currentTerm = args.Term
	rf.leaderId = args.LeaderId
	rf.role = Follower
	// renew heartbeat
	// 有两个作用：
	// 如果为leader，失去leader地位，停止发送心跳信息
	// 如果为candidate，停止竞选
	rf.heartBeat <- struct{}{}

	reply.Term = args.Term

	if args.PreLogIndex < len(rf.log) {
		e := rf.log[args.PreLogIndex]
		if args.PreLogTerm == e.Term {
			rf.log = append(rf.log[:e.Index+1], args.Entries...)
			reply.Success = true
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = MinAB(len(rf.log)-1, args.LeaderCommit)
			}
			return
		}
	}

	reply.Success = false
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.role == Leader

	if isLeader {
		index = len(rf.log)
		term = rf.currentTerm
		rf.log = append(rf.log, Entry{Index: index, Term: term, Command: command})
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

		log.Printf("Start: index:%d,term:%d,entry:%v\n", index, term, rf.log[index])
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.

		_, isLeader := rf.GetState()
		// this node is leader, will send rpc to maintain authority
		if isLeader {
			log.Printf("Leader[%d]: startAppendEntries\n", rf.me)
			rf.startAppendEntries()
		}

		// this node is not leader,will wait leader heartbeat(appendEntries)
		if !rf.heartbeat() {

			// start election
			log.Printf("%d start election\n", rf.me)
			rf.mu.Lock()
			rf.role = Candidate
			rf.mu.Unlock()
			rf.startElection()

		}
	}
}

func (rf *Raft) sendApplyMsg() {
	for {
		rf.mu.Lock()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		entries := append([]Entry{}, rf.log[rf.lastApplied+1:rf.commitIndex+1]...)
		rf.mu.Unlock()

		if commitIndex < lastApplied {
			log.Println("commitIndex 不可能小于 lastApplied")
			return
		}
		if commitIndex == lastApplied {
			time.Sleep(150 * time.Millisecond)
			continue
		}

		log.Printf("sendApplyMsg peer[%d]: %#v\n", rf.me, entries)
		for _, entry := range entries {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			rf.applyMsg <- msg

			rf.mu.Lock()
			if rf.lastApplied < msg.CommandIndex {
				rf.lastApplied = msg.CommandIndex
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startAppendEntries() {

	for {
		rf.mu.Lock()
		role := rf.role
		rf.heartBeatCount = 1
		rf.mu.Unlock()

		if role != Leader {
			return
		}

		l := len(rf.peers)
		q := make([]chan struct{}, l)
		for i := 0; i < l; i++ {
			q[i] = make(chan struct{}, 1)
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me || len(q[i]) == 1 {
				continue
			}
			rf.mu.Lock()
			preLogIndex := rf.nextIndex[i] - 1
			entries := make([]Entry, len(rf.log)-preLogIndex-1)
			copy(entries, rf.log[preLogIndex+1:])
			entriesArgs := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PreLogIndex:  preLogIndex,
				Entries:      entries,
				PreLogTerm:   rf.log[preLogIndex].Term,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			q[i] <- struct{}{}

			entriesReply := AppendEntriesReply{}
			go func(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
				ok := rf.sendAppendEntries(server, &args, &reply)
				<-q[server]
				if !ok {
					log.Printf("Leader[%d]-term<%d>: sendAppendEntries to %d failed!", args.LeaderId, args.Term, server)
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if !reply.Success {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = Follower
					} else {
						rf.nextIndex[server]--
					}
					return
				}

				rf.matchIndex[server] = len(rf.log) - 1
				rf.nextIndex[server] = len(rf.log)
				rf.heartBeatCount++
				if rf.heartBeatCount > len(rf.peers)/2 {
					rf.commitIndex = len(rf.log) - 1
					log.Printf("Leader[%d]-term<%d>: maintain authority successfully!\n", args.LeaderId, args.Term)
				}
			}(i, entriesArgs, entriesReply)
		}
		select {
		case <-time.After(120 * time.Millisecond):
		case <-rf.heartBeat:
			log.Printf("Leader[%d]-term<%d>: lose his leader,become follower!\n", rf.me, rf.currentTerm)
		}
	}
}

func (rf *Raft) startElection() {
	for {
		rf.mu.Lock()
		if rf.role != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.currentTerm++
		rf.votes = 1
		rf.votedFor = rf.me
		lastLogIndex := len(rf.log) - 1
		voteArgs := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  rf.log[lastLogIndex].Term,
		}
		rf.mu.Unlock()

		// In order to prevent congestion, set up a buffer queue
		l := len(rf.peers)
		q := make([]chan struct{}, l)
		for i := 0; i < l; i++ {
			q[i] = make(chan struct{}, 1)
		}

		for i := 0; i < l; i++ {
			if i == rf.me || len(q[i]) == 1 {
				continue
			}

			// 防止过多请求重复失败，一个返回之后在发送另外一个
			// 成功返回，则正常循环，不耽误任何一次心跳
			// 失败返回，往往会超时
			q[i] <- struct{}{}

			voteReply := RequestVoteReply{}
			go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
				ok := rf.sendRequestVote(server, &args, &reply)
				<-q[server]
				if !ok {
					log.Printf("Candidate[%d]: sendRequestVote to %d failed!\n", args.CandidateId, server)
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				//log.Printf("Reply:%#v\n", reply)
				// 判断当前任期是否过期
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1 // 更新选票
					rf.role = Follower
					return
				}

				if reply.VoteGranted {
					rf.votes++
					if rf.votes > len(rf.peers)/2 && rf.role == Candidate {
						rf.role = Leader
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0
						}
						rf.done <- struct{}{}
					}
					return
				}
			}(i, voteArgs, voteReply)
		}
		// election wait for a random amount of time between 500 and 800 milliseconds.
		electionTimeOut := 800 + (rand.Int63() % 300)
		select {
		case <-time.After(time.Duration(electionTimeOut) * time.Millisecond):
			log.Printf("Candidate[%d]: election failed,beacause election timeout!\n", rf.me)
		case <-rf.done:
			log.Printf("Candidate[%d]: election successfully, become leader!\n", rf.me)
		case <-rf.heartBeat:
			log.Printf("Candidate[%d]: election failed,beacause the leader has been elected!\n", rf.me)
		}
	}
}

// 程序开始调用的判断是否参加竞选的依据
func (rf *Raft) heartbeat() bool {
	select {
	case <-time.After(time.Duration(heartBeatTimeOut+rand.Int63()%100) * time.Millisecond):
		return false
	case <-rf.heartBeat:
		return true
	}
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower
	rf.log = []Entry{{}}

	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))

	rf.applyMsg = applyCh

	rf.currentTerm = 0
	rf.done = make(chan struct{})
	rf.heartBeat = make(chan struct{})
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rand.Seed(time.Now().UnixNano())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendApplyMsg()

	return rf
}
