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
	"bytes"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	electionTimeout  = 500
)

func getHeartBeatTimeOut() time.Duration {
	return time.Duration(heartBeatTimeOut+rand.Int63()%100) * time.Millisecond
}
func getElectionTimeOut() time.Duration {
	return time.Duration(electionTimeout+rand.Int63()%200) * time.Millisecond
}

func (rf *Raft) getLastLog() *Entry {
	return rf.logs[len(rf.logs)-1]
}
func (rf *Raft) getFirstLog() *Entry {
	return rf.logs[0]
}

func (rf *Raft) getRealIndex(index int) int {
	l := 0
	r := len(rf.logs)

	for l < r {
		m := (l + r) / 2
		realIndex := rf.logs[m].Index
		if realIndex == index {
			return m
		} else if realIndex > index {
			r = m
		} else {
			l = m + 1
		}
	}
	log.Fatalf("peer[%d] index %d not found,so return 0", rf.me, index)
	return 0
}

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

// Raft implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role Role

	logs        []*Entry
	votedFor    int
	currentTerm int

	nextIndex  []int
	matchIndex []int

	commitIndex int
	lastApplied int

	applyMsg chan ApplyMsg // 提交信息

	heartBeatTimer *time.Timer
	electionTimer  *time.Timer
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
func (rf *Raft) persist(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		log.Println("persist currentTerm failed!")
	}
	if err := e.Encode(rf.votedFor); err != nil {
		log.Println("persist votedFor failed!")
	}
	log.Println("persist")
	commitIndex := rf.getRealIndex(rf.commitIndex)
	if err := e.Encode(rf.logs[:commitIndex+1]); err != nil {
		log.Println("persist log failed")
	}
	if err := e.Encode(rf.lastApplied); err != nil {
		log.Println("persist lastApplied failed")
	}
	if err := e.Encode(rf.commitIndex); err != nil {
		log.Println("persist commitIndex failed")
	}
	raftstate := w.Bytes()

	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}

	rf.persister.Save(raftstate, snapshot)

	if rf.role == Leader {
		file, err := os.OpenFile("leader.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
			0666)
		if err != nil {
			log.Fatal(err)
		}

		defer file.Close()

		w1 := new(bytes.Buffer)
		e1 := labgob.NewEncoder(w1)
		if err := e1.Encode(rf.logs[:commitIndex+1]); err != nil {
			log.Println("persist log failed")
		}
		if err := e1.Encode(rf.commitIndex); err != nil {
			log.Println("persist commitIndex failed")
		}

		raftstate = w1.Bytes()
		file.Write(raftstate)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term, votedFor, lastApplied, commitIndex int
	var logs []*Entry
	log.Println("readPersist start!")
	if d.Decode(&term) == nil {
		rf.currentTerm = term
	} else {
		log.Println("readPersist currentTerm failed!")
	}
	if d.Decode(&votedFor) == nil {
		rf.votedFor = votedFor
	} else {
		log.Println("readPersist votedFor failed!")
	}
	if d.Decode(&logs) == nil {
		rf.logs = logs
	} else {
		log.Println("readPersist log failed")
	}
	if d.Decode(&lastApplied) == nil {
		rf.lastApplied = lastApplied
	} else {
		log.Println("readPersist lastApplied failed")
	}
	if d.Decode(&commitIndex) == nil {
		rf.commitIndex = commitIndex
	} else {
		log.Println("readPersist commitIndex failed")
	}
}

func (rf *Raft) readLeaderFile() {
	data, _ := os.ReadFile("leader.log")
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var commitIndex int
	var logs []*Entry

	if err := d.Decode(&logs); err == nil {
		//rf.logs = logs
	} else {
		log.Println("readfile log failed, ", err)
	}

	if err := d.Decode(&commitIndex); err == nil {
		//rf.commitIndex = commitIndex
		if commitIndex > rf.commitIndex {
			rf.commitIndex = commitIndex
			rf.logs = logs
			log.Println("readFile logs: ")
			for _, l := range logs {
				log.Printf("%#v\n", l)
			}
		}
	} else {
		log.Println("readfile commitIndex failed, ", err)
	}
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	firstLog := rf.getFirstLog()
	if firstLog.Index > index {
		log.Printf("peer[%d] Snapshot to index[%d] failed,because firstLog.index is %d\n", rf.me, index, firstLog.Index)
	}

	lastLog := rf.getLastLog()
	if lastLog.Index < index {
		log.Printf("peer[%d] Snapshot to index[%d] failed,because lastLog.index is %d\n", rf.me, index, lastLog.Index)
	}

	log.Printf("peer[%d] Snapshot index[%d] \n", rf.me, index)
	realIndex := rf.getRealIndex(index)
	rf.logs = rf.logs[realIndex:]
	rf.logs[0].Command = 0
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	rf.persist(snapshot)

	log.Printf("peer[%d] persist snapshot,index:[%d]\n", rf.me, index)
	for _, e := range rf.logs {
		log.Printf("%#v\n", e)
	}
}

type SnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}
type SnapShotReply struct {
	Term int
}

func (rf *Raft) InstallSnapShotRpc(args *SnapShotArgs, reply *SnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimer.Reset(getElectionTimeOut())
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	rf.electionTimer.Reset(getElectionTimeOut())

	// outdated snapshot
	if args.LastIncludedIndex <= rf.commitIndex {
		log.Println("InstallSnapShotRpc is outOfDate")
		return
	}

	log.Printf("arg:%#v\n", args)
	for _, e := range rf.logs {
		log.Printf("%#v\n", e)
	}
	if args.LastIncludedIndex > rf.getLastLog().Index {
		rf.logs = []*Entry{{}}
	} else {
		rf.logs = rf.logs[args.LastIncludedIndex-rf.getFirstLog().Index:]
	}
	// update dummy entry with lastIncludedTerm and lastIncludedIndex
	rf.logs[0].Command = 0
	rf.logs[0].Term, rf.logs[0].Index = args.LastIncludedTerm, args.LastIncludedIndex
	rf.lastApplied, rf.commitIndex = args.LastIncludedIndex, args.LastIncludedIndex

	rf.persist(args.Data)

	go func() {
		msg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.applyMsg <- msg
	}()
}

func (rf *Raft) sendInstallSnapShot(server int) bool {
	firstLog := rf.getFirstLog()
	args := SnapShotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: firstLog.Index,
		LastIncludedTerm:  firstLog.Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := SnapShotReply{}

	ok := rf.peers[server].Call("Raft.InstallSnapShotRpc", &args, &reply)
	if !ok {
		return ok
	}

	if reply.Term > rf.currentTerm {
		log.Printf("peer[%d] find a newer peer[%d] with term[%d]，so snapshot failed\n", rf.me, server, reply.Term)
		return false
	}
	return true
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
		rf.role = Follower
	} else {
		return
	}

	lastLog := rf.getLastLog()

	lastLogIndex := lastLog.Index
	lastLogTerm := lastLog.Term
	log.Printf("Peer[%d] with term[%d] receive from [%d] %#v\n", rf.me, rf.currentTerm, args.CandidateId, args)
	if args.LastLogTerm > lastLogTerm || (args.LastLogIndex >= lastLogIndex && args.LastLogTerm == lastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.electionTimer.Reset(getElectionTimeOut())
		log.Printf("peer[%d] votefor %d\n", rf.me, args.CandidateId)
	} else {
		log.Printf("peer[%d] do not votefor %d\n", rf.me, args.CandidateId)
	}
	rf.persist(nil)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []*Entry // copy log entries
	LeaderCommit int      // leader's commitIndex
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

// AppendEntries send heartBeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.currentTerm = args.Term
	rf.role = Follower
	reply.Term = args.Term
	// renew heartbeat
	// 有两个作用：
	// 如果为leader，失去leader地位，停止发送心跳信息
	// 如果为candidate，停止竞选
	rf.electionTimer.Reset(getElectionTimeOut())
	rf.persist(nil)
	//log.Printf("peer[%d] receive leader[%d] appendEntries\n", rf.me, args.LeaderId)

	lastIndex := rf.getLastLog().Index
	preLogIndex, preLogTerm := args.PreLogIndex, args.PreLogTerm
	if preLogIndex <= lastIndex {
		realArgIndex := rf.getRealIndex(preLogIndex)
		e := rf.logs[realArgIndex]

		if e.Term == preLogTerm && preLogIndex == e.Index {
			if args.Entries != nil {
				rf.logs = append(rf.logs[:realArgIndex+1], args.Entries...)
			}
			reply.Success = true
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = MinAB(rf.getLastLog().Index, args.LeaderCommit)
				log.Printf("peer[%d] update his commitindex to %d\n", rf.me, rf.commitIndex)
			}
			log.Printf("peer[%d] receive leader[%d] \n", rf.me, args.LeaderId)
			for _, e := range args.Entries {
				log.Printf("%#v ", e)
			}
			return
		} else {
			appliedIndex := rf.getRealIndex(rf.lastApplied + 1)
			reply.XTerm = e.Term
			reply.XIndex = rf.lastApplied + 1
			//reply.XLen = realArgIndex - 1
			for i := realArgIndex - 1; i >= appliedIndex; i-- {
				if rf.logs[i].Term != e.Term {
					reply.XTerm = e.Term
					reply.XIndex = rf.logs[i].Index + 1
					//reply.XLen = realArgIndex - i + 1
					break
				}
			}
		}
	} else {
		reply.XTerm = -1
		reply.XLen = args.PreLogIndex - lastIndex
	}
	//if args.PreLogIndex <= lastIndex {
	//	log.Println("AppendEntries")
	//	if e.Term == args.PreLogTerm {
	//		if args.Entries != nil {
	//			rf.logs = append(rf.logs[:realArgIndex+1], args.Entries...)
	//		}
	//		reply.Success = true
	//		if args.LeaderCommit > rf.commitIndex {
	//			rf.commitIndex = MinAB(rf.getLastLog().Index, args.LeaderCommit)
	//			log.Printf("peer[%d] update his commitindex to %d\n", rf.me, rf.commitIndex)
	//		}
	//		log.Printf("peer[%d] receive leader[%d] \n", rf.me, args.LeaderId)
	//		for _, e := range args.Entries {
	//			log.Printf("%#v ", e)
	//		}
	//		return
	//	} else {
	//		reply.XTerm = e.Term
	//		reply.XIndex = rf.logs[0].Index
	//		reply.XLen = realArgIndex
	//		for i := realArgIndex - 1; i >= 0; i-- {
	//			if rf.logs[i].Term != e.Term {
	//				reply.XTerm = e.Term
	//				reply.XIndex = rf.logs[i].Index + 1
	//				reply.XLen = realArgIndex - i
	//				break
	//			}
	//		}
	//	}
	//} else {
	//	reply.XTerm = -1
	//	reply.XLen = args.PreLogIndex - lastIndex
	//}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.role == Leader
	lastLog := rf.getLastLog()
	if isLeader {
		if lastLog.Command == command {
			index = lastLog.Index
			term = lastLog.Term
			log.Printf("duplite command %v with term[%d] index[%d]\n", command, term, index)
		} else {
			index = lastLog.Index + 1
			term = rf.currentTerm
			rf.logs = append(rf.logs, &Entry{Index: index, Term: term, Command: command})
			rf.nextIndex[rf.me] = index + 1
			rf.matchIndex[rf.me] = index
		}

		log.Printf("[%d]Start: index:%d,term:%d,entry:%v\n", rf.me, index, term, rf.logs[rf.getRealIndex(index)])
	}

	return index, term, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
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
	//log.Println("dead: ", z)
	return z == 1
}

func (rf *Raft) sendApplyMsg() {
	for !rf.killed() {
		rf.mu.Lock()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		var entries []*Entry
		if rf.logs[len(rf.logs)-1].Term >= rf.currentTerm {
			//log.Println("sendApplyMsg")
			f := rf.getRealIndex(lastApplied)
			e := rf.getRealIndex(commitIndex)
			for i := f + 1; i <= e; i++ {
				entries = append(entries, rf.logs[i])
			}
		}
		rf.mu.Unlock()

		if commitIndex < lastApplied {
			log.Printf("peer[%d] commitIndex{%d] 不可能小于 lastApplied[%d]\n", rf.me, commitIndex, lastApplied)
			return
		}
		if commitIndex == lastApplied {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		for _, entry := range entries {
			log.Printf("sendApplyMsg peer[%d]: %#v\n", rf.me, entry)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			rf.applyMsg <- msg

			rf.mu.Lock()
			rf.lastApplied = entry.Index
			rf.persist(nil)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startAppendEntries() {
	heartBeatCount := 1
	endFlag := false

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		firstLog := rf.getFirstLog()
		//log.Println("peer:", rf.me, " rf.nextIndex[i]: ", rf.nextIndex[i], "firstLog.Index:", firstLog.Index)
		if rf.nextIndex[i] <= firstLog.Index {
			log.Printf("peer[%d] 落后leader[%d]太多\n", i, rf.me)
			rf.nextIndex[i] = firstLog.Index + 1
			go rf.sendInstallSnapShot(i)
		}

		log.Println("startAppendEntries")
		realIndex := rf.getRealIndex(rf.nextIndex[i] - 1)
		preLog := rf.logs[realIndex]

		entriesArgs := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PreLogIndex:  preLog.Index,
			PreLogTerm:   preLog.Term,
			LeaderCommit: rf.commitIndex,
			Entries:      nil,
		}

		if realIndex+1 < len(rf.logs) {
			entries := make([]*Entry, len(rf.logs)-realIndex-1)
			copy(entries, rf.logs[realIndex+1:])
			entriesArgs.Entries = entries
		}
		//log.Printf("entries: %#v\n", entriesArgs.Entries)
		lastLog := rf.getLastLog()

		entriesReply := AppendEntriesReply{}
		go func(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
			log.Printf("leader[%d] send to[%d] args:%#v\n", rf.me, server, args)
			for _, e := range args.Entries {
				log.Printf("%#v\n", e)
			}
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role == Leader {
				if !reply.Success {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = Follower
						rf.votedFor = -1
					} else {
						log.Printf("%d reply:%#v\n", server, reply)
						if reply.XTerm == -1 {
							rf.nextIndex[server] -= reply.XLen
						} else {
							// ? 这里可能不存在realIndex
							log.Println("Update")
							realIndex := rf.getRealIndex(reply.XIndex)
							for j := realIndex; j < len(rf.logs); j++ {
								if rf.logs[j].Term != reply.XTerm {
									rf.nextIndex[server] = j - realIndex + reply.XIndex
									break
								}
							}
						}
					}
					rf.persist(nil)
					return
				}

				rf.nextIndex[server] = lastLog.Index + 1

				heartBeatCount++
				if heartBeatCount > len(rf.peers)/2 && !endFlag {
					if len(args.Entries) != 0 {
						rf.commitIndex = lastLog.Index
					}
					rf.persist(nil)
					endFlag = true
					log.Printf("Leader[%d]-term<%d>: maintain authority successfully!\n", rf.me, rf.currentTerm)
				}
			}

		}(i, entriesArgs, entriesReply)
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.role = Candidate
	lastLog := rf.getLastLog()
	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	votes := 1
	rf.persist(nil)
	log.Printf("%d start election\n", rf.me)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		voteReply := RequestVoteReply{}
		go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role == Candidate {
				if reply.VoteGranted {
					votes++
					if votes > len(rf.peers)/2 {
						rf.role = Leader
						log.Printf("peer[%d] become leader!\n", rf.me)
						if lastLog.Term > 0 {
							rf.readLeaderFile()
							lastLog = rf.getLastLog()
							rf.logs = append(rf.logs, &Entry{Term: rf.currentTerm, Index: lastLog.Index + 1, Command: 0})
							rf.persist(nil)
						}
						lastLog = rf.getLastLog()
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = lastLog.Index + 1
						}
						rf.startAppendEntries()
						rf.heartBeatTimer.Reset(getHeartBeatTimeOut())
					}
				} else {
					if reply.Term > rf.currentTerm {
						log.Printf("peer[%d] find a newer[%d] with term[%d]\n", rf.me, server, reply.Term)
						rf.currentTerm = reply.Term
						rf.votedFor = -1 // 更新选票
						rf.role = Follower
						rf.persist(nil)
					}
				}
			}
		}(i, voteArgs, voteReply)
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
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyMsg:       applyCh,
		role:           Follower,
		votedFor:       -1,
		logs:           []*Entry{{Index: 0, Term: 0, Command: 0}},
		commitIndex:    0,
		matchIndex:     make([]int, len(peers)),
		nextIndex:      make([]int, len(peers)),
		currentTerm:    0,
		heartBeatTimer: time.NewTimer(getHeartBeatTimeOut()),
		electionTimer:  time.NewTimer(getElectionTimeOut()),
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = lastLog.Index + 1
	}

	// 初始化随机数种子
	//rand.Seed(time.Now().UnixNano())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendApplyMsg()

	return rf
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		select {
		case <-rf.electionTimer.C: // 选举时间到期重新选举
			rf.mu.Lock()
			if rf.role != Leader {
				rf.startElection()
				rf.electionTimer.Reset(getElectionTimeOut())
			}
			rf.mu.Unlock()
		case <-rf.heartBeatTimer.C: // 心跳时间到期 如果是leader就要重新发送
			rf.mu.Lock()
			if rf.role == Leader {
				rf.startAppendEntries()
				rf.heartBeatTimer.Reset(getHeartBeatTimeOut())
			}
			rf.mu.Unlock()
		}
	}
}
