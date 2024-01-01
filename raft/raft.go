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
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"mit-s6.824/labgob"
	"mit-s6.824/labrpc"
)

var logger *slog.Logger

func init() {
	file, err := os.OpenFile("task.json", os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0666)
	if err != nil {
		log.Fatal(err)
	}

	jsonHandler := slog.NewJSONHandler(file, nil)
	logger = slog.New(jsonHandler)
}

func (rf *Raft) getLastLog() *Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLog() *Entry {
	return rf.logs[0]
}

// getRealIndex 获取当前的实际位置
func (rf *Raft) getRealIndex(index int) int {
	l, r := 0, len(rf.logs)
	// logger.Info("getRealIndex", slog.Int("len", r), slog.Any("entries", rf.logs))
	for l < r {
		m := (l + r) / 2
		logIndex := rf.logs[m].Index
		if logIndex == index {
			return m
		} else if logIndex > index {
			r = m
		} else {
			l = m + 1
		}
	}

	logger.Error("index not found in logs", slog.Int("node", rf.me), slog.Int("index", index))
	return -1
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

// Entry 日志项
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// Raft implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role Role // 当前角色

	logs []*Entry // 日志

	votedFor       int // 投票人
	votes          int // 选票次数
	heartBeatCount int // 心跳次数
	currentTerm    int // 当前term

	nextIndex         []int // 每一个节点下一个开始复制的位置
	matchIndex        []int // 每一个节点现在匹配到哪一个位置
	commitIndex       int   // 提交到的位置
	lastApplied       int
	lastIncludedIndex int
	lastIncludedTerm  int

	applyMsg chan ApplyMsg // 提交信息

	heartBeatTimer *time.Timer // 心跳时间间隔
	electionTimer  *time.Timer // 选举时间间隔

	randSeed *RandSeedSafe // 随机数种子
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	logger.Info("getState")
	term := rf.currentTerm
	isleader := rf.role == Leader

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
	if err := e.Encode(rf.logs); err != nil {
		log.Println("persist log failed")
	}
	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		log.Println("persist lastIncludeIndex failed")
	}
	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		log.Println("persist lastIncludeTerm failed")
	}

	raftstate := w.Bytes()

	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}

	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term, votedFor, lastIncludeIndex, lastIncludeTerm int
	var logs []*Entry
	//log.Println("readPersist start!")
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

	if d.Decode(&lastIncludeIndex) == nil {
		rf.lastIncludedIndex = lastIncludeIndex
		rf.lastApplied, rf.commitIndex = lastIncludeIndex, lastIncludeIndex
	} else {
		log.Println("readPersist lastIncludeIndex failed")
	}
	if d.Decode(&lastIncludeTerm) == nil {
		rf.lastIncludedTerm = lastIncludeTerm
	} else {
		log.Println("readPersist lastIncludeTerm failed")
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

	realIndex := rf.getRealIndex(index)
	rf.logs = rf.logs[realIndex:]
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logs[0].Term
	rf.persist(snapshot)
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
	rf.electionTimer.Reset(rf.randSeed.getElectionTimeOut())
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	rf.role = Follower
	rf.electionTimer.Reset(rf.randSeed.getElectionTimeOut())

	// outdated snapshot
	if args.LastIncludedIndex <= rf.commitIndex {
		log.Println(rf.me, " InstallSnapShotRpc is outOfDate")
		return
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
	rf.lastIncludedIndex, rf.lastIncludedTerm = args.LastIncludedIndex, args.LastIncludedTerm

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

	//log.Printf("%d install snapshot to index %d\n", rf.me, args.LastIncludedIndex)
}

func (rf *Raft) sendInstallSnapShot(server int) bool {
	rf.mu.RLock()
	firstLog := rf.getFirstLog()
	args := SnapShotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: firstLog.Index,
		LastIncludedTerm:  firstLog.Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.RUnlock()

	reply := SnapShotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapShotRpc", &args, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 更新改节点的日志同步位置
	rf.nextIndex[server] = rf.lastIncludedIndex + 1
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
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
		(args.Term == rf.currentTerm &&
			(rf.votedFor == -1 || rf.votedFor == args.CandidateId)) {
		rf.currentTerm = args.Term
		rf.role = Follower
	} else {
		return
	}

	lastLog := rf.getLastLog()

	lastLogIndex := lastLog.Index
	lastLogTerm := lastLog.Term
	if args.LastLogTerm > lastLogTerm || (args.LastLogIndex >= lastLogIndex && args.LastLogTerm == lastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.RestElectionTicker()
	}

	rf.persist(nil)
}

// AppendEntriesArgs 发送日志的结构体
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []*Entry // copy log entries
	LeaderCommit int      // leader's commitIndex
}

// AppendEntriesReply 日志请求的返回结构体
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // 当前最新日志的term
	XIndex  int // 当前最新日志的index
}

// AppendEntries 接受日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		rf.persist(nil)
		rf.mu.Unlock()
	}()

	// term小于当前的term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.currentTerm = args.Term
	rf.role = Follower
	reply.Term = args.Term
	rf.RestElectionTicker()

	preLogIndex, preLogTerm := args.PreLogIndex, args.PreLogTerm
	lastLog := rf.getLastLog()
	// 如果小于请求中的最小index，则返回自己的定位
	if lastLog.Index < preLogIndex {
		reply.XTerm = lastLog.Term
		reply.XIndex = lastLog.Index
		return
	}

	// index确定开始覆盖的位置
	var index int
	var copyFlag bool
	switch {
	// 已经持久化的日志,只复制后面的部分即可
	case preLogIndex < rf.lastIncludedIndex:
		index = 0
		// 可以考虑二分优化
		for i, e := range args.Entries {
			if e.Index == rf.lastIncludedIndex && e.Term == rf.lastIncludedTerm {
				copyFlag = true
				args.Entries = args.Entries[i+1:]
				break
			}
		}

	case preLogIndex == rf.lastIncludedIndex:
		index = 0
		copyFlag = true

	default:
		for i, e := range rf.logs {
			if e.Index != preLogIndex {
				continue
			}

			if e.Term == preLogTerm {
				copyFlag = true
				index = i + 1
				break
			}

			// 不一致直接返回位置
			reply.XTerm = e.Term
			reply.XIndex = e.Index
			return
		}
	}

	if copyFlag {
		rf.logs = append(rf.logs[:index], args.Entries...)
	}
	// 重新获取最新的日志
	lastLog = rf.getLastLog()
	reply.Success = true
	reply.XTerm = lastLog.Term
	reply.XIndex = lastLog.Index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastLog.Index)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

// Start 投递命令
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.role == Leader
	if isLeader {
		index := rf.getLastLog().Index + 1
		term = rf.currentTerm
		rf.logs = append(rf.logs, &Entry{
			Index:   index,
			Term:    term,
			Command: command,
		})
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

		logger.Info("")
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
	return z == 1
}

func (rf *Raft) sendApplyMsg() {
	for !rf.killed() {
		rf.mu.RLock()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied

		if commitIndex == lastApplied {
			rf.mu.RUnlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		var entries []*Entry
		if rf.logs[len(rf.logs)-1].Term >= rf.currentTerm {
			f := rf.getRealIndex(lastApplied)
			e := rf.getRealIndex(commitIndex)
			for i := f + 1; i <= e; i++ {
				entries = append(entries, rf.logs[i])
			}
		}
		rf.mu.RUnlock()

		if commitIndex < lastApplied {
			logger.Error("commitIndex 不可能小于 lastApplied",
				slog.Int("me", rf.me),
				slog.Int("commitIndex", rf.commitIndex),
				slog.Int("lastApplied", rf.lastApplied))
			return
		}

		for _, entry := range entries {
			logger.Info("sendApplyMsg",
				slog.Int("node", rf.me),
				slog.Any("entry", entry))
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

func (rf *Raft) startSendEntries() {
	logger.Info("Start to send entries", slog.Int("leader", rf.me))

	for i := 0; i < len(rf.peers); i++ {
		// 身份转变，立刻退出
		if rf.getRoleSafely() != Leader {
			return
		}

		if i == rf.me {
			continue
		}

		rf.mu.Lock()

		// 落后太多，直接发送快照进行下载
		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			rf.mu.Unlock()
			go rf.sendInstallSnapShot(i)
			continue
		}

		preIndex := rf.getRealIndex(rf.nextIndex[i] - 1)
		preLog := rf.logs[preIndex]
		entriesArgs := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PreLogIndex:  preLog.Index,
			PreLogTerm:   preLog.Term,
			LeaderCommit: rf.commitIndex,
		}

		if preIndex+1 < len(rf.logs) {
			entries := make([]*Entry, len(rf.logs)-preIndex-1)
			copy(entries, rf.logs[preIndex+1:])
			entriesArgs.Entries = entries
		}
		rf.mu.Unlock()

		go rf.handleSendEntries(i, entriesArgs)
	}
}

func (rf *Raft) handleSendEntries(server int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)

	rf.mu.Lock()
	defer func() {
		rf.persist(nil)
		rf.mu.Unlock()
	}()

	if !ok {
		logger.Error("hadleSendEntries: sendEntries failed",
			slog.Int("leader", rf.me),
			slog.Int("peer", server))
		return
	}

	logger.Info("hadleSendEntries: sendEntries successfully",
		slog.Int("leader", rf.me),
		slog.Int("peer", server))

	if rf.role != Leader || rf.killed() {
		return
	}

	if reply.Success {
		rf.nextIndex[server] = reply.XIndex + 1

		rf.heartBeatCount++
		if rf.heartBeatCount > len(rf.peers)/2 {
			rf.commitIndex = reply.XIndex
		}
		return
	}

	if reply.Term > rf.currentTerm {
		rf.toFollwerState(reply.Term, -1)
		return
	}

	for i := len(rf.logs) - 1; i >= 0; i-- {
		index, term := rf.logs[i].Index, rf.logs[i].Term
		if index == reply.XIndex && term == reply.XTerm {
			// leader's log has XTerm
			rf.nextIndex[server] = index + 1
			break
		}
		//  如果不存在，传输整个term过去，提升传输效率
		if term < reply.XTerm {
			// leader's log doesn't have XTerm
			rf.nextIndex[server] = index + 1
			break
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.toCandidateState()
	lastLog := rf.getLastLog()
	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	logger.Info("start election",
		slog.Int("node", rf.me),
		slog.Int("term", rf.currentTerm))
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go rf.handleSendVote(i, voteArgs)
	}
}

func (rf *Raft) handleSendVote(server int, args RequestVoteArgs) {
	voteReply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &voteReply)
	
	rf.mu.Lock()
	defer func() {
		rf.persist(nil)
		rf.mu.Unlock()
	}()

	if !ok {
		logger.Error("handleVote: sendRequestVote failed",
			slog.Int("me", rf.me),
			slog.Int("peer", server))
		return
	}

	logger.Info("handleVote: sendRequestVote successfully",
		slog.Int("me", rf.me),
		slog.Int("peer", server))

	// 拒绝投票并且term已经不是最新的，更新自己的身份
	if !voteReply.VoteGranted && voteReply.Term >= rf.currentTerm {
		rf.toFollwerState(voteReply.Term, -1)
		logger.Info("handleVote: find a newer with big term",
			slog.Int("leader", rf.me),
			slog.Int("peer", server),
			slog.Int("term", voteReply.Term))
		return
	}

	// 检查身份不再是 Candidate
	if rf.role != Candidate {
		// 处理过半之后的选票
		if rf.role == Leader {
			return
		}

		logger.Info("handleVote: role is a Follower, the vote invalid for curNode",
			slog.Int("curNode", rf.me),
			slog.Int("peer", server))
		return
	}

	rf.votes++
	// 超过半数通票，成为leader
	if rf.votes > len(rf.peers)/2 {
		logger.Info("become leader", slog.Int("node", rf.me))
		rf.toLeaderState()
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
		peers:             peers,
		persister:         persister,
		me:                me,
		dead:              0,
		applyMsg:          applyCh,
		role:              Follower,
		votedFor:          -1,
		logs:              []*Entry{{Index: 0, Term: 0}},
		commitIndex:       0,
		matchIndex:        make([]int, len(peers)),
		nextIndex:         make([]int, len(peers)),
		currentTerm:       0,
		randSeed:          NewRandSeedSafe(),
		lastIncludedIndex: -1,
	}
	rf.heartBeatTimer = time.NewTimer(rf.randSeed.getHeartBeatTimeOut())
	rf.electionTimer = time.NewTimer(rf.randSeed.getElectionTimeOut())

	rf.readPersist(persister.ReadRaftState())

	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = lastLog.Index + 1
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendApplyMsg()

	return rf
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		var role Role
		select {
		case <-rf.heartBeatTimer.C: // 心跳时间到期 如果是leader就要重新发送
			role = rf.getRoleSafely()
			if role == Leader {
				rf.startSendEntries()
				rf.RestHeartBeatTickerSafely()
			}

		case <-rf.electionTimer.C: // 选举时间到期重新选举
			role = rf.getRoleSafely()
			if role != Leader {
				rf.startElection()
				rf.RestElectionTickerSafely()
			}
		}
	}
}
