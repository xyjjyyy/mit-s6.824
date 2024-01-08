package raft

import (
	"bytes"
	"log"
	"log/slog"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"mit-s6.824/labgob"
	"mit-s6.824/labrpc"
)

func init() {
	file, err := os.OpenFile("task.json", os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0666)
	if err != nil {
		log.Fatal(err)
	}

	jsonHandler := slog.NewJSONHandler(file, nil)
	logger := slog.New(jsonHandler)
	slog.SetDefault(logger)
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

	slog.Error("index not found in logs", slog.Int("node", rf.me), slog.Int("index", index))
	return -1
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

	nextIndex   []int // 每一个节点下一个开始复制的位置
	matchIndex  []int // 每一个节点现在匹配到哪一个位置
	commitIndex int   // 提交到的位置

	lastApplied int // 上次commit的记录

	lastIncludedIndex int // 持久化之后的index
	lastIncludedTerm  int // 持久化之后的term

	applyMsg chan ApplyMsg // 提交信息

	heartBeatTimer *time.Timer // 心跳时间间隔
	electionTimer  *time.Timer // 选举时间间隔

	randSeed *RandSeedSafe // 随机数种子

	noopflag bool // noop标志位，标明是否可以泵入noop日志
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

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

// Start 投递命令
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.role == Leader

	if !isLeader {
		return index, term, false
	}

	lastLog := rf.getLastLog()
	// 确保日志不重复
	// TODO：思考更好的方式进行避免重复，比如UUID
	if reflect.DeepEqual(command, lastLog.Command) {
		return index, term, false
	}

	index = lastLog.Index + 1
	term = rf.currentTerm
	rf.logs = append(rf.logs, &Entry{
		Index:   index,
		Term:    term,
		Command: command,
	})
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index

	rf.noopflag = true

	slog.Info("Start cmd",
		slog.Int("node", rf.me),
		slog.Int("term", rf.currentTerm),
		slog.Any("cmd", command))

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
