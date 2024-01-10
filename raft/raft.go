package raft

import (
	"log"
	"log/slog"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

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
	if len(rf.logs) == 0 {
		return nil
	}

	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLog() *Entry {
	return rf.logs[0]
}

// getRealIndex 获取当前的实际位置
func (rf *Raft) getRealIndex(index int) int {
	// 处理临界特殊点
	if index == rf.lastIncludedIndex {
		return -1
	}

	// 二分查找正常情况
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
	panic("index not found in logs")
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

	votedFor int // 投票人

	currentTerm int // 当前term

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

	nextIndex := rf.getLastLog().Index + 1
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = nextIndex
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
