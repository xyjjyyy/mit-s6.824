package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"bytes"
	"log/slog"
	"sync"

	"mit-s6.824/labgob"
)

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

func (rf *Raft) sendInstallSnapShot(server int) bool {
	rf.mu.RLock()
	args := SnapShotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.RUnlock()

	reply := SnapShotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapShotRpc", &args, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer func() {
		rf.persist(nil)
		rf.mu.Unlock()
	}()

	// 更新改节点的日志同步位置
	rf.nextIndex[server] = rf.lastIncludedIndex + 1
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		return false
	}

	return true
}

func (rf *Raft) persist(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(rf.currentTerm) != nil {
		persistError("currentTerm")
	}
	if e.Encode(rf.votedFor) != nil {
		persistError("votedFor")
	}
	if e.Encode(rf.logs) != nil {
		persistError("logs")
	}
	if e.Encode(rf.lastIncludedIndex) != nil {
		persistError("lastIncludedIndex")
	}
	if e.Encode(rf.lastIncludedTerm) != nil {
		persistError("lastIncludedTerm")
	}
	// if e.Encode(rf.lastApplied) != nil {
	// 	persistError("lastApplied")
	// }

	raftstate := w.Bytes()
	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludeIndex, lastIncludeTerm int
	var logs []*Entry

	if d.Decode(&currentTerm) != nil {
		panic("readPersist currentTerm failed!")
	}
	if d.Decode(&votedFor) != nil {
		panic("readPersist votedFor failed!")
	}
	if d.Decode(&logs) != nil {
		panic("readPersist log failed")
	}
	if d.Decode(&lastIncludeIndex) != nil {
		panic("readPersist lastIncludeIndex failed")
	}
	if d.Decode(&lastIncludeTerm) != nil {
		panic("readPersist lastIncludeTerm failed")
	}
	// if d.Decode(&lastApplied) != nil {
	// 	panic("readPersist lastApplied failed")
	// }

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.lastIncludedIndex = lastIncludeIndex
	rf.lastIncludedTerm = lastIncludeTerm
	rf.lastApplied = lastIncludeIndex
	rf.commitIndex = lastIncludeIndex

	slog.Info("read Persist", slog.Any("raft obj", rf))
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	realIndex := rf.getRealIndex(index)
	if realIndex == -1 {
		return
	}

	slog.Info("Snapshot", slog.Int("index", index), slog.Int("realIndex", realIndex), slog.Int("node", rf.me))
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logs[realIndex].Term
	rf.logs = rf.logs[realIndex+1:]
	rf.persist(snapshot)
}

func persistError(name string) {
	slog.Error("persist failed", slog.String("name", name))
}
