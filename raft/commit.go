package raft

import (
	"log/slog"
	"time"
)

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
			slog.Error("commitIndex 不可能小于 lastApplied",
				slog.Int("me", rf.me),
				slog.Int("commitIndex", rf.commitIndex),
				slog.Int("lastApplied", rf.lastApplied))
			return
		}

		for _, entry := range entries {
			slog.Info("sendApplyMsg",
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
