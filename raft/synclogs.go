package raft

import (
	"log/slog"
)

func (rf *Raft) startSendEntries() {
	slog.Info("Start to send entries", slog.Int("leader", rf.me))

	rf.mu.Lock()
	rf.heartBeatCount = 0
	rf.mu.Unlock()

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

		single := make(chan struct{})
		go rf.handleSendEntries(i, entriesArgs, single)
	}
}

func (rf *Raft) handleSendEntries(server int, args AppendEntriesArgs, single chan struct{}) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)

	rf.mu.Lock()
	defer func() {
		rf.persist(nil)
		rf.mu.Unlock()
	}()

	if !ok {
		slog.Error("hadleSendEntries: sendEntries failed",
			slog.Int("leader", rf.me),
			slog.Int("peer", server))
		return
	}

	slog.Info("hadleSendEntries: sendEntries successfully",
		slog.Int("leader", rf.me),
		slog.Int("peer", server))

	if rf.role != Leader || rf.killed() {
		return
	}

	if reply.Success {
		rf.nextIndex[server] = reply.XIndex + 1

		select {
		case <-single:

		default:
			rf.heartBeatCount++
			if rf.heartBeatCount > len(rf.peers)/2 {
				rf.commitIndex = reply.XIndex
				close(single)
			}
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
			rf.nextIndex[server] = index + 1
			break
		}
	}
}

// AppendEntries 接受日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		rf.persist(nil)
		rf.mu.Unlock()
	}()
	slog.Info("args content", slog.Any("args", args), slog.Int("node", rf.me))

	// term小于当前的term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.toFollwerState(args.Term, args.LeaderId)

	preLogIndex, preLogTerm := args.PreLogIndex, args.PreLogTerm
	lastLog := rf.getLastLog()
	slog.Info("lastLog content", slog.Any("lastLog", lastLog))
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
	rf.noopflag=true
	reply.Success = true
	reply.XTerm = lastLog.Term
	reply.XIndex = lastLog.Index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastLog.Index)
	}
}
