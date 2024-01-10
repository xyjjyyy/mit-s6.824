package raft

import (
	"log/slog"
)

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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
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

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.toFollwerState(WithTerm(args.Term), WithVotedFor(args.LeaderId))

	// outdated snapshot
	if args.LastIncludedIndex <= rf.lastApplied {
		slog.Info("InstallSnapShotRpc is outOfDate",
			slog.Int("leader", args.LeaderId),
			slog.Int("node", rf.me),
			slog.Int("lasstApplied", rf.lastApplied),
			slog.Int("lastincludedIndex", args.LastIncludedIndex))
		return
	}

	if args.LastIncludedIndex > rf.getLastLog().Index {
		rf.logs = nil
	} else {
		rf.logs = rf.logs[args.LastIncludedIndex-rf.getFirstLog().Index:]
	}

	// update dummy entry with lastIncludedTerm and lastIncludedIndex
	rf.logs[0].Command = 0
	rf.logs[0].Term, rf.logs[0].Index = args.LastIncludedTerm, args.LastIncludedIndex
	rf.lastApplied, rf.commitIndex = args.LastIncludedIndex, args.LastIncludedIndex
	rf.lastIncludedIndex, rf.lastIncludedTerm = args.LastIncludedIndex, args.LastIncludedTerm

	rf.persist(args.Data)
	rf.mu.Unlock()

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyMsg <- msg
}
