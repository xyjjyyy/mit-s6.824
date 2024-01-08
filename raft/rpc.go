package raft

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
