package raft

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (rf *Raft) getRoleSafely() Role {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.role
}

func (rf *Raft) toFollwerState(options ...followerOption) {
	for _, option := range options {
		option(rf)
	}
	rf.role = Follower
	rf.electionTimer.Reset(rf.randSeed.getElectionTimeOut())
}

type followerOption func(*Raft)

func WithTerm(term int) followerOption {
	return func(r *Raft) {
		r.currentTerm = term
	}
}

func WithVotedFor(votedFor int) followerOption {
	return func(r *Raft) {
		r.votedFor = votedFor
	}
}

func (rf *Raft) toCandidateState() {
	rf.currentTerm++
	// 投票给自己
	rf.votedFor = rf.me
	rf.role = Candidate
}

func (rf *Raft) toLeaderState() {
	rf.role = Leader
	lastLog := rf.getLastLog()
	index := lastLog.Index + 1

	if rf.noopflag {
		noLogEntry := &Entry{
			Term:    rf.currentTerm,
			Index:   index,
			Command: nil,
		}

		// 成为leader后，追加一条noLogEntry，用于提交之前的日志
		rf.logs = append(rf.logs, noLogEntry)
	}

	// 更新nextIndex
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = index
	}
	// 时间立刻到期
	rf.heartBeatTimer.Reset(0)
}
