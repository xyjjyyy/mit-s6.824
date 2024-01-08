package raft

import "log/slog"

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
	slog.Info("start election",
		slog.Int("node", rf.me),
		slog.Int("term", rf.currentTerm))
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		single := make(chan struct{})
		go rf.handleSendVote(i, voteArgs, single)
	}
}

func (rf *Raft) handleSendVote(server int, args RequestVoteArgs, single chan struct{}) {
	voteReply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &voteReply)

	rf.mu.Lock()
	defer func() {
		rf.persist(nil)
		rf.mu.Unlock()
	}()

	if !ok {
		slog.Error("handleVote: sendRequestVote failed",
			slog.Int("me", rf.me),
			slog.Int("peer", server))
		return
	}

	slog.Info("handleVote: sendRequestVote successfully",
		slog.Int("me", rf.me),
		slog.Int("peer", server))

	// 拒绝投票
	if !voteReply.VoteGranted {
		// term已经不是最新的，更新自己的身份
		if voteReply.Term >= rf.currentTerm {
			rf.toFollwerState(voteReply.Term, -1)
			slog.Info("handleVote: find a newer node",
				slog.Int("leader", rf.me),
				slog.Int("peer", server),
				slog.Int("term", voteReply.Term))
		}
		return
	}

	// 检查身份不再是 Candidate
	if rf.role != Candidate {
		// 处理过半之后的选票
		if rf.role == Leader {
			return
		}

		slog.Info("handleVote: role is a Follower, the vote invalid for curNode",
			slog.Int("curNode", rf.me),
			slog.Int("peer", server))
		return
	}

	select {
	case <-single:

	default:
		rf.votes++
		// 超过半数通票，成为leader
		if rf.votes > len(rf.peers)/2 {
			slog.Info("become leader", slog.Int("node", rf.me))
			rf.toLeaderState()
			close(single) // 关闭信号
		}
	}
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			return
		}
	}

	rf.currentTerm = args.Term
	rf.role = Follower

	lastLog := rf.getLastLog()
	lastLogIndex := lastLog.Index
	lastLogTerm := lastLog.Term
	if args.LastLogTerm > lastLogTerm ||
		(args.LastLogIndex >= lastLogIndex && args.LastLogTerm == lastLogTerm) {
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.toFollwerState(args.Term, args.CandidateId)
	}

	rf.persist(nil)
}
