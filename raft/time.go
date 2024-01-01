package raft

import (
	"math/rand"
	"sync"
	"time"
)

var randSeed *RandSeedSafe

// RandSeedSafe 并发安全的随机数生成器
type RandSeedSafe struct {
	mu       sync.Mutex
	RandSeed *rand.Rand
}

// NewRandSeedSafe 返回并发安全的随机数生成器
func NewRandSeedSafe() *RandSeedSafe {
	if randSeed != nil {
		return randSeed
	}

	randSeed = &RandSeedSafe{
		RandSeed: rand.New(rand.NewSource(time.Now().UnixMicro())),
	}
	return randSeed
}

// 这个时间可以调整，可以调的更小，让系统运行的更快，但是当前时间完全足够通过所有的测试点
// 不过2023课程要求1s最好不要超过十个rpc发送
const (
	heartBeatTimeOut = 100
	electionTimeout  = 250
)

func (rs *RandSeedSafe) getHeartBeatTimeOut() time.Duration {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	return time.Duration(heartBeatTimeOut+rs.RandSeed.Int63()%50) * time.Millisecond
}

func (rs *RandSeedSafe) getElectionTimeOut() time.Duration {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	return time.Duration(electionTimeout+rs.RandSeed.Int63()%100) * time.Millisecond
}

// RestHeartBeatTicker 重置心跳时间
func (rf *Raft) RestHeartBeatTicker() {
	rf.heartBeatTimer.Reset(rf.randSeed.getHeartBeatTimeOut())
}

// RestElectionTicker 重置选举时间
func (rf *Raft) RestElectionTicker() {
	rf.electionTimer.Reset(rf.randSeed.getElectionTimeOut())
}

// RestHeartBeatTickerSafely 并发安全的重置心跳时间
func (rf *Raft) RestHeartBeatTickerSafely() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.heartBeatTimer.Reset(rf.randSeed.getHeartBeatTimeOut())
}

// RestElectionTickerSafely 并发安全的重置选举时间
func (rf *Raft) RestElectionTickerSafely() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.electionTimer.Reset(rf.randSeed.getElectionTimeOut())
}
