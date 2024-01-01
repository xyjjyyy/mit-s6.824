package raft

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestRand(t *testing.T) {
	randSeed := rand.New(rand.NewSource(time.Now().UnixMicro()))
	for i := 0; i < 20; i++ {
		num1 := randSeed.Int63() % 50
		num2 := randSeed.Int63() % 50

		fmt.Println(num1, num2)
	}
}
