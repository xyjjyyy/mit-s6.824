package raft

import (
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func MinAB(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func MinSliceApartZero(sli []int) int {
	m := sli[0]
	for _, s := range sli {
		if s != 0 && s < m {
			m = s
		}
	}
	return m
}
