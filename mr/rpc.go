package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	Success = iota
	Failure
	Pending
)

const (
	Map     = "Map"
	Shuffle = "Shuffle"
	Reduce  = "Reduce"
	Done    = "Done"
)

const TimeOut = 10

const (
	MaxRetry      = 10
	RetryInterval = 1

	MaxServerTimeout = 5
)

type ApplyArgs struct {
	Type string
}

type ApplyReply struct {
	Task     *Task
	Finished bool // 流程是否已经完结
}

type SubmitArgs struct {
	Type      string
	Index     int
	State     int
	FilePaths []string
}

type SubmitReply struct { // empty struct
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
