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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
const (
	//responce中的state
	MAPPING  = 0
	REDUCING = 1
	WAIT     = 2
	COMPLETE = 3

	// request中的state
	FREE       = 0
	MAPDONE    = 1
	REDUCEDONE = 2
)

type RPCArgs struct {
	State   int
	Content map[string]any
}

type RPCResponce struct {
	State   int
	Content map[string]any
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
