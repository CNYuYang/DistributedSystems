package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Task struct {
	TaskID   int
	FileName string
	NReduce  int
	NMap     int
	MapID    int
	ReduceID int
}

// WORK_TYPE

const (
	TASK_MAP    int = 1
	TASK_REDUCE int = 2
	TASK_FILSH  int = 3
	TASK_WAIT   int = 4
)

//COMMICATION_TYPE

const (
	ASK_TASK     int = 10
	MAP_FILSH    int = 11
	REDUCE_FILSH int = 12
)

type Arg struct {
	ASK_TYPE int
	ASK_ID   int
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
