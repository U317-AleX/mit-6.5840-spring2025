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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RegisterWorkerArgs struct {}	// empty args

type RegisterWorkerReply struct {
	WorkerID int // worker ID
	ReduceTasks int // number of reduce tasks
}

type RequestWorkArgs struct {
	WorkerID int // worker ID
}

type RequestWorkReply struct {
	WorkerID int // worker ID
	WorkType string // "map" or "reduce"
	FileName string // file name for map work
	mapTasksID int // worker ID for map work
	reduceTaskID int // worker ID for reduce work
}

type WorkFinishedArgs struct {
	WorkerID int // worker ID
	WorkType string // "map" or "reduce"
	FileName string // file name for map work
	mapTasksID int // worker ID for map work
	reduceTaskID int // worker ID for reduce work
}

type WorkFinishedReply struct {}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
