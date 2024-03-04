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

// worker
type WorkerRequest struct{
	WorkerId string
	WorkerDone int
}


// 任务类型
type JobType int
const(
	MapJob = iota
	ReduceJob
	WaitingJob
	KillJob
)


// 任务状态
type JobCondition int
const(
	JobWorking = iota
	JobWaiting
	JobDone
)

// coordinator 的执行状态
type Condition int
const(
	MapPhase = iota
	ReducePhase
	AllDone
)



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
