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

const GetJob = "Coordinator.GetJob"
const CompleteJob = "Coordinator.CompleteJob"

type MapOrReduce int

const (
	None MapOrReduce = iota
	Map
	Reduce
)

//struct for returning a job from master
type WorkerJobPayload struct {
	Id            string //request id
	Index         int    //index of the map or reduced task
	MapOrReduce   MapOrReduce
	FileLocations []string //list of file locations either for map task or reduce task
	NReduce       int      //no of reduce tasks
}

type WorkerJobRequest struct {
	Id string //worker id
}

type WorkerJobCompletionPayload struct {
	Id            string //request id
	Index         int    //index of the map or reduced task
	WorkerId      string
	MapOrReduce   MapOrReduce
	FileLocations []string //list of file locations either for map task or reduce task
	NReduce       int      //no of reduce tasks
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
