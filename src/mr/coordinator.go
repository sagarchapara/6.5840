package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Status int

const (
	Idle Status = iota
	InProgress
	Completed
	Failed
	Unknown
	Waiting
)

type Coordinator struct {
	mu          sync.RWMutex
	workers     map[string]WorkerStatus
	mapTasks    map[int]MapTask
	reduceTasks map[int]ReduceTask
	nReduce     int
	nMap        int
}

type WorkerStatus struct {
	status Status
}

type MapTask struct {
	id        int
	requestId string
	file      string
	status    Status
	workerId  string
	jobTime   time.Time
}

type ReduceTask struct {
	id            int
	requestId     string
	status        Status
	workerId      string
	fileLocations map[int]string
	jobTime       time.Time
}

func (c *Coordinator) GetJob(args *WorkerJobRequest, reply *WorkerJobPayload) error {
	// first check if the worker is already registered

	if !c.mu.TryLock() {
		log.Printf("Coordinator is busy, worker %s will try again later", args.Id)
		return nil
	}

	defer c.mu.Unlock()

	//check if all map tasks are completed, if not reply with a map task
	for _, task := range c.mapTasks {

		//Make sure it's not the same worker that is requesting the job
		if task.workerId == args.Id {
			continue
		}

		if task.status == Idle || (task.status == InProgress && time.Since(task.jobTime) > 10*time.Second) {

			//Mark the worker as in progress
			c.workers[args.Id] = WorkerStatus{
				status: InProgress,
			}

			requestId := uuid.New().String()

			log.Printf("Assigning map task %d to worker %s with request id %s", task.id, args.Id, requestId)

			//Assign the task to the worker
			task.status = InProgress
			task.workerId = args.Id
			task.jobTime = time.Now()
			task.requestId = requestId

			//reply with the task
			reply.Id = requestId
			reply.Index = task.id
			reply.MapOrReduce = Map
			reply.FileLocations = []string{task.file}
			reply.NReduce = c.nReduce
			return nil
		}
	}

	//check if all reduce tasks are completed, if not reply with a reduce task
	for _, task := range c.reduceTasks {

		//Make sure it's not the same worker that is requesting the job
		if task.workerId == args.Id {
			continue
		}

		if task.status == Idle || (task.status == InProgress && time.Since(task.jobTime) > 10*time.Second) {

			//Mark the worker as in progress
			c.workers[args.Id] = WorkerStatus{
				status: InProgress,
			}

			requestId := uuid.New().String()

			log.Printf("Assigning reduce task %d to worker %s with request id %s", task.id, args.Id, requestId)

			//Assign the task to the worker
			task.status = InProgress
			task.workerId = args.Id
			task.jobTime = time.Now()
			task.requestId = requestId

			//reply with the task
			reply.Id = requestId
			reply.Index = task.id
			reply.MapOrReduce = Reduce
			reply.FileLocations = make([]string, 0)
			for _, location := range task.fileLocations {
				if location != "" {
					reply.FileLocations = append(reply.FileLocations, location)
				}
			}
			reply.NReduce = c.nReduce
			return nil
		}
	}

	return nil
}

func (c *Coordinator) CompleteJob(args *WorkerJobCompletionPayload, reply *struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("Received job completion from worker %s for %s task %d with requestId %s", args.WorkerId, strconv.Itoa(int(args.MapOrReduce)), args.Index, args.Id)

	if args.MapOrReduce == Map {

		//check if the map task is already completed
		if c.mapTasks[args.Index].status == Completed {
			return nil
		}

		log.Printf("Updating map task %d status to completed", args.Index)

		//update the map task status and also reduce task file locations
		c.mapTasks[args.Index] = MapTask{
			id:       args.Index,
			status:   Completed,
			workerId: args.WorkerId,
		}

		//update the reduce task file locations
		for i, file := range args.FileLocations {

			log.Printf("Updating reduce task %d file location to %s", i, file)

			// Create a temporary variable to hold the reduce task
			reduceTask := c.reduceTasks[i]

			log.Printf("lenght of reduce task %d before %d", i, len(reduceTask.fileLocations))

			// Update the file locations
			reduceTask.fileLocations[args.Index] = file

			log.Printf("lenght of reduce task %d after %d", i, len(reduceTask.fileLocations))

			// Check if all the reduce tasks have the file locations, if yes then mark the reduce task as idle
			if len(reduceTask.fileLocations) == c.nMap {

				log.Printf("Updating reduce task %d status to idle", i)

				reduceTask.status = Idle
			}

			// Assign the updated reduce task back to the map
			c.reduceTasks[i] = reduceTask
		}

	} else if args.MapOrReduce == Reduce {

		//check if the reduce task is already completed
		if c.reduceTasks[args.Index].status == Completed {
			return nil
		}

		log.Printf("Updating reduce task %d status to completed", args.Index)

		//update the reduce task status
		c.reduceTasks[args.Index] = ReduceTask{
			id:       args.Index,
			status:   Completed,
			workerId: args.WorkerId,
		}
	}

	log.Printf("Job completion from worker %s for %s task %d with requestId %s completed", args.WorkerId, strconv.Itoa(int(args.MapOrReduce)), args.Index, args.Id)

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	//If all reduce tasks are completed we are done
	for _, task := range c.reduceTasks {
		if task.status != Completed {
			return ret
		}
	}

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mu:          sync.RWMutex{},
		workers:     make(map[string]WorkerStatus),
		mapTasks:    make(map[int]MapTask),
		reduceTasks: make(map[int]ReduceTask),
		nReduce:     nReduce,
		nMap:        len(files),
	}

	//initialize the map tasks
	for i, file := range files {

		log.Printf("Creating map task %d with file %s", i, file)

		c.mapTasks[i] = MapTask{
			id:     i,
			file:   file,
			status: Idle,
		}
	}

	//initialize the reduce tasks
	for i := 0; i < nReduce; i++ {

		log.Printf("Creating reduce task %d", i)

		c.reduceTasks[i] = ReduceTask{
			id:            i,
			status:        Waiting, //Waiting for all map tasks to complete, we can mark it as idle once we have all the file locations
			fileLocations: make(map[int]string),
		}
	}

	c.server()
	return &c
}
