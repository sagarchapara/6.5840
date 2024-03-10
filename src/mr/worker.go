package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerState struct {
	mu         sync.Mutex
	id         string
	request    chan WorkerJobPayload
	exit       chan bool
	mapf       func(string, string) []KeyValue
	reducef    func(string, []string) string
	errorCount int
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerId := uuid.New()

	state := WorkerState{
		mu:         sync.Mutex{},
		id:         workerId.String(),
		request:    make(chan WorkerJobPayload),
		exit:       make(chan bool),
		mapf:       mapf,
		reducef:    reducef,
		errorCount: 0,
	}

	log.Printf("Worker %s started", state.id)

	RunWorker(&state)

	log.Printf("Worker %s stopped", state.id)
}

func RunWorker(state *WorkerState) {

	log.Printf("Worker %s is running", state.id)

	ticker := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-state.exit:
			log.Printf("Worker %s is exiting", state.id)
			ticker.Stop()
			return
		case request := <-state.request:
			log.Printf("Worker %s received a request", state.id)
			go HandleRequest(state, &request)
		case <-ticker.C:
			log.Printf("Worker %s is pinging master", state.id)
			go PingMaster(state)
		}
	}
}

func PingMaster(state *WorkerState) {
	if !state.mu.TryLock() {
		log.Printf("Worker %s is already busy", state.id)
		return
	}
	defer state.mu.Unlock()

	args := WorkerJobRequest{
		Id: state.id,
	}

	reply := WorkerJobPayload{}

	err := call(GetJob, &args, &reply)

	log.Printf("Worker %s pinged master", state.id)

	if err != nil {
		log.Printf("Cannot get job from master %s %v", state.id, err)

		state.errorCount++

		if state.errorCount > 3 {
			state.exit <- true
		}

		return
	}

	state.errorCount = 0

	if reply.Id != "" {
		state.request <- reply
	}
}

func HandleRequest(state *WorkerState, request *WorkerJobPayload) {
	if request.MapOrReduce == Map {
		unqId := state.id + "_" + request.Id + "_" + strconv.Itoa(request.Index)

		fileLocations, err := ExecuteMapTask(unqId, request.FileLocations, state.mapf, request.NReduce)

		if err != nil {
			log.Fatalf("Cannot execute map task %v", err)
			return
		}

		WorkerJobCompletionPayload := WorkerJobCompletionPayload{
			Id:            request.Id,
			Index:         request.Index,
			WorkerId:      state.id,
			MapOrReduce:   request.MapOrReduce,
			FileLocations: fileLocations,
			NReduce:       request.NReduce,
		}

		ReplyMasterForCompletion(state, &WorkerJobCompletionPayload)

	} else if request.MapOrReduce == Reduce {
		filepath, err := ExecuteReduceTask(request.Index, request.FileLocations, state.reducef)

		if err != nil {
			log.Fatalf("Cannot execute reduce task %v", err)
			return
		}

		//after this update master
		WorkerJobCompletionPayload := WorkerJobCompletionPayload{
			Id:            request.Id,
			Index:         request.Index,
			WorkerId:      state.id,
			MapOrReduce:   request.MapOrReduce,
			FileLocations: []string{filepath}, // Create a []string with a single element, filepath
			NReduce:       request.NReduce,
		}

		ReplyMasterForCompletion(state, &WorkerJobCompletionPayload)
	}
}

func ReplyMasterForCompletion(state *WorkerState, request *WorkerJobCompletionPayload) {
	err := call(CompleteJob, &request, &struct{}{})

	if err != nil {
		log.Fatalf("Cannot complete job %v", err)
	}
}

func ExecuteMapTask(workerId string, files []string, mapf func(string, string) []KeyValue, nReduce int) ([]string, error) {

	keyHashMap := make(map[int][]KeyValue)

	for _, filename := range files {
		kva := ReadAndMapFromFile(filename, mapf)

		for _, kv := range kva {
			kHash := ihash(kv.Key) % nReduce

			keyHashMap[kHash] = append(keyHashMap[kHash], kv)
		}
	}

	fileLocations := make([]string, nReduce)

	for index, kva := range keyHashMap {
		fileLocation, err := WriteKeyValueToFile(workerId, index, kva)

		if err != nil {
			log.Fatalf("Something went bad %s", fileLocation)
			return nil, err
		}

		log.Printf("Map task completed, fileLocation for %d reduce task %s", index, fileLocation)

		fileLocations[index] = fileLocation
	}

	return fileLocations, nil
}

func ExecuteReduceTask(id int, fileLocations []string, reducef func(string, []string) string) (string, error) {
	kvp := make(map[string][]string)

	for _, fileName := range fileLocations {
		keyValues, err := ReadKeyValueFromFile(fileName)

		if err != nil {
			log.Fatalf("Cannot read from file %v", fileName)
			return "", err
		}

		for _, keyValue := range keyValues {
			kvp[keyValue.Key] = append(kvp[keyValue.Key], keyValue.Value)
		}
	}

	fileLocation, err := WriteReduceResultToFile(id, kvp, reducef)

	log.Printf("Reduce task completed, fileLocation for %d reduce task %s", id, fileLocation)

	if err != nil {
		return "", err
	}

	return fileLocation, nil
}

func WriteReduceResultToFile(index int, data map[string][]string, reducef func(string, []string) string) (string, error) {
	fileName := fmt.Sprintf("mr-out-%d", index)

	fileLocation := GetFileLocation(fileName)

	tempFile, err := os.CreateTemp("", "tempfile")
	if err != nil {
		log.Fatalf("cannot create temporary file: %v", err)
		return "", err
	}
	defer tempFile.Close()

	for key, value := range data {
		result := reducef(key, value)

		fmt.Fprintf(tempFile, "%v %v\n", key, result)
	}

	err = os.Rename(tempFile.Name(), fileLocation)
	if err != nil {
		log.Fatalf("cannot rename temporary file: %v", err)
		return "", err
	}

	return fileLocation, nil
}

func ReadAndMapFromFile(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(filename)

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	kva := mapf(filename, string(content))
	return kva
}

func ReadKeyValueFromFile(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file %s: %v", filename, err)
	}
	defer file.Close()

	var kva []KeyValue // Declare and initialize kva as an empty slice of KeyValue
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var kv KeyValue
		err := decoder.Decode(&kv)
		if err != nil {
			return nil, fmt.Errorf("error decoding JSON from file %s: %v", filename, err)
		}
		kva = append(kva, kv)
	}

	return kva, nil
}

func WriteKeyValueToFile(workerId string, index int, kva []KeyValue) (string, error) {

	fileName := "mr_" + strconv.Itoa(index) + "_" + workerId

	fileLocation := GetFileLocation(fileName)

	// Create a temporary file
	file, err := os.CreateTemp("", "tempfile")
	if err != nil {
		log.Fatalf("cannot create temporary file: %v", err)
		return "", err
	}
	defer file.Close()

	// Write each line to the file
	encoder := json.NewEncoder(file)
	for _, kv := range kva {
		err := encoder.Encode(kv)
		if err != nil {
			log.Fatalf("cannot write to file %s", fileLocation)
			return "", err
		}
	}

	//now rename the temp file to fileLocation
	err = os.Rename(file.Name(), fileLocation)
	if err != nil {
		log.Fatalf("cannot rename temporary file: %v", err)
		return "", err
	}

	return fileLocation, nil
}

func GetFileLocation(fileName string) string {
	// dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))

	// return fmt.Sprintf("%s/%s", dir, fileName)

	return fileName
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return nil
	}

	fmt.Println(err)
	return err
}
