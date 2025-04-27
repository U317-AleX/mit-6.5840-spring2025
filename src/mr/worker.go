package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	RegisterWorkerArgs := RegisterWorkerArgs{}

	RegisterWorkerReply := RegisterWorkerReply{
		WorkerID: 0,
		ReduceTasks: 0,
	}

	ok := call("Coordinator.RegisterWorker", &RegisterWorkerArgs, &RegisterWorkerReply)

	if !ok {
		log.Fatal("RegisterWorker failed")
	}

	workerID := RegisterWorkerReply.WorkerID
	reduceTasks := RegisterWorkerReply.ReduceTasks

	for {
		// Request work from the coordinator
		RequestWorkArgs := RequestWorkArgs{
			WorkerID: workerID,
		}

		RequestWorkReply := RequestWorkReply{
			WorkerID: 0,
			WorkType: "",
			FileName: "",
			mapTasksID: 0,
			reduceTaskID: 0,
		}

		ok := call("Coordinator.RequestWork", &RequestWorkArgs, &RequestWorkReply)

		if !ok {
			log.Fatal("RequestWork failed")
		}

		if RequestWorkReply.WorkType == "" {
			// No more work to do, exit the loop
			log.Printf("worker " + strconv.Itoa(workerID) + " exited")
			return
		}

		if RequestWorkReply.WorkType == "map" {
			file, err := os.Open(RequestWorkReply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", RequestWorkReply.FileName)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", RequestWorkReply.FileName)
			}
			file.Close()
			kva := mapf(RequestWorkReply.FileName, string(content))
			
			// create intermediate files for each reduce task
			for i := 0; i < reduceTasks; i++ {
				oname := "mr-" + strconv.Itoa(RequestWorkReply.mapTasksID) + "-" + strconv.Itoa(i)
				ofile, err := os.Create(oname)
				if err != nil {
					log.Fatalf("cannot create %v", oname)
				}
				enc := json.NewEncoder(ofile)
				for _, kv := range kva {
					if ihash(kv.Key) % reduceTasks == i {
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatalf("cannot encode %v", kv)
						}
					}
				}
				ofile.Close()
			}

			// send the intermediate files to the coordinator
			WorkFinishedArgs := WorkFinishedArgs{
				WorkerID: workerID,
				WorkType: "map",
				FileName: RequestWorkReply.FileName,
				mapTasksID: RequestWorkReply.mapTasksID,
			}

			WorkFinishedReply := WorkFinishedReply{}

			ok = call("Coordinator.WorkFinished", &WorkFinishedArgs, &WorkFinishedReply)
			if !ok {
				log.Fatal("WorkFinished failed")
			}
		}

		if RequestWorkReply.WorkType == "reduce" {
			// open files for the reduce task
			kva := []KeyValue{}
			files, err := filepath.Glob("mr-*-" + strconv.Itoa(RequestWorkReply.reduceTaskID))
			if err != nil {
				log.Fatalf("cannot find files for reduce task %d", RequestWorkReply.reduceTaskID)
			}

			for _, file := range files {
				f, err := os.Open(file)
				if err != nil {
					log.Fatalf("cannot open %v", file)
				}
				
				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				f.Close()
			}

			// sort the intermediate key/value pairs
			sort.Sort(ByKey(kva))

			// create the output file for the reduce task
			oname := "mr-out-" + strconv.Itoa(RequestWorkReply.reduceTaskID)
			ofile, err := os.Create(oname)
			if err != nil {
				log.Fatalf("cannot create %v", oname)
			}

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			ofile.Close()

			// send the reduce task finished message to the coordinator
			WorkFinishedArgs := WorkFinishedArgs{
				WorkerID: workerID,
				WorkType: "reduce",
				reduceTaskID: RequestWorkReply.reduceTaskID,
			}

			WorkFinishedReply := WorkFinishedReply{}

			ok = call("Coordinator.WorkFinished", &WorkFinishedArgs, &WorkFinishedReply)
			if !ok {
				log.Fatal("WorkFinished failed")
			}
		}
	}
}

// uncomment to send the Example RPC to the coordinator.
// CallExample()
//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}