package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)


type Coordinator struct {
	// Your definitions here.
	wokerIDs			[]int					// worker IDs
	workerNum 			int						// number of workers
	fileNames 			[]string				// input file names
	maptasks			int						// map tasks
	reducetasks			int						// reduce tasks
	mapTasksStatus		map[string]bool			// filename -> bool, true means finished, 
												// false means not finished
	reduceTasksStatus	map[int]bool			// int -> bool, we have nReduce reduce tasks
												// false means not finished
	finishedMapTasks	int						// finished map tasks
	finishedReduceTasks	int						// finished reduce tasks
	rwlock				sync.RWMutex			// read-write lock for syn-safity
}

// Your code here -- RPC handlers for the worker to call.
// Your code here.

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.rwlock.Lock()
	reply.WorkerID = len(c.wokerIDs) + 1
	reply.ReduceTasks = c.reducetasks
	c.wokerIDs = append(c.wokerIDs, reply.WorkerID)
	c.workerNum++
	c.rwlock.Unlock()

	log.Printf("Worker %d registered\n", reply.WorkerID)
	return nil
}

func (c *Coordinator) RequestWork(args *RequestWorkArgs, reply *RequestWorkReply) error {
	c.rwlock.RLock()

	if c.finishedMapTasks < c.maptasks {
		reply.WorkType = "map"
		for i, file := range c.fileNames {
			if !c.mapTasksStatus[file] {
				reply.FileName = file
				reply.mapTasksID = i
				return nil
			}
		}
	}

	// only the workers with IDs <= nReduce can do reduce work
	if c.finishedMapTasks >= c.maptasks && c.finishedReduceTasks < c.reducetasks && args.WorkerID <= c.reducetasks {
		reply.WorkType = "reduce"
		for i := 0; i < c.reducetasks; i++ {
			if !c.reduceTasksStatus[i] {
				reply.reduceTaskID = i
				return nil
			}
		}
	}

	c.rwlock.RUnlock()

	reply.WorkType = ""	// no more work to do

	return nil
}

func (c *Coordinator) WorkFinished(args *WorkFinishedArgs, reply *WorkFinishedReply) error {
	c.rwlock.Lock()
	if args.WorkType == "map" {
		c.mapTasksStatus[args.FileName] = true	
		c.finishedMapTasks++
		return nil
	}

	if args.WorkType == "reduce" {
		c.reduceTasksStatus[args.reduceTaskID] = true	
		c.finishedReduceTasks++
		return nil
	}
	c.rwlock.Unlock()

	log.Printf("Worker %d finished work\n", args.WorkerID)
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	// Your code here.
	c.rwlock.RLock()
	if c.finishedMapTasks == c.maptasks && c.finishedReduceTasks == c.reducetasks {
		ret = true
	}
	c.rwlock.RUnlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// initialize the coordinator
	c.fileNames = files
	c.maptasks = len(files)
	c.reducetasks = nReduce
	c.wokerIDs = make([]int, 0)
	c.workerNum = 0
	c.mapTasksStatus = make(map[string]bool)
	c.reduceTasksStatus = make(map[int]bool)
	c.finishedMapTasks = 0
	c.finishedReduceTasks = 0

	for _, file := range files {
		c.mapTasksStatus[file] = false	// set status to false (not finished)
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasksStatus[i] = false	// set status to false (not finished)
	}



	c.server()
	return &c
}
