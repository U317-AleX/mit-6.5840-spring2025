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
	mapTasksStatus		map[string]int			// filename -> int, 0: unsigned, 1: signed, 
												// 2: finished, 
	reduceTasksStatus	map[int]int				// int -> int, we have nReduce reduce tasks
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
	c.rwlock.Lock()

	if c.finishedMapTasks < c.maptasks {
		reply.WorkType = "map"
		for i, file := range c.fileNames {
			if c.mapTasksStatus[file] == 0 {
				c.mapTasksStatus[file] = 1
				reply.FileName = file
				reply.mapTasksID = i
				c.rwlock.Unlock()

				// sleep(10)
				// c.rwlock.Lock()
				// if c.mapTasksStatus[file] != 2 {
				// 		c.mapTasksStatus[file] = 0
				// }
				// c.rwlock.Unlock()
				return nil
			}
		}
	}

	if c.finishedMapTasks == c.maptasks && c.finishedReduceTasks < c.reducetasks {
		reply.WorkType = "reduce"
		for i := 0; i < c.reducetasks; i++ {
			if c.reduceTasksStatus[i] == 0 {
				c.reduceTasksStatus[i] = 1
				reply.reduceTaskID = i
				c.rwlock.Unlock()

				// sleep(10)
				// c.rwlock.Lock()
				// if c.mapTasksStatus[i] != 2 {
				// 		c.mapTasksStatus[i] = 0
				// }
				// c.rwlock.Unlock()
				return nil
			}
		}
	}

	
	reply.WorkType = ""	// no more work to do
	
	c.rwlock.Unlock()
	return nil
}

func (c *Coordinator) WorkFinished(args *WorkFinishedArgs, reply *WorkFinishedReply) error {
	c.rwlock.Lock()
	if args.WorkType == "map" {
		if c.mapTasksStatus[args.FileName] == 1 {
			c.mapTasksStatus[args.FileName] = 2	
			c.finishedMapTasks++
		}
		c.rwlock.Unlock()
		return nil
	}

	if args.WorkType == "reduce" {
		if c.reduceTasksStatus[args.reduceTaskID] == 1 { 
			c.reduceTasksStatus[args.reduceTaskID] = 2	
			c.finishedReduceTasks++
		}
		c.rwlock.Unlock()
		return nil
	}
	
	log.Printf("Worker %d finished work\n", args.WorkerID)
	c.rwlock.Unlock()
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
	log.Printf("Coordinator server is listening on %s\n", sockname)
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
	c.mapTasksStatus = make(map[string]int)
	c.reduceTasksStatus = make(map[int]int)
	c.finishedMapTasks = 0
	c.finishedReduceTasks = 0

	for _, file := range files {
		c.mapTasksStatus[file] = 0	// set status to false (not finished)
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasksStatus[i] = 0	// set status to false (not finished)
	}



	c.server()
	return &c
}
