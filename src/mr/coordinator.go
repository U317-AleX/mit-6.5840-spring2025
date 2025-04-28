package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
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
    mapTasksStartTime map[string]time.Time		// map task start time
	reduceTasksStartTime map[int]time.Time		// reduce task start time
}

// Your code here -- RPC handlers for the worker to call.
// Your code here.

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.rwlock.RLock()
	reply.WorkerID = len(c.wokerIDs) + 1
	reply.ReduceTasks = c.reducetasks
	c.rwlock.RUnlock()

	c.rwlock.Lock()
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
				c.rwlock.Unlock()

				c.rwlock.Lock()
				c.mapTasksStatus[file] = 1
				c.mapTasksStartTime[file] = time.Now() // record the start time
				c.rwlock.Unlock()

				reply.FileName = file
				reply.MapTaskID = i

				log.Printf("Worker %d assigned map task %s taskID : %d \n", args.WorkerID, reply.FileName, reply.MapTaskID)

				return nil
			}
		}
	}

	if c.finishedMapTasks == c.maptasks && c.finishedReduceTasks < c.reducetasks {
		reply.WorkType = "reduce"
		for i := 0; i < c.reducetasks; i++ {
			if c.reduceTasksStatus[i] == 0 {
				c.rwlock.Unlock()

				c.rwlock.Lock()
				c.reduceTasksStatus[i] = 1
				c.reduceTasksStartTime[i] = time.Now() // record the start time
				c.rwlock.Unlock()

				reply.ReduceTaskID = i

				log.Printf("Worker %d assigned reduce task %d \n", args.WorkerID, reply.ReduceTaskID)

				return nil
			}
		}
	}

	reply.WorkType = "wait"
	log.Printf("Worker %d no work to do\n", args.WorkerID)
	
	c.rwlock.Unlock()
	return nil
}

func (c *Coordinator) WorkFinished(args *WorkFinishedArgs, reply *WorkFinishedReply) error {
	c.rwlock.Lock()
	if args.WorkType == "map" {
		if c.mapTasksStatus[args.FileName] != 2 {
			c.mapTasksStatus[args.FileName] = 2	
			c.finishedMapTasks++
		}
		c.rwlock.Unlock()
		log.Printf("Worker %d finished map task %s \n", args.WorkerID, args.FileName)
		return nil
	}

	if args.WorkType == "reduce" {
		if c.reduceTasksStatus[args.ReduceTaskID] != 2 { 
			c.reduceTasksStatus[args.ReduceTaskID] = 2	
			c.finishedReduceTasks++
		}
		c.rwlock.Unlock()
		log.Printf("Worker %d finished reduce task %d \n", args.WorkerID, args.ReduceTaskID)
		return nil
	}
	
	c.rwlock.Unlock()
	return nil
}

func (c *Coordinator) DoneTest(args *DoneArgs, reply *DoneReply) error {
	reply.Done = c.Done()

	if reply.Done {
		log.Printf("All tasks are done\n")
	} else {
		log.Printf("Tasks are not done yet\n")
	}

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

func (c *Coordinator) ticker() {
    for {
        time.Sleep(time.Second) // wake up every second

        c.rwlock.Lock()

        now := time.Now()
        timeout := 10 * time.Second

		// check if a map task is done
        for file, status := range c.mapTasksStatus {
            if status == 1 {
                startTime := c.mapTasksStartTime[file]
                if now.Sub(startTime) > timeout {
                    // time out
                    log.Printf("Map task %v timeout, reset.\n", file)
                    c.mapTasksStatus[file] = 0
                }
            }
        }

        // check if a reduce task is done
        for id, status := range c.reduceTasksStatus {
            if status == 1 {
                startTime := c.reduceTasksStartTime[id]
                if now.Sub(startTime) > timeout {
                    log.Printf("Reduce task %v timeout, reset.\n", id)
                    c.reduceTasksStatus[id] = 0
                }
            }
        }

        c.rwlock.Unlock()
    }
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
	c.mapTasksStartTime = make(map[string]time.Time)
	c.reduceTasksStartTime = make(map[int]time.Time)

	for _, file := range files {
		c.mapTasksStatus[file] = 0	// set status to false (not finished)
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasksStatus[i] = 0	// set status to false (not finished)
	}


	go c.ticker() // start a goroutine to check for timeouts
	c.server()
	return &c
}
