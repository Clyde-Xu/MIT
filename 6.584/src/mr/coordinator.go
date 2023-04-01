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
)

type TaskState int
type TaskType int

const (
	NotStarted TaskState = iota
	InProgress
	Done
)

const (
	None TaskType = iota
	Map
	Reduce
)

type MapTask struct {
	fileName string
	mapState TaskState
}

type ReduceTask struct {
	reduceState TaskState
}

type WorkerTask struct {
	taskType           TaskType
	index              int
	startTimeInSeconds int64
}

type Coordinator struct {
	// Your definitions here.
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	workers     map[int]WorkerTask

	mu sync.Mutex
}

func (c *Coordinator) Initialize(files []string, reduceNumber int) {
	c.workers = make(map[int]WorkerTask)
	c.mapTasks = make([]MapTask, len(files))
	for index, name := range files {
		c.mapTasks[index].fileName = name
	}
	c.reduceTasks = make([]ReduceTask, reduceNumber)
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args TaskArgs, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Mark the previous task assigned as done.
	if val, ok := c.workers[args.WorkerId]; ok {
		if val.taskType == Map {
			if args.Done {
				c.mapTasks[val.index].mapState = Done
			} else {
				c.mapTasks[val.index].mapState = NotStarted
			}
		} else if val.taskType == Reduce {
			if args.Done {
				c.reduceTasks[val.index].reduceState = Done
			} else {
				c.reduceTasks[val.index].reduceState = NotStarted
			}
		}
		// reset the start time.
		val.index = -1
		val.startTimeInSeconds = 0
		val.taskType = None
		c.workers[args.WorkerId] = val
	}
	mapDone := true
	// assign a map task.
	for index, task := range c.mapTasks {
		mapDone = mapDone && task.mapState == Done
		if task.mapState == NotStarted {
			reply.Cmd = task.fileName
			reply.ReduceNumber = len(c.reduceTasks)
			workerTask := WorkerTask{
				taskType:           Map,
				index:              index,
				startTimeInSeconds: time.Now().Unix(),
			}
			c.workers[args.WorkerId] = workerTask
			c.mapTasks[index].mapState = InProgress
			return nil
		}
	}
	// map not finished, and all tasks assigned, send back "wait"
	if !mapDone {
		reply.Cmd = "wait"
		return nil
	}
	reduceDone := true
	for index, task := range c.reduceTasks {
		reduceDone = reduceDone && task.reduceState == Done
		if task.reduceState == NotStarted {
			reply.Cmd = "reduce-" + strconv.Itoa(index)
			reply.ReduceNumber = len(c.reduceTasks)
			workerTask := WorkerTask{
				taskType:           Reduce,
				index:              index,
				startTimeInSeconds: time.Now().Unix(),
			}
			c.workers[args.WorkerId] = workerTask
			c.reduceTasks[index].reduceState = InProgress
			return nil
		}
	}
	// reduce not finished, and all tasks assigned, send back "wait"
	if !reduceDone {
		reply.Cmd = "wait"
		return nil
	}
	// all tasks finished.
	reply.Cmd = "please exit"
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for _, task := range c.reduceTasks {
		if task.reduceState != Done {
			// check whether there are hang workers.
			for key, worker := range c.workers {
				if worker.startTimeInSeconds != 0 {
					diff := now.Unix() - worker.startTimeInSeconds
					if diff > 15 {
						if worker.taskType == Map {
							c.mapTasks[worker.index].mapState = NotStarted
						} else if worker.taskType == Reduce {
							c.reduceTasks[worker.index].reduceState = NotStarted
						}
						worker.taskType = None
						worker.startTimeInSeconds = 0
						c.workers[key] = worker
					}
				}
			}
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Initialize(files, nReduce)

	c.server()
	return &c
}
