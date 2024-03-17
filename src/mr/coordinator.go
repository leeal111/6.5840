package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait4Task
	NoMoreTask
)

type TaskState int

const (
	Waiting TaskState = iota
	Running
	Finished
)

type CoordinatorState int

const (
	Mapping CoordinatorState = iota
	Reducing
	Done
)

type Workee struct {
	TaskID int
	timer  *time.Timer
}
type Task struct {
	TaskType     TaskType
	TaskState    TaskState
	FileNames    []string
	ResFileNames []string
}

type Coordinator struct {
	// Your definitions here.
	Workees          []Workee
	Tasks            []Task
	CoordinatorState CoordinatorState
	mutex            sync.Mutex
	NReduce          int
	TimeoutCh        chan int
	TimeoutDuration  time.Duration
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetWorkeeID(args *GetWorkeeIDArgs, reply *GetWorkeeIDReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	reply.WorkeeID = len(c.Workees)
	c.Workees = append(c.Workees, Workee{TaskID: -1, timer: time.NewTimer(c.TimeoutDuration)})
	go func() {
		timer := c.Workees[reply.WorkeeID].timer
		<-timer.C
		c.TimeoutCh <- reply.WorkeeID
	}()
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.CoordinatorState == Done {
		reply.TaskType = NoMoreTask
	} else {
		reply.TaskType = Wait4Task
	}
	for i, task := range c.Tasks {
		if task.TaskState == Waiting {
			reply.TaskType = task.TaskType
			reply.TaskID = i
			reply.FileNames = task.FileNames
			reply.NReduce = c.NReduce
			c.Workees[args.WorkeeID].TaskID = i
			c.Tasks[i].TaskState = Running
			break
		}
	}
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.Workees[args.WorkeeID].TaskID == -1 {
		fmt.Printf("error: 超时任务，舍弃\n")
		return nil
	}
	task := &c.Tasks[c.Workees[args.WorkeeID].TaskID]
	task.TaskState = Finished
	task.ResFileNames = args.ResultFileNames
	c.Workees[args.WorkeeID].TaskID = -1
	for _, task := range c.Tasks {
		if task.TaskState != Finished {
			return nil
		}
	}
	c.ChangeState()
	return nil
}

func (c *Coordinator) ChangeState() {
	if c.CoordinatorState == Mapping {
		tasks := []Task{}
		for i := 0; i < c.NReduce; i++ {
			tasks = append(tasks, Task{TaskType: Reduce, TaskState: Waiting})
		}
		for _, ctask := range c.Tasks {
			for j := range len(tasks) {
				tasks[j].FileNames = append(tasks[j].FileNames, ctask.ResFileNames[j])
			}
		}
		c.Tasks = append(c.Tasks, tasks...)
		c.CoordinatorState = Reducing
	} else if c.CoordinatorState == Reducing {
		c.CoordinatorState = Done
	}
}

func (c *Coordinator) RecvHeartBeat(args *RecvHeartBeatArgs, reply *RecvHeartBeatReply) error {
	workID := args.WorkeeID
	c.Workees[workID].timer.Reset(c.TimeoutDuration)
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
	ret := false

	// Your code here.
	ret = (c.CoordinatorState == Done)
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{CoordinatorState: Mapping, NReduce: nReduce,
		TimeoutCh: make(chan int), TimeoutDuration: 5 * time.Second}

	// Your code here.
	for _, file := range files {
		c.Tasks = append(c.Tasks, Task{TaskType: Map,
			TaskState: Waiting, FileNames: []string{file}})
	}
	// 超时处理
	go func() {
		for {
			workeeID := <-c.TimeoutCh
			fmt.Printf("workee %d 超时\n", workeeID)
			c.Tasks[c.Workees[workeeID].TaskID].TaskState = Waiting
			c.Workees[workeeID].TaskID = -1
		}
	}()
	c.server()
	return &c
}
