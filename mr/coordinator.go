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

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type CoordinateTaskStatus int

const (
	Idle CoordinateTaskStatus = iota // waiting to be processed
	InProgress
	Completed
)

type Task struct {
	Input      string
	TaskState  State
	NReducer   int
	TaskID     int
	MapResults []string
	Output     string
}

type CoordinateTask struct {
	TaskStatus    CoordinateTaskStatus
	StartTime     time.Time
	TaskReference *Task
}

type Coordinator struct {
	TaskQueue       chan *Task
	TaskMeta        map[int]*CoordinateTask
	CoordinatePhase State
	NReduce         int
	Inputfiles      []string
	MapResults      [][]string
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

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

func (c Coordinator) createMapTask() {
	for idx, filename := range c.Inputfiles {
		taskMeta := Task{
			Input:     filename,
			TaskState: Map,
			NReducer:  c.NReduce,
			TaskID:    idx,
		}
		c.TaskQueue <- &taskMeta

		c.TaskMeta[idx] = &CoordinateTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	mu.Lock()

	defer mu.Unlock()

	if task.TaskState != c.CoordinatePhase || c.TaskMeta[task.TaskID].TaskStatus == Completed {
		//可能是因为网络原因重试任务的重复结果
		return nil
	}

	c.TaskMeta[task.TaskID].TaskStatus = Completed
	go c.processTaskResult(task)
	return nil
}

func (c *Coordinator) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()

	switch task.TaskState {
	case Map:
		for reducerTaskID, filepath := range task.MapResults {
			c.MapResults[reducerTaskID] = append(c.MapResults[reducerTaskID], filepath)
		}

		if c.allTaskDone() {
			c.createReduceTask()
			c.CoordinatePhase = Reduce
		}
	case Reduce:
		if c.allTaskDone() {
			c.CoordinatePhase = Exit
		}
	}
}

func (c *Coordinator) allTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()

	ret := c.CoordinatePhase == Exit

	// Your code here.

	return ret
}

func (c *Coordinator) createReduceTask() {
	c.TaskMeta = make(map[int]*CoordinateTask)
	for idx, files := range c.MapResults {
		taskMeta := Task{
			TaskState:  Reduce,
			NReducer:   c.NReduce,
			TaskID:     idx,
			MapResults: files,
		}
		c.TaskQueue <- &taskMeta
		c.TaskMeta[idx] = &CoordinateTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}
func (c *Coordinator) catchTimeout() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()

		if c.CoordinatePhase == Exit {
			mu.Unlock()
			return
		}

		for _, coorTask := range c.TaskMeta {
			if coorTask.TaskStatus == InProgress && time.Now().Sub(coorTask.StartTime) > 10*time.Second {
				c.TaskQueue <- coorTask.TaskReference
				coorTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:       make(chan *Task, max(nReduce, len(files))),
		TaskMeta:        make(map[int]*CoordinateTask),
		CoordinatePhase: Map,
		NReduce:         nReduce,
		Inputfiles:      files,
		MapResults:      make([][]string, nReduce),
	}

	//create map task into taskqueue
	c.createMapTask()

	//coordinator 成为server后，其他节点都是worker
	c.server()

	//handle task timeour error
	go c.catchTimeout()
	return &c
}

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	if len(c.TaskQueue) > 0 {
		//有就发出去
		*reply = *<-c.TaskQueue
		// 记录task的启动时间
		c.TaskMeta[reply.TaskID].TaskStatus = InProgress
		c.TaskMeta[reply.TaskID].StartTime = time.Now()
	} else if c.CoordinatePhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		// 没有task就让worker 等待
		*reply = Task{TaskState: Wait}
	}
	return nil
}
