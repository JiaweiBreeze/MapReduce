# mapreduce

MapReduce 是一种分布式计算框架，主要用于大规模数据集的处理。它由谷歌公司于2004年提出，并成为大数据处理领域的核心技术之一。MapReduce 的工作流程包括两个主要阶段：Map（映射）阶段和 Reduce（归约）阶段。
MapReduce在大数据处理领域有广泛应用，包括但不限于：

- 数据分析：大规模日志数据分析、用户行为分析等。
- 搜索引擎：索引构建、页面排名计算等。
- 机器学习：训练大规模数据集的模型。
---

## Map阶段
在Map阶段，输入数据被分割成小块，称为“输入切片”（Input Splits），然后由多个Map任务并行处理。每个Map任务接收一个输入切片，并将其处理成一系列键值对（key-value pairs）。这些键值对根据键的值被分类并

## Shuffle and Sort阶段
Map阶段生成的键值对会在此阶段进行洗牌和排序。相同键的所有键值对会被分配到同一个Reduce任务中。这个过程确保了相同键的数据能够集中处理


## Reduce阶段
在Reduce阶段，每个Reduce任务接收来自Map任务的分类和排序后的键值对。Reduce任务将相同键的值进行汇总、合并或其他操作，生成最终的输出结果。这个阶段的输出通常也是一系列键值对，可以进一步处理或存储

## MapReduce的执行流程

根据论文第三节，MapReduce的执行流程分这么几步：

> 1. 启动Coordinator，接受n个输入文件，创建n个map任务，余下的节点成为worker。Coordinator为server服务，处理来自worker的rpc请求，包括分发任务、标记任务状态、对于异常状态例如超时、worker 宕机等问题处理。worker负责执行map操作和reduce操作。

> 2. 空闲的worker调用assign rpc请求，从coordinator获取一个任务，任务可能是map阶段、可能是reduce阶段，完成后将结果写至持久化存储，例如GPS等，并调用 completed rpc请求，将结果返回给coordinator。map阶段，worker接收切分后的input，执行Map函数，将结果缓存到内存，并切分成R份（reducer数量）。缓存后的中间结果会周期性的写到本地磁盘，并切分成R份（reducer数量）。R个文件的位置会发送给coordinator, coordinator转发给reducer。reduce阶段，worker收到中间文件的位置信息，通过RPC读取。读取完先根据中间<k, v>排序，然后按照key分组、合并。之后在排序后的数据上迭代，将中间<k, v> 交给reduce 函数处理。最终结果写给对应的output文件（分片）

> 3. coordinator处理来自worker的计算结果。对于map 的结果，coordinator将所有worker的map结果，，保证每个reducer处理不同的key范围。对于reduce的结果，则不做处理，将本次结果记录为成功即退出并唤醒用户程序


---

## MapReduce实现

### Coordinator数据结构设计

每个(Map或者Reduce)Task有分为idle, in-progress, completed 三种状态。

```go
type CoordinateTaskStatus int

const (
	Idle CoordinateTaskStatus = iota // waiting to be processed
	InProgress
	Completed
)
```

Coordinator存储这些Task的信息。

```go
type CoordinateTask struct {
	TaskStatus    CoordinateTaskStatus
	StartTime     time.Time
	TaskReference *Task
}
```

coordinator存储任务队列、map阶段存储的中间结果、任务状态、最终结果

```go
type Coordinator struct {
	TaskQueue       chan *Task
	TaskMeta        map[int]*CoordinateTask
	CoordinatePhase State
	NReduce         int
	Inputfiles      []string
	MapResults      [][]string
}
```
任务信息

```go
type Task struct {
	Input      string
	TaskState  State
	NReducer   int
	TaskID     int
	MapResults []string
	Output     string
}
```

任务状态

```go
type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)
```

### MapReduce执行过程

**1. 启动Coordinator**

```go
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
```

**2. coordinator RPC调用，分配任务**

```go
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
```

**3. 启动worker**

```go
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task := getTask()

		switch task.TaskState {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}

}
```

**4. worker向coordinator发送RPC请求任务**

```go
func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	call("Coordinator.AssignTask", &args, &reply)
	return reply
}
```

**5. worker获得MapTask，交给mapper处理**

```go
func mapper(task *Task, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(task.Input)
	if err != nil {
		log.Fatal("Failed to read file: "+task.Input, err)
	}

	intermediates := mapf(task.Input, string(content))

	//根据key做hash，再根据reduceid做hash分布
	buffer := make([][]KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReducer
		buffer[slot] = append(buffer[slot], intermediate)
	}

	mapOutput := make([]string, 0)
	for i := 0; i < task.NReducer; i++ {
		mapOutput = append(mapOutput, writeToLocalFile(task.TaskID, i, &buffer[i]))
	}

	task.MapResults = mapOutput
	TaskCompleted(task)
}
```

**6. worker任务完成后通知coordinator**

```go
func TaskCompleted(task *Task) {
	reply := ExampleReply{}
	call("Coordinator.TaskCompleted", task, &reply)
}
```

**7. coordinator收到完成后的Task**

+ 如果所有的MapTask都已经完成，创建ReduceTask，转入Reduce阶段
+ 如果所有的ReduceTask都已经完成，转入Exit阶段

```go
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
```

判断是否当前阶段所有任务都已经完成

```go
func (c *Coordinator) allTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}
```

**8. 转入Reduce阶段，worker获得ReduceTask，交给reducer处理**

```go
func reducer(task *Task, reducef func(string, []string) string) {
	mapOutputs := *readFromLocalFile(task.MapResults)
	sort.Sort(ByKey(mapOutputs))

	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create tmp file", err)
	}

	//找到相同的key
	i := 0
	for i < len(mapOutputs) {
		j := i + 1
		for j < len(mapOutputs) && mapOutputs[j].Key == mapOutputs[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, mapOutputs[k].Value)
		}

		output := reducef(mapOutputs[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", mapOutputs[i].Key, output)
		i = j
	}

	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskID)
	os.Rename(tempFile.Name(), oname)
	task.Output = os.ModeNamedPipe.String()
	TaskCompleted(task)
}
```

**9. coordinator确认所有ReduceTask都已经完成，转入Exit阶段**

```go
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()

	ret := c.CoordinatePhase == Exit

	// Your code here.

	return ret
}
```



**10. carsh处理**

test当中有容错的要求，不过只针对worker。mapreduce论文中提到了：

1. 周期性向worker发送心跳检测
+ 如果worker失联一段时间，coordinator将worker标记成failed
+ worker失效之后
  + 已完成的map task被重新标记为idle
  + 已完成的reduce task不需要改变
  + 原因是：map的结果被写在local disk，worker machine 宕机会导致map的结果丢失；reduce结果存储在GFS，不会随着machine down丢失
2. 对于in-progress 且超时的任务，启动backup执行


1. 周期性检查task是否完成。将超时未完成的任务，交给新的worker，backup执行

```go
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
```


---

## 测试
执行`test-mr.sh`

