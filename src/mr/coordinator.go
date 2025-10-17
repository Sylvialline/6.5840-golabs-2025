package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type taskT struct {
	kind string // in {"map", "reduce"}
	tid int // map/reduce task id
	mapInput string
	reduceInput []string
}

type assignT struct {
	wid int // worker id
	task taskT
}

const TTL = 10 * time.Second // Time To Live: 10s
type startT struct {
	assigned assignT
	startTime time.Time
}


type doneT struct {
	wid int
	kind string
	mapOutput []string // mapOutput[i]表示应该被第i个reduce任务读入的文件名
	reduceOutput string
}

type Coordinator struct {
	// Your definitions here.
	nMap int
	nReduce int
	nMapDone int
	nReduceDone int
	doneCh chan bool

	mapTasks []taskT
	reduceTasks []taskT
	
	inflight map[int]startT // in-flight tasks: wid -> started task

	assignQ chan assignT
	startQ chan startT
	doneQ chan doneT
}

// Your code here -- RPC handlers for the worker to call.

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

	// Your code here.
	select{
	case <-c.doneCh:
		return true
	default:
		return false
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
	c.nMap = len(files)
	c.nReduce = nReduce

	// init c.mapTasks
	for i, file := range files {
		c.mapTasks = append(c.mapTasks, taskT{
			kind: "map",
			tid: i,
			mapInput: file,
		})
	}

	// init c.reduceTasks
	c.reduceTasks = make([]taskT, nReduce)
	for i, task := range c.reduceTasks {
		task.kind = "reduce"
		task.tid = i
	}

	c.assignQ = make(chan assignT)
	c.startQ = make(chan startT)
	c.doneQ = make(chan doneT)

	go c.master()

	c.server()
	return &c
}

var widCounter = 0

func makeAssign(task taskT) (assigned assignT) {
	widCounter ++
	assigned.wid = widCounter
	assigned.task = task
	return
}

func makeStart(assigned assignT) (started startT) {
	started.assigned = assigned
	started.startTime = time.Now()
	return
}

func (c *Coordinator) handleMapDone(mapOutput []string) {
	c.nMapDone ++
	for i, file := range mapOutput {
		c.reduceTasks[i].reduceInput = append(c.reduceTasks[i].reduceInput, file)
	}
	
	if c.nMapDone == c.nMap {
		for _, task := range c.reduceTasks {
			c.assignQ <- makeAssign(task)
		}
	}
}

func (c *Coordinator) handleReduceDone(reduceOutput string, tid int) {
	c.nReduceDone ++
	os.Rename(reduceOutput, fmt.Sprintf("mr-out-%v", tid))
	
	if c.nReduceDone == c.nReduce {
		c.doneCh <- true
	}
}

func (c *Coordinator) handleCrash(task taskT) {
	c.assignQ <- makeAssign(task)
}

func (c *Coordinator) handleTimeout() {
	for wid, started := range c.inflight {
		// 如果任务started超过10s还在c.inflight中，则认为这个worker崩溃了
		if time.Since(started.startTime) >= TTL {
			c.handleCrash(started.assigned.task)
			delete(c.inflight, wid) // go 保证边遍历边删除合法
		}
	}
}

func (c *Coordinator) master() {
	for _, task := range c.mapTasks {
		c.assignQ <- makeAssign(task)
	}

	for{
		select{
		case started := <-c.startQ:
			c.inflight[started.assigned.wid] = started

		case done := <-c.doneQ:
			tid := -1
			for wid, started := range c.inflight {
				if wid == done.wid {
					tid = started.assigned.task.tid
					delete(c.inflight, wid)
					break
				}
			}
			if tid == -1 { break }

			switch done.kind {
			case "map":
				c.handleMapDone(done.mapOutput)
			case "reduce":
				c.handleReduceDone(done.reduceOutput, tid)
			}

		default:
			time.Sleep(100 * time.Millisecond)
			c.handleTimeout()
		}
	}
}

// rpc types and handlers

type Empty struct{}
type RequestReply struct{
	Wid int
	Kind string
	MapInput string
	NReduce int
	ReduceInput []string
}
type CompleteArgs struct{
	Wid int
	Kind string
	MapOutput []string
	ReduceOutput string
}

func (c *Coordinator) RequestTask(empty *Empty, reply *RequestReply) error {
	assigned := <- c.assignQ
	reply.Wid = assigned.wid
	reply.Kind = assigned.task.kind

	switch reply.Kind {
	case "map":
		reply.MapInput = assigned.task.mapInput
		reply.NReduce = c.nReduce
	case "reduce":
		reply.ReduceInput = assigned.task.reduceInput
	}

	c.startQ <- makeStart(assigned)
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteArgs, empty *Empty) error {
	done := doneT{}
	done.wid = args.Wid
	done.kind = args.Kind
	
	switch done.kind {
	case "map":
		done.mapOutput = args.MapOutput
	case "reduce":
		done.reduceOutput = args.ReduceOutput
	}

	c.doneQ <- done
	return nil
}