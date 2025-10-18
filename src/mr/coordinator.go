package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	kvsrv "6.5840/kvsrv1"
)

const MAX_CHANNEL_SIZE = 100

type taskT struct {
	kind string // in {"map", "reduce"}
	tid int // map/reduce task id
	mapInput string
	reduceInput []string
}

type assignT struct {
	wid int // worker id, 唯一标识任务的一次执行
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
	
	wakeCh chan int
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
	for i := range c.reduceTasks {
		c.reduceTasks[i].kind = "reduce"
		c.reduceTasks[i].tid = i
	}

	c.assignQ = make(chan assignT, MAX_CHANNEL_SIZE)
	c.startQ = make(chan startT, MAX_CHANNEL_SIZE)
	c.doneQ = make(chan doneT, MAX_CHANNEL_SIZE)

	c.doneCh = make(chan bool, 1)
	c.wakeCh = make(chan int, MAX_CHANNEL_SIZE)

	c.inflight = map[int]startT{}

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

	// 重命名为正式输出
	os.Rename(reduceOutput, fmt.Sprintf("mr-out-%v", tid))
	
	if c.nReduceDone == c.nReduce {
		c.doneCh <- true
	}
}

func (c *Coordinator) handleCrash(task taskT) {
	c.assignQ <- makeAssign(task)
}

func (c *Coordinator) startTimer(wid int, duration time.Duration) {
	time.Sleep(duration)
	c.wakeCh <- wid
}

func (c *Coordinator) master() {
	kvsrv.DPrintf("master start")
	for _, task := range c.mapTasks {
		// kvsrv.DPrintf("%v\n", task)
		c.assignQ <- makeAssign(task)
	}
	kvsrv.DPrintf("master for-select")
	for{
		select{
		case started := <-c.startQ:
			kvsrv.DPrintf("master case started")
			wid := started.assigned.wid
			c.inflight[wid] = started
			go c.startTimer(wid, TTL)

		case done := <-c.doneQ:
			kvsrv.DPrintf("master case done")
			started, exist := c.inflight[done.wid]
			if !exist {
				// 这个任务已经超时，或者被本来判定为超时的任务抢先完成
				// 抛弃这个任务
				break
			}
			tid := started.assigned.task.tid
			delete(c.inflight, done.wid)

			switch done.kind {
			case "map":
				c.handleMapDone(done.mapOutput)
			case "reduce":
				c.handleReduceDone(done.reduceOutput, tid)
			}

		case wid := <-c.wakeCh:
			kvsrv.DPrintf("master case wid")
			started, exist := c.inflight[wid]
			if exist {
				// 这个任务在inflight中超过10s，认为崩溃
				delete(c.inflight, wid)
				c.handleCrash(started.assigned.task)
			}
		}
	}
}
