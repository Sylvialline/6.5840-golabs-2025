package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
