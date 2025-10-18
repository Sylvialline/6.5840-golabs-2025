package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"

	kvsrv "6.5840/kvsrv1"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValues struct {
	// 因为要写入文件，所以导出
	Key    string
	Values []string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func callRequest() (reply RequestReply) {
	ok := call("Coordinator.RequestTask", &Empty{}, &reply)
	if !ok {
		// 认为coordinator已经正常退出
		kvsrv.DPrintf("yay")
		os.Exit(0)
	}
	return
}

func callComplete(args *CompleteArgs) {
	call("Coordinator.CompleteTask", args, &Empty{})
}

// for sorting by key.
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func groupByKey(kva []KeyValue) []KeyValues {
	var groups []KeyValues
	sort.Sort(ByKey(kva))
	a := 0
	for a < len(kva) {
		b := a + 1
		for b < len(kva) && kva[a].Key == kva[b].Key {
			b++
		}
		Values := make([]string, b-a)
		for c := a; c < b; c++ {
			Values[c-a] = kva[c].Value
		}
		groups = append(groups, KeyValues{kva[a].Key, Values})
		a = b
	}
	return groups
}

func unionByKey(kvsa []KeyValues) []KeyValues {
	var groups []KeyValues

	// 另一种排序方法，用sort.Slice
	sort.Slice(kvsa, func(i, j int) bool {
		return kvsa[i].Key < kvsa[j].Key
	})

	a := 0
	for a < len(kvsa) {
		b := a + 1
		for b < len(kvsa) && kvsa[a].Key == kvsa[b].Key {
			b++
		}
		Values := []string{}
		for c := a; c < b; c++ {
			Values = append(Values, kvsa[c].Values...)
		}
		groups = append(groups, KeyValues{kvsa[a].Key, Values})
		a = b
	}
	return groups
}

func handleMap(wid int, nReduce int, mapInput string, 
	mapf func(string, string) []KeyValue) (mapOutput []string) {
	
	content := readContent(mapInput)

	kva := mapf(mapInput, string(content))
	bucket := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		i := ihash(kv.Key) % nReduce
		bucket[i] = append(bucket[i], kv)
	}

	mapOutput = make([]string, nReduce)

	for i := range nReduce {
		mapOutput[i] = fmt.Sprintf("mr-map-%v-%v", wid, i)
		writeJSON(mapOutput[i], groupByKey(bucket[i]))
	}

	return
}

func handleReduce(wid int, reduceInput []string, 
	reducef func(string, []string) string) (reduceOutput string) {

	intermediate := []KeyValues{}
	for _, filename := range reduceInput {
		kvsa := readJSON(filename)
		intermediate = append(intermediate, kvsa...)
	}
	intermediate = unionByKey(intermediate)

	reduceOutput = fmt.Sprintf("mr-reduce-%v", wid)
	ofile, err := os.Create(reduceOutput)
	if err != nil {
		log.Fatalf("handleReduce: create create: %v", reduceOutput)
	}
	
	for _, kvs := range intermediate {
		output := reducef(kvs.Key, kvs.Values)
		fmt.Fprintf(ofile, "%v %v\n", kvs.Key, output)
	}

	return
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := callRequest()
		args := CompleteArgs{
			Wid: reply.Wid,
			Kind: reply.Kind,
		}
		kvsrv.DPrintf("Worker#%v: %v\n", args.Wid, args.Kind)

		switch reply.Kind {
		case "map":
			args.MapOutput = handleMap(reply.Wid, reply.NReduce, reply.MapInput, mapf)
			kvsrv.DPrintf("Worker#%v:\nMapInput: %#v\nMapOutput: %#v\n\n", args.Wid, reply.MapInput, args.MapOutput)
		case "reduce":
			args.ReduceOutput = handleReduce(reply.Wid, reply.ReduceInput, reducef)
			kvsrv.DPrintf("Worker#%v:\nReduceInput: %#v\nReduceOutput: %#v\n\n", args.Wid, reply.ReduceInput, args.ReduceOutput)
		}

		callComplete(&args)
	}
	


	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func readContent(filename string) []byte {
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("readContent: cannot read %v", filename)
	}

	return content
}

func writeJSON(filename string, data []KeyValues) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("writeJSON: connot open %v", filename)
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	err = enc.Encode(data)
	if err != nil {
		log.Fatalf("writeJSON: connot encode %+v", data)
	}
}

func readJSON(filename string) []KeyValues {
	var data []KeyValues
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("readJSON: connot open %v", filename)
	}
	defer file.Close()

	json.NewDecoder(file).Decode(&data)
	if err != nil {
		log.Fatalf("readJSON: connot decode %v: %v", filename, err)
	}
	return data
}
