package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//心跳

	for {
		//向主节点申请任务
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if ok {
			task := reply.Task
			if task.TaskType == NoTask {
				fmt.Printf("No More Task!\n")
				break
			}
			switch task.TaskType {
			case Map:
				doMap(task, mapf)
			case Reduce:
				doReduce(task, reducef)
			case Wait:
				time.Sleep(time.Second)
			}
		} else {
			fmt.Printf("call failed!\n")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMap(task Task, mapf func(string, string) []KeyValue) {
	fileName := task.FileNames[0]
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))

	hashedKva := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		hashedKva[ihash(kv.Key)%task.NReduce] = append(hashedKva[ihash(kv.Key)%task.NReduce], kv)
	}
	tmpFileNames := make([]string, 0)
	for i := 0; i < task.NReduce; i++ {
		tmpFileName := "mr-" + strconv.Itoa(task.ID) + "-" + strconv.Itoa(i)
		tmpFileNames = append(tmpFileNames, tmpFileName)
		ofile, _ := os.Create(tmpFileName)
		enc := json.NewEncoder(ofile)
		for _, kv := range hashedKva[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
	args := TaskDoneArgs{task.ID, tmpFileNames}
	call("Master.TaskDone", &args, nil)
}

func doReduce(task Task, reducef func(string, []string) string) {

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
