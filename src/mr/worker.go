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
	id := initWorkeeID()
	go func() {
		for {
			args := RecvHeartBeatArgs{id}
			call("Coordinator.RecvHeartBeat", &args, nil)
			time.Sleep(2 * time.Second)
		}
	}()

	//循环请求并执行任务
	for {
		args := GetTaskArgs{id}
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if ok {
			if reply.TaskType == NoMoreTask {
				fmt.Printf("No More Task!\n")
				break
			}
			if reply.TaskType == Wait4Task {
				//可能暂时没有任务
				fmt.Printf("Waiting!\n")
				time.Sleep(5 * time.Second)
				continue
			}
			var resFileNames []string
			switch reply.TaskType {
			case Map:
				resFileNames = doMap(reply, mapf)
			case Reduce:
				resFileNames = doReduce(reply, reducef)
			}
			args := TaskDoneArgs{id, resFileNames}
			call("Coordinator.TaskDone", &args, nil)
		} else {
			fmt.Printf("call failed!\n")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func initWorkeeID() int {
	args := GetWorkeeIDArgs{}
	reply := GetWorkeeIDReply{}
	ok := call("Coordinator.GetWorkeeID", args, &reply)
	if ok {
		fmt.Printf("Get WorkeeID:%d\n", reply.WorkeeID)
		return reply.WorkeeID
	} else {
		fmt.Printf("call failed!\n")
		return -1
	}
}

func doMap(reply GetTaskReply, mapf func(string, string) []KeyValue) []string {
	taskID := reply.TaskID
	fileName := reply.FileNames[0]

	nReduce := reply.NReduce
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

	hashedKva := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		hashedKva[ihash(kv.Key)%nReduce] = append(hashedKva[ihash(kv.Key)%nReduce], kv)
	}
	tmpFileNames := make([]string, 0)
	for i := 0; i < nReduce; i++ {
		tmpFileName := "mr-" + strconv.Itoa(taskID) + "-" + strconv.Itoa(i)
		tmpFileNames = append(tmpFileNames, tmpFileName)
		ofile, _ := os.Create(tmpFileName)
		enc := json.NewEncoder(ofile)
		for _, kv := range hashedKva[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
	return tmpFileNames
}

func doReduce(reply GetTaskReply, reducef func(string, []string) string) []string {
	taskID := reply.TaskID
	fileNames := reply.FileNames
	shuffleMap := shuffle(fileNames)
	resultKVs := make([]KeyValue, 0)
	for k, v := range shuffleMap {
		output := reducef(k, v)
		resultKVs = append(resultKVs, KeyValue{k, output})
	}

	oname := "mr-out-" + strconv.Itoa(taskID)
	ofile, _ := os.Create(oname)
	for _, v := range resultKVs {
		fmt.Fprintf(ofile, "%v %v\n", v.Key, v.Value)
	}
	ofile.Close()
	return []string{oname}
}

func shuffle(fileNames []string) map[string][]string {
	shuffledMap := make(map[string][]string)
	for _, fileName := range fileNames {
		fd, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(fd)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			shuffledMap[kv.Key] = append(shuffledMap[kv.Key], kv.Value)
		}
		fd.Close()
	}
	return shuffledMap
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
