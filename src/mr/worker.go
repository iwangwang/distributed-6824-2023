package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.  copy from mrsequential.go
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		reply := getTaskCall()

		switch reply.State {
		case MAPPING:
			fmt.Println("map task execute")
			workerMap(mapf, reply)

		case REDUCING:
			fmt.Println("reduce task execute")
			reduceMap(reducef, reply)

		case WAIT:
			// fmt.Println("wait")
			time.Sleep(time.Duration(1000) * time.Millisecond)
		case COMPLETE:
			fmt.Println("complete")
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func workerMap(mapf func(string, string) []KeyValue, reply RPCResponce) {
	intermediate := []KeyValue{}
	filename, _ := reply.Content["url"].(string)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	mapResultPersistent(intermediate, reply.Content["nReduce"].(int), reply.Content["fileidx"].(int))
}

func reduceMap(reducef func(string, []string) string, reply RPCResponce) {
	filepaths := reply.Content["reduceFilePaths"].([]string)
	intermediate := []KeyValue{}
	for _, filepath := range filepaths {
		file, _ := os.Open(filepath)
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reply.Content["reduceIdx"].(int))
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	ReduceTaskDoneCall(reply.Content["reduceIdx"].(int))
}

func mapResultPersistent(intermediate []KeyValue, nReduce int, fileidx int) {
	tmpFiles := make([]*os.File, nReduce)
	encoder := make([]*json.Encoder, nReduce)
	filepaths := make([]string, nReduce)
	// fmt.Printf("mapResultPersistent: %v", fileidx)
	// fmt.Println(intermediate)
	//最终在worker的服务器上产生nReduce个中间文件，总共产生file个数*nReduce个中间文件
	for i := 0; i < nReduce; i++ {
		var err error
		if _, err = os.Stat("mr-inter"); os.IsNotExist(err) {
			os.Mkdir("mr-inter", os.ModePerm)
		}
		tmpFiles[i], err = os.Create("mr-inter/temp-" + strconv.Itoa(fileidx) + "-" + strconv.Itoa(i))
		if err != nil {
			fmt.Printf("mapResultPersistent file Create: %v\n", err)
		}
		// fmt.Printf("tmpFile name: %v \n", tmpFiles[i].Name())
		filepaths[i] = tmpFiles[i].Name()
		encoder[i] = json.NewEncoder(tmpFiles[i])
		defer tmpFiles[i].Close()

	}
	// fmt.Printf("tmpFile name: %v \n", tmpFiles[i])
	//文件json编码写入
	for _, kv := range intermediate {
		offset := ihash(kv.Key) % nReduce
		err := encoder[offset].Encode(&kv)
		if err != nil {
			fmt.Printf("File%v Key%v Value%v error:%v \n", fileidx, kv.Key, kv.Value, err)
		}
	}

	MapTaskDoneCall(filepaths, fileidx)
}

// 获取任务职责
func getTaskCall() RPCResponce {
	args := RPCArgs{
		Content: make(map[string]any),
	}
	reply := RPCResponce{}
	// args.Content = make(map[string]any)
	// reply.Content = make(map[string]any)

	call("Coordinator.DispatchTask", &args, &reply)
	return reply
}

func MapTaskDoneCall(filepaths []string, fileidx int) {

	args := RPCArgs{}
	reply := RPCResponce{}

	content := make(map[string]any)
	content["filepaths"] = filepaths
	content["fileidx"] = fileidx

	args.State = MAPDONE
	args.Content = content

	// fmt.Printf("cliend TaskDone call: %v \n", args)
	// fmt.Printf("map task complete: %v\n", os.Getuid())
	call("Coordinator.TaskDone", &args, &reply)

}

func ReduceTaskDoneCall(reduceIdx int) {
	args := RPCArgs{}
	reply := RPCResponce{}

	content := make(map[string]any)
	content["reduceIdx"] = reduceIdx
	args.State = REDUCEDONE
	args.Content = content
	// fmt.Printf("reduce task complete: %v\n", os.Getuid())
	call("Coordinator.TaskDone", &args, &reply)
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

	fmt.Println("call: " + err.Error())
	return false
}
