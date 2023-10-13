package mr

import (
	"container/list"
	"strings"
	"time"

	// "time"

	// "errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type void struct{}

type Coordinator struct {
	// Your definitions here.
	filepaths      []string
	interFilePaths [][]string
	fileidx        int
	reduceIdx      int

	completeMap    int
	completeReduce int
	nReduce        int
	mu             sync.Mutex

	mapTaskQueue      *list.List
	reduceTaskQueue   *list.List
	mapWaitAckList    map[int]void
	reduceWaitAckList map[int]void

	done         bool
	activeWorker int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) DispatchTask(args *RPCArgs, response *RPCResponce) error {
	//根据现况来分配任务
	// c.mapmu.Lock()
	// fmt.Print("dispatchTask execute: ")

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapTaskQueue.Len() > 0 {
		response.State = MAPPING

		tmp := make(map[string]any)
		tmp["nReduce"] = c.nReduce
		fileidx := c.mapTaskQueue.Remove(c.mapTaskQueue.Front()).(int)
		tmp["fileidx"] = fileidx
		tmp["url"] = c.filepaths[fileidx]
		fmt.Printf("map reply: %v\n", tmp)
		(*response).Content = tmp
		if c.fileidx < len(c.filepaths) {
			var v void
			c.mapWaitAckList[fileidx] = v
		}

		c.fileidx++
		// c.activeWorker++
		// (*response).Content["nReduce"] = &c.nReduce
		// response.Content["fileidx"] = &c.fileidx
		// response.Content["url"] = &c.filepaths[c.fileidx]
		// fmt.Println(c.fileidx)

	} else if len(c.mapWaitAckList) == 0 && c.reduceTaskQueue.Len() > 0 {
		(*response).State = REDUCING
		tmp := make(map[string]any)
		reduceIdx := c.reduceTaskQueue.Remove(c.reduceTaskQueue.Front()).(int)
		tmp["reduceFilePaths"] = c.interFilePaths[reduceIdx]
		tmp["reduceIdx"] = reduceIdx
		fmt.Printf("reduce reply: %v\n", tmp)
		(*response).Content = tmp
		if c.reduceIdx < c.nReduce {
			var v void
			c.reduceWaitAckList[reduceIdx] = v
		}
		c.reduceIdx++
		c.activeWorker++
	} else if (c.mapTaskQueue.Len() == 0 && len(c.mapWaitAckList) > 0) || (c.reduceTaskQueue.Len() == 0 && len(c.reduceWaitAckList) > 0) {
		(*response).State = WAIT
		go func() {
			time.Sleep(time.Duration(5) * time.Second)
			c.checkAckList()
		}()
		// c.checkAckList()

	} else {
		c.done = true
		(*response).State = COMPLETE
	}

	return nil
}

func (c *Coordinator) TaskDone(args *RPCArgs, responce *RPCResponce) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Printf("server TaskDone called: %v\n", *args)
	c.activeWorker--
	switch args.State {
	case MAPDONE:
		fileidx := (*args).Content["fileidx"].(int)
		_, ok := c.mapWaitAckList[fileidx]
		if !ok {
			fmt.Println("map request is expire")
			return nil
		} else {
			delete(c.mapWaitAckList, fileidx)
			fmt.Println(c.mapWaitAckList)

			for i := 0; i < c.nReduce; i++ {
				oldpath := (*args).Content["filepaths"].([]string)[i]
				newpath := strings.Replace(oldpath, "temp", "mr", 1)
				os.Rename(oldpath, newpath)
				c.interFilePaths[i] = append(c.interFilePaths[i], newpath)
			}
		}

	case REDUCEDONE:
		reduceIdx := (*args).Content["reduceIdx"].(int)
		_, ok := c.reduceWaitAckList[reduceIdx]
		if !ok {
			fmt.Println("reduce request is expire")
			return nil
		} else {
			delete(c.reduceWaitAckList, reduceIdx)
		}

	}

	return nil
}

func (c *Coordinator) checkAckList() bool {
	// time.Sleep(time.Duration(10) * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println("checkAckList")
	if c.mapTaskQueue.Len() == 0 && len(c.mapWaitAckList) > 0 {
		for k, _ := range c.mapWaitAckList {
			c.mapTaskQueue.PushBack(k)
		}
		return true
	} else if c.reduceTaskQueue.Len() == 0 && len(c.reduceWaitAckList) > 0 {
		for k, _ := range c.reduceWaitAckList {
			c.reduceTaskQueue.PushBack(k)
		}
		return true
	}
	return false
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
	// ret := false

	// Your code here.
	// fmt.Println(c.nReduce - c.completeReduce)
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// for _, file := range files {
	// 	// map线程需要全部完成才能启动reduce线程
	// 	go func(path string) {

	// 	}(file)
	// }
	// 初始化Coordinator类成员变量
	fmt.Println("MakeCoordinator execute")
	c.filepaths = files
	c.interFilePaths = make([][]string, nReduce)
	for i := 0; i < nReduce; i++ {
		c.interFilePaths[i] = make([]string, len(files))
	}
	c.nReduce = nReduce
	c.completeMap = 0
	c.fileidx = 0
	c.reduceIdx = 0
	c.completeReduce = 0

	c.mapTaskQueue = list.New()
	for i := 0; i < len(files); i++ {
		c.mapTaskQueue.PushBack(i)
	}
	c.reduceTaskQueue = list.New()
	for i := 0; i < nReduce; i++ {
		c.reduceTaskQueue.PushBack(i)
	}
	c.mapWaitAckList = make(map[int]void)
	c.reduceWaitAckList = make(map[int]void)

	c.activeWorker = 0
	c.server()
	return &c
}
