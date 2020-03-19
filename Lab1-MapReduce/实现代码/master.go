package mr

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	files         []string
	nReduce       int
	nMap          int
	isFilshMap    bool
	isFilshReduce bool
	mapState      map[int]int
	mapTime       map[int]time.Time
	reduceState   map[int]int
	reduceTime    map[int]time.Time
	mux           sync.Mutex
}

const (
	NoAction = 1
	Doing    = 2
	Done     = 3
)

// Your code here -- RPC handlers for the worker to call.

func (m *Master) OrderTask(args Arg, task *Task) error {
	//fmt.Println(unsafe.Pointer(task))
	switch args.ASK_TYPE {

	case ASK_TASK:
		if m.isFilshMap {
			if m.isFilshReduce {
				task.TaskID = TASK_FILSH
				return nil
			}
			//Reduce任务
			m.reduceTask(task)
			return nil
		}
		//Map任务
		m.mapTask(task)
	case MAP_FILSH:
		m.filshMap(args.ASK_ID)
	case REDUCE_FILSH:
		m.filshReduce(args.ASK_ID)
	}
	return nil
}

func (m *Master) filshMap(id int) {
	m.mux.Lock()
	defer m.mux.Unlock()
	m.mapState[id] = Done
	fmt.Printf("master : 第 %d 号Map任务已完成 \n", id)
	for i := 0; i < m.nMap; i++ {
		if m.mapState[i] != Done {
			return
		}
	}
	m.isFilshMap = true

}

func (m *Master) filshReduce(id int) {
	m.mux.Lock()
	defer m.mux.Unlock()
	m.reduceState[id] = Done
	fmt.Printf("master : 第 %d 号Reduce任务已完成 \n", id)
	for i := 0; i < m.nReduce; i++ {
		if m.reduceState[i] != Done {
			return
		}
	}
	m.isFilshReduce = true
}

func (m *Master) getMapTask() (string, int) {
	ten := time.Second * 10
	for id := 0; id < m.nMap; id++ {
		sub := time.Now().Sub(m.mapTime[id])
		if m.mapState[id] == NoAction || (m.mapState[id] == Doing && sub > ten){
			return m.files[id], id
		}
	}
	return "", -1
}

func (m *Master) mapTask(task *Task) {
	m.mux.Lock()
	defer m.mux.Unlock()
	fileName, id := m.getMapTask()
	if id == -1 {
		task.TaskID = TASK_WAIT
		return
	}
	fmt.Printf("master : 分配第 %d 号Map任务 操作的文件是 %s \n", id, m.files[id])
	m.mapState[id] = Doing
	m.mapTime[id] = time.Now()
	task.NMap = m.nMap
	task.NReduce = m.nReduce
	task.FileName = fileName
	task.TaskID = TASK_MAP
	task.MapID = id
	task.ReduceID = 0

}

func (m *Master) getReduceTask() int {
	ten := time.Second * 10
	for id := 0; id < m.nReduce; id++ {
		sub := time.Now().Sub(m.reduceTime[id])
		if m.reduceState[id] == NoAction || (m.reduceState[id] == Doing && sub > ten) {
			return id
		}
	}
	return -1
}

func (m *Master) reduceTask(task *Task) {
	m.mux.Lock()
	defer m.mux.Unlock()
	id := m.getReduceTask()
	if id == -1 {
		task.TaskID = TASK_WAIT
		return
	}
	m.reduceState[id] = Doing
	m.reduceTime[id] = time.Now()
	fmt.Printf("master : 分配第 %d 号Reduce任务 \n", id)
	task.TaskID = TASK_REDUCE
	task.NMap = m.nMap
	task.NReduce = m.nReduce
	task.ReduceID = id

}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mux.Lock()
	defer m.mux.Unlock()
	// ret := false

	// Your code here.

	return m.isFilshReduce
}

func removeIntermediaFiles() {
	files, _ := filepath.Glob("mr-[0-9]*-[0-9]*")
	for _, fi := range files {
		os.Remove(fi)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mux.Lock()
	defer m.mux.Unlock()
	// Your code here.
	removeIntermediaFiles()
	m.isFilshMap = false
	m.isFilshReduce = false
	m.nReduce = nReduce
	m.nMap = len(files)

	m.files = make([]string, m.nMap)

	m.mapState = make(map[int]int, m.nMap)
	m.reduceState = make(map[int]int, m.nReduce)

	m.mapTime = make(map[int]time.Time, m.nMap)
	m.reduceTime = make(map[int]time.Time, m.nReduce)

	for i := 0; i < m.nMap; i++ {
		m.mapState[i] = 1
	}

	for i := 0; i < nReduce; i++ {
		m.reduceState[i] = 1
	}

	for id, str := range files {
		m.files[id] = str
	}

	//fmt.Println(m)

	m.server()
	return &m
}
