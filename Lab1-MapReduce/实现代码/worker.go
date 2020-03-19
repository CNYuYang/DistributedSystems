package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task := AskTask(ASK_TASK, 0)
		//fmt.Println(task)
		//time.Sleep(time.Second)

		switch task.TaskID {
		case TASK_MAP:
			DoingMap(task.MapID, task.NReduce, task.FileName, mapf)
		case TASK_REDUCE:
			DoingReduce(task.ReduceID, task.NMap, reducef)
		case TASK_WAIT:
			time.Sleep(time.Millisecond * 50)
		case TASK_FILSH:
			fmt.Println("Worker Over")
			return
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func DoingReduce(taskId int, nMap int, reducef func(string, []string) string) {
	//time.Sleep(time.Second)
	var kva []KeyValue
	for i := 0; i < nMap; i++ {
		fileName := fmt.Sprintf("mr-%v-%v",i,taskId)
		ifile, _ := os.Open(fileName)
		defer ifile.Close()
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv) ;err!= nil{
				break
			}
			kva = append(kva,kv)
		}
	}
	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%v",taskId)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	defer ofile.Close()
	//fmt.Printf("woker : 第 %d 号Reduce任务已完成,\n", taskId)
	AskTask(REDUCE_FILSH, taskId)
}

func DoingMap(taskId int, nReduce int, fileName string, mapf func(string, string) []KeyValue) {
	var myMap map[int]ByKey
	myMap = make(map[int]ByKey, nReduce)

	intermediate := []KeyValue{}
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	defer file.Close()
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)
	for _, item := range intermediate {
		i := ihash(item.Key) % nReduce
		myMap[i] = append(myMap[i], item)
	}
	for idx, item := range myMap {
		oname := fmt.Sprintf("mr-%v-%v", taskId, idx)
		ofile, _ := os.Create(oname)
		defer ofile.Close()
		enc := json.NewEncoder(ofile)
		for _, kv := range item {
			err := enc.Encode(&kv)
			if err != nil {
				break
			}
		}
	}

	//fmt.Printf("woker : 第 %d 号Map任务已完成,Map的对象是 %s \n", taskId, fileName)
	AskTask(MAP_FILSH, taskId)
}

func AskTask(AskType int, Id int) Task {
	args := Arg{}
	args.ASK_TYPE = AskType
	args.ASK_ID = Id
	var task Task
	call("Master.OrderTask", &args, &task)
	return task
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := masterSock()
	//c, err := rpc.DialHTTP("unix", sockname)
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
