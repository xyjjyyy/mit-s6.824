package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"time"
)
import "net/rpc"
import "hash/fnv"

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type MRWorker struct {
	c *rpc.Client
}

var curRetry = 0

// ByKey for sorting by key.
type ByKey []KeyValue

// for sorting by key.

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string, nReduce int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()&0x7fffffff) % nReduce
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := &MRWorker{}
	w.CallMapTask(mapf)
	w.CallReduceWork(reducef)
	log.Println("All work is finished!")
}

func (w *MRWorker) CallMapTask(mapf func(string, string) []KeyValue) {
	for {
		submitArgs, err := w.mapWork(mapf)
		// rpc链接失败
		if submitArgs == nil && err != nil {
			log.Printf("CallMapTask error: %v", err)
			if curRetry++; curRetry >= MaxRetry {
				log.Fatalln("retry too many,ending...")
			}
			time.Sleep(RetryInterval * time.Second)
			continue
		}
		curRetry = 0
		// map任务全部完成
		if submitArgs == nil && err == nil {
			log.Println("The Map work is finished!")
			return
		}
		// no available work but not finished
		// so pending...
		if submitArgs.State == Pending {
			log.Println("Pending...")
			time.Sleep(time.Second)
			continue
		}
		// 失败任务的错误原因
		if err != nil {
			log.Printf("Map error:%v\n", err)
		}
		err = w.submitWork(submitArgs)
		if err != nil {
			log.Fatalf("CallMapTask error: %v", err)
		}
	}
}

func (w *MRWorker) CallReduceWork(
	reducef func(string, []string) string) {
	for {
		submitArgs, err := w.reduceWork(reducef)
		// rpc链接失败
		if submitArgs == nil && err != nil {
			log.Printf("CallReduceTask error: %v", err)
			if curRetry++; curRetry >= MaxRetry {
				log.Fatalln("retry too many,ending...")
			}
			time.Sleep(RetryInterval * time.Second)
			continue
		}
		curRetry = 0
		// map任务全部完成
		if submitArgs == nil && err == nil {
			log.Println("The Reduce work is finished!")
			w.c.Close()
			return
		}
		// no available work but not finished
		// so pending...
		if submitArgs.State == Pending {
			log.Println("Reduce Worker Pending...")
			time.Sleep(time.Second)
			continue
		}
		// 失败任务的错误原因
		if err != nil {
			log.Printf("Reduce error:%v\n", err)
		}
		err = w.submitWork(submitArgs)
		if err != nil {
			log.Fatalf("CallReduceTask error: %v", err)
		}
	}
}

func (w *MRWorker) mapWork(mapf func(string, string) []KeyValue) (*SubmitArgs, error) {
	applyArgs := ApplyArgs{
		Type: Map,
	}
	applyReply := ApplyReply{}

	err := w.call("Coordinator.ApplyTask", &applyArgs, &applyReply)
	if err != nil {
		log.Println("CallMapTask failed!")
		return nil, err
	}

	if applyReply.Task == nil {
		if applyReply.Finished {
			return nil, nil
		}
		return &SubmitArgs{State: Pending}, nil
	}

	log.Printf("MapWork: %#v\n", applyReply.Task)

	filename := applyReply.Task.FilePath
	nReduce := applyReply.Task.NReduce
	index := applyReply.Task.Index
	submitArgs := &SubmitArgs{
		Type:      Map,
		Index:     index,
		State:     Failure,
		FilePaths: make([]string, nReduce),
	}

	intermediate := make(map[int][]KeyValue, nReduce)

	file, err := os.Open(filename)
	if err != nil {
		log.Printf("CallMapTask error: cannot open %v", filename)
		return submitArgs, err
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", filename)
		return submitArgs, err
	}
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		parition := ihash(kv.Key, nReduce)
		intermediate[parition] = append(intermediate[parition], kv)
	}

	for i := 0; i < nReduce; i++ {
		sort.Sort(ByKey(intermediate[i]))
		filedir := fmt.Sprintf("mr-map-worker-%d/", index)
		filename := fmt.Sprintf("mr-map-%d", i)
		err := CreateTmpFile(filedir, filename, intermediate[i])
		if err != nil {
			log.Println("CreateTmpFile error: ", err)
			return submitArgs, err
		}
		submitArgs.FilePaths[i] = filedir + filename
	}
	submitArgs.State = Success
	return submitArgs, nil
}

func (w *MRWorker) submitWork(submitArgs *SubmitArgs) error {
	// don't care return value
	log.Printf("Submit: %#v ", submitArgs)
	err := w.call("Coordinator.SubmitTask", submitArgs, nil)
	if err != nil {
		log.Println("CallMapTask call Coordinator.SubmitTask failed!")
		return err
	}
	return nil
}

func (w *MRWorker) reduceWork(
	reducef func(string, []string) string) (*SubmitArgs, error) {
	applyArgs := ApplyArgs{
		Type: Reduce,
	}
	applyReply := ApplyReply{}

	err := w.call("Coordinator.ApplyTask", &applyArgs, &applyReply)
	if err != nil {
		log.Printf("CallReduceWork call Coordinator.ApplyTask failed: %v\n", err)
		return nil, err
	}
	if applyReply.Task == nil {
		if applyReply.Finished {
			return nil, nil
		}
		return &SubmitArgs{State: Pending}, nil
	}

	log.Printf("ReduceWork: %#v\n", applyReply.Task)

	var intermediate []KeyValue
	filename := applyReply.Task.FilePath
	index := applyReply.Task.Index
	submitArgs := &SubmitArgs{
		Type:  Reduce,
		Index: index,
		State: Failure,
	}

	kva, _ := ReadKVFile(filename)
	intermediate = append(intermediate, kva...)

	oname := fmt.Sprintf("mr-out-%d", index)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	submitArgs.State = Success
	ofile.Close()
	//os.Remove(filename)
	return submitArgs, nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func (w *MRWorker) call(rpcname string, args interface{}, reply interface{}) error {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	w.c = c

	err = c.Call(rpcname, args, reply)
	if err != nil {
		log.Println("call error: ", err)
		return err
	}

	return nil
}
