package mr

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

type Task struct {
	Type     string        // 属于Map还是Reduce
	FilePath string        // 文件路径
	Index    int           // 任务编号
	NReduce  int           // 分区编号
	Finished chan struct{} // 完成信号
}

type Coordinator struct {
	lock sync.Mutex

	srv *http.Server

	done  chan struct{}
	heart chan struct{}

	state          string // 任务流程状态
	nMap           int
	nReduce        int
	tasks          map[int]*Task
	availableTasks chan *Task
	tmpFiles       map[int][]string
}

//You can add it for your own use to clear temporary files,
//but the sh file in the test link has a clear function,
//so it needs to be commented

//func init() {
//	files, err := filepath.Glob("mr-out-*")
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//
//	for _, file := range files {
//		err := os.Remove(file)
//		if err != nil {
//			fmt.Println(err)
//		} else {
//			fmt.Printf("File %s has been removed\n", file)
//		}
//	}
//}

// RPC handlers for the worker to call.

var retryTimeout = 0

func (c *Coordinator) ApplyTask(args *ApplyArgs, reply *ApplyReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.heart <- struct{}{}
	if c.nMap == 0 && args.Type == Map {
		reply.Finished = true
		return nil
	}
	if c.nReduce == 0 && args.Type == Reduce {
		reply.Finished = true
		c.done <- struct{}{}
		return nil
	}

	if args.Type != c.state {
		return fmt.Errorf("current state is %s,but your state is %s", c.state, args.Type)
	}

	select {
	case task, ok := <-c.availableTasks:
		if ok {
			log.Printf("task < %d : %s > is running\n", task.Index, task.Type)
			reply.Task = task
			go c.HandleTask(task)
		}
	default: // 说明还在运行中
	}

	log.Printf("reply.Task: %#v\n", reply.Task)
	return nil
}

func (c *Coordinator) HandleTask(task *Task) {
	select {
	case <-time.After(TimeOut * time.Second):
		c.lock.Lock()
		defer c.lock.Unlock()
		log.Printf("task < %d : %s> timeout!\n", task.Index, task.Type)
		c.availableTasks <- task
	case <-task.Finished:
		c.lock.Lock()
		defer c.lock.Unlock()
		log.Printf("task < %d : %s> finished!\n", task.Index, task.Type)
		if task.Type == Map {
			c.nMap--

			// switch state
			if c.nMap == 0 {
				log.Println("Map Work is finished!")
				c.state = Shuffle
				if err := c.shuffle(); err != nil {
					log.Fatalf("task< %d : shuffle> failed!\n", task.Index)
				}

			}
		} else {
			c.nReduce--

			// close chan
			if c.nReduce == 0 {
				// switch state
				c.state = Done
				close(c.availableTasks)
			}
		}

	}
}

func (c *Coordinator) SubmitTask(args *SubmitArgs, reply *SubmitReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.heart <- struct{}{}
	if args.State == Failure {
		c.availableTasks <- c.tasks[args.Index]
		return nil
	}

	c.tasks[args.Index].Finished <- struct{}{}

	if args.Type == Map {
		for i, filePaths := range args.FilePaths {
			c.tmpFiles[i] = append(c.tmpFiles[i], filePaths)
		}
	}
	return nil
}

func (c *Coordinator) shuffle() error {
	if c.state != Shuffle {
		return errors.New("c.state must be `Shuffle` state while shuffle")
	}
	log.Println("shuffle is running!")

	for i := 0; i < c.nReduce; i++ {
		var intermediate []KeyValue
		for _, filename := range c.tmpFiles[i] {
			kva, _ := ReadKVFile(filename)
			intermediate = append(intermediate, kva...)
		}
		sort.Sort(ByKey(intermediate))
		odir := "mr-shuffle-worker/"
		oname := fmt.Sprintf("mr-shuffle-%d", i)
		if err := CreateTmpFile(odir, oname, intermediate); err != nil {
			log.Printf("shuffle <%d> error: %v", i, err)
			return err
		}
		task := &Task{
			Type:     Reduce,
			FilePath: odir + oname,
			Index:    i,
			NReduce:  c.nReduce,
			Finished: make(chan struct{}),
		}
		c.availableTasks <- task
		c.tasks[i] = task
	}

	// switch state
	c.state = Reduce
	log.Println("reduce is running!")
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {

	err := rpc.Register(c)
	if err != nil {
		return
	}
	rpc.HandleHTTP()

	c.srv = &http.Server{
		Addr:        ":5559",
		IdleTimeout: 15 * time.Second,
	}
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatalln("listen error: ", err)
	}
	//l, _ := net.Listen("tcp", c.srv.Addr)
	go c.StartServer(l)
	c.ServerTimeOut(l)

}

func (c *Coordinator) StartServer(l net.Listener) {
	err := c.srv.Serve(l)
	if err != nil && err != http.ErrServerClosed {
		log.Fatalf("Error starting server: %v", err)
	}
}

func (c *Coordinator) ServerTimeOut(l net.Listener) {
	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-time.After(15 * time.Second):
			log.Println("server timeout!")
			//go c.StartServer(l)
			if retryTimeout++; retryTimeout >= MaxServerTimeout {
				log.Println("server retry failed!!!")
				return
			}
		case <-c.heart:
			log.Println("heart activate!")
			retryTimeout = 0
		case <-sigCh:
			c.srv.Shutdown(context.Background())
			log.Println("user exit programmer!")
			return
		case <-c.done:
			c.srv.Shutdown(context.Background())
			log.Println("done and server is closing!")
			return
		}
	}
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	log.Printf("state : %v\n", c.state)
	if c.state == Done {
		return true
	}

	return false
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	l := Max(len(files), nReduce)
	c := Coordinator{
		state:          Map,
		nMap:           len(files),
		nReduce:        nReduce,
		tasks:          make(map[int]*Task, l),
		availableTasks: make(chan *Task, l),
		tmpFiles:       make(map[int][]string, nReduce),
		done:           make(chan struct{}),
		heart:          make(chan struct{}),
	}

	for i, file := range files {
		t := &Task{
			Type:     Map,
			FilePath: file,
			Index:    i,
			NReduce:  nReduce,
			Finished: make(chan struct{}),
		}

		c.tasks[i] = t
		c.availableTasks <- t
	}

	CreateDirs(nReduce)
	c.server()
	RemoveDirs(nReduce)
	return &c
}
