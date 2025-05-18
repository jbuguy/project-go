package master

import (
	"net"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

type Task struct {
	jobName    string
	taskNumber int
	inFile     string
	funcName   string
	nReduce    int
}
type Master struct {
	tasks   []Task
	waiting []chan Task
	mu      sync.Mutex
	clients []Client
}
type Client struct {
	id   string
	task Task
	t    time.Time
}
type KeyValue struct {
	Key   string
	Value string
}
type Args struct {
	id string
}
type Args2 struct {
	jobName    string
	taskNumber int
}

func (master *Master) getTask(args Args, reply *Task) error {
	taskchan := make(chan Task, 1)
	master.mu.Lock()
	if len(master.tasks) > 0 {
		*reply = master.tasks[0]
		master.clients = append(master.clients, Client{args.id, *reply, time.Now()})
		sort.Slice(master.clients, func(i, j int) bool {
			return master.clients[i].t.Before(master.clients[j].t)
		})
		master.tasks = master.tasks[1:]
		return nil
	}
	master.waiting = append(master.waiting, taskchan)
	master.mu.Unlock()
	*reply = <-taskchan
	sort.Slice(master.clients, func(i, j int) bool {
		return master.clients[i].t.Before(master.clients[j].t)
	})
	return nil

}
func (master *Master) addTask(task Task) {
	master.mu.Lock()
	defer master.mu.Unlock()
	if len(master.waiting) > 0 {
		workerChan := master.waiting[0]
		master.waiting = master.waiting[1:]
		workerChan <- task
	} else {
		master.tasks = append(master.tasks, task)
	}
}
func (Master *Master) ReportTaskDone(Args2, reply bool) {

}
func main() {
	master := new(Master)
	rpc.Register(master)
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		return
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}
