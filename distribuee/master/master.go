package master

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
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
	tasks     []Task
	waiting   []chan Task
	mutex     sync.Mutex
	clients   []Client
	id        int
	completed map[string]bool
	numtasks  int
	nMap      int
	nReduce   int
	pass      chan int
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

// json variables types
type statusjson struct {
	TaskID string `json:"task_id"`
	Status string `json:"status"`
}

type Liststatus struct {
	Lstatus []statusjson `json:"stats"`
}
type Par1 struct {
	NMap    int `json:"nMap"`
	NReduce int `json:"nReduce"`
}

var ls Liststatus
var master Master

func main() {
	master = Master{id: 0}
	rpc.Register(&master)
	listener, err := net.Listen("tcp", ":1234")
	http.Handle("/", http.FileServer(http.Dir("./web")))
	http.HandleFunc("/status", handleStatus)
	http.HandleFunc("/start", handleStart)
	http.ListenAndServe(":8080", nil)
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
func handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		return
	}
	var par Par1
	err := json.NewDecoder(r.Body).Decode(&par)
	if err != nil {
		return
	}
	master.nMap = par.NMap
	master.nMap = par.NReduce
	go master.run()

}
func (master *Master) run() {
	for i := 0; i < master.nMap; i++ {
		master.addTask(Task{jobName: "map", taskNumber: i})
	}
	<-master.pass
	for i := 0; i < master.nMap; i++ {
		master.addTask(Task{jobName: "reduce", taskNumber: i})
	}
	<-master.pass
}

func (master *Master) getId(args *struct{}, id *string) error {
	*id = fmt.Sprint(master.id)
	master.id += 1
	return nil
}
func (master *Master) getTask(args Args, reply *Task) error {
	taskchan := make(chan Task, 1)
	master.mutex.Lock()
	defer master.mutex.Unlock()
	if len(master.tasks) > 0 {
		*reply = master.tasks[0]
		master.clients = append(master.clients, Client{args.id, *reply, time.Now()})
		x := fmt.Sprintf("%s%d", reply.jobName, reply.taskNumber)
		master.completed[x] = false
		sort.Slice(master.clients, func(i, j int) bool {
			return master.clients[i].t.Before(master.clients[j].t)
		})
		master.tasks = master.tasks[1:]
		return nil
	}
	x := fmt.Sprintf("%s%d", reply.jobName, reply.taskNumber)
	master.completed[x] = false
	master.waiting = append(master.waiting, taskchan)
	*reply = <-taskchan
	sort.Slice(master.clients, func(i, j int) bool {
		return master.clients[i].t.Before(master.clients[j].t)
	})
	return nil

}
func (master *Master) addTask(task Task) {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	if len(master.waiting) > 0 {
		workerChan := master.waiting[0]
		master.waiting = master.waiting[1:]
		workerChan <- task
	} else {
		master.tasks = append(master.tasks, task)
	}
}

// TODO:cpmplete
func (master *Master) ReportTaskDone(args Args2, reply *bool) error {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	master.completed[fmt.Sprintf("%s%d", args.jobName, args.taskNumber)] = true
	*reply = true
	if len(master.completed) == master.numtasks {
		master.pass <- 1
	}
	return nil
}

func handleStatus(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(ls)

}
func (master *Master) Ping(args Args, reply *bool) error {
	for i, _ := range master.clients {
		if master.clients[i].id == args.id {
			master.clients[i].t = time.Now()
			*reply = true
			return nil
		}
	}
	*reply = false
	return nil
}
