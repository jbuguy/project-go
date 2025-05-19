package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"sort"
	"sync"
	"time"
)

type Task struct {
	jobName    string
	taskNumber int
	inFile     string
	typeName   string
	nReduce    int
}
type Master struct {
	tasks     []Task
	waiting   []chan Task
	mutex     sync.Mutex
	working   Clients
	id        int
	completed map[string]bool
	numtasks  int
	stage     chan int
}
type Clients struct {
	clients []Client
	mutex   sync.Mutex
}

var nReduce int
var nMap int

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
	setuphttp()
	master = Master{id: 0, stage: make(chan int), completed: make(map[string]bool)}
	listener := setuprpc()
	listenWorkers(listener)
}
func heartbeat(timeout int) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		master.working.mutex.Lock()
		clients := []Client{}
		for _, client := range master.working.clients {
			if now.Sub(client.t) < time.Duration(timeout)*time.Second {
				clients = append(clients, client)
			} else {
				x := fmt.Sprintf("%s%d", client.task.jobName, client.task.taskNumber)
				if !master.completed[x] {
					master.addTask(client.task)
				}
			}
		}
		master.working.clients = clients
		master.working.mutex.Unlock()
	}
}

func setuprpc() net.Listener {
	rpc.Register(&master)
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		return nil
	}
	return listener
}

func listenWorkers(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		fmt.Printf("worker %d has connected", master.id)
		go rpc.ServeConn(conn)
	}
}

func setuphttp() {
	http.Handle("/", http.FileServer(http.Dir("./distribuee/web")))
	http.HandleFunc("/status", handleStatus)
	http.HandleFunc("/start", handleStart)
	http.ListenAndServe(":8080", nil)
}

func (master *Master) run() {
	count := split("file", 1)
	go heartbeat(10)
	for i := range count {
		master.addTask(Task{jobName: "wordcount", taskNumber: i, inFile: fmt.Sprintf("file.part%d", i), typeName: "map", nReduce: nReduce})
	}
	<-master.stage
	master.init()
	for i := range nReduce {
		master.addTask(Task{jobName: "wordcount", taskNumber: i, inFile: fmt.Sprintf("disttmp.part%d", i), typeName: "reduce", nReduce: count})
	}
	<-master.stage
}

func (master *Master) init() {
	master.completed = make(map[string]bool)
	master.tasks = make([]Task, 0)
	master.waiting = make([]chan Task, 0)
}

func (master *Master) getId(args *struct{}, id *string) error {
	*id = fmt.Sprint(master.id)
	master.id += 1
	return nil
}
func (master *Master) getTask(args Args, reply *Task) error {
	master.mutex.Lock()
	if len(master.tasks) > 0 {
		*reply = master.tasks[0]
		master.tasks = master.tasks[1:]
		master.assignTask(args.id, reply)
		master.mutex.Unlock()
		return nil
	}
	taskchan := make(chan Task, 1)
	master.waiting = append(master.waiting, taskchan)
	master.mutex.Unlock()
	*reply = <-taskchan
	master.mutex.Lock()
	master.assignTask(args.id, reply)
	master.mutex.Unlock()
	return nil
}

func (master *Master) assignTask(id string, task *Task) {
	master.working.clients = append(master.working.clients, Client{id, *task, time.Now()})
	key := fmt.Sprintf("%s%d", task.jobName, task.taskNumber)
	master.completed[key] = false
	sort.Slice(master.working.clients, func(i, j int) bool {
		return master.working.clients[i].t.Before(master.working.clients[j].t)
	})
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
	remove(master, args.jobName, args.taskNumber)
	if master.completedTasks() == master.numtasks {
		master.stage <- 1
	}
	return nil
}

func remove(master *Master, jobName string, taskNumber int) {
	i := 0
	master.working.mutex.Lock()
	defer master.working.mutex.Unlock()
	for j, v := range master.working.clients {
		if v.task.taskNumber == taskNumber && v.task.jobName == jobName {
			i = j
			break
		}
	}
	master.working.clients = slices.Delete(master.working.clients, i, i+1)
}
func (master *Master) completedTasks() int {
	c := 0
	for _, v := range master.completed {
		if v == true {
			c++
		}
	}
	return c
}

func handleStatus(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(ls)

}
func (master *Master) Ping(args Args, reply *bool) error {
	for i := range master.working.clients {
		if master.working.clients[i].id == args.id {
			master.working.clients[i].t = time.Now()
			*reply = true
			return nil
		}
	}
	*reply = false
	return nil
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
	nMap = par.NMap
	nReduce = par.NReduce
	go master.run()

}
func split(filename string, lines int) int {
	file, err := os.Open(filename)
	if err != nil {
		return 0
	}
	sc := bufio.NewScanner(file)
	part := 1
	line := 0
	var out *os.File
	var w *bufio.Writer
	f := func() {
		if out != nil {
			w.Flush()
			out.Close()
		}
		partname, _ := os.Create(fmt.Sprintf("%s.part%d", filename, part))
		w = bufio.NewWriter(partname)
		part++
		line = 0
		return
	}
	f()
	for sc.Scan() {
		if line >= lines {
			f()
		}
		w.WriteString(sc.Text() + "\n")
		line++
	}
	if out != nil {
		w.Flush()
		out.Close()
	}
	return part - 1
}
