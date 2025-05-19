package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
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
	JobName    string
	TaskNumber int
	InFile     string
	TypeName   string
	Number     int
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
	Id string
}
type Args2 struct {
	JobName    string
	TaskNumber int
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
	go setuphttp()
	master = Master{id: 0, stage: make(chan int), completed: make(map[string]bool)}
	listener := setuprpc()
	listenWorkers(listener)
}
func heartbeat(timeout int) {
	ticker := time.NewTicker(time.Second * time.Duration(timeout))
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		master.working.mutex.Lock()
		clients := []Client{}
		for _, client := range master.working.clients {
			if now.Sub(client.t) < time.Duration(timeout)*time.Second {
				fmt.Println("added client", client.id)
				clients = append(clients, client)
			} else {
				x := fmt.Sprintf("%s%d", client.task.JobName, client.task.TaskNumber)
				if !master.completed[x] {
					fmt.Println("removed client ", client.id)
					master.addTask(client.task)
				}
			}
		}
		master.working.clients = clients
		master.working.mutex.Unlock()
	}
}

func setuprpc() net.Listener {
	fmt.Println("init rpc")
	rpc.Register(&master)
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		return nil
	}
	fmt.Println("finished init rpc")
	return listener
}

func listenWorkers(listener net.Listener) {
	for {
		fmt.Println("waiting for connection")
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		fmt.Println("serving client ", master.id)
		go rpc.ServeConn(conn)
	}
}

func setuphttp() {
	fmt.Println("init http")
	http.Handle("/", http.FileServer(http.Dir("./distribuee/web")))
	http.HandleFunc("/status", handleStatus)
	http.HandleFunc("/start", handleStart)
	fmt.Println("finished init http")
	http.ListenAndServe(":8080", nil)
}

func (master *Master) run() {
	log.Print("going to split the file")
	count := split("../files/file.txt", 1)
	log.Print("files splited to ", count)
	log.Print("starting mapping stage")
	master.numtasks = count
	go heartbeat(10)
	for i := range count {
		master.addTask(Task{JobName: "wordcount", TaskNumber: i, InFile: fmt.Sprintf("file.txt.part%d.txt", i+1), TypeName: "map", Number: nReduce})
	}
	<-master.stage
	log.Print("mapping stage ended")
	master.init()
	log.Print("starting reduce stage")
	master.numtasks = nReduce
	for i := range nReduce {
		master.addTask(Task{JobName: "wordcount", TaskNumber: i, TypeName: "reduce", Number: count})
	}
	<-master.stage
	log.Print("reduce stage ended")
}

func (master *Master) init() {
	master.completed = make(map[string]bool)
	master.tasks = make([]Task, 0)
	master.waiting = make([]chan Task, 0)
}

func (master *Master) GetId(args *struct{}, id *string) error {
	*id = fmt.Sprintf("%d", master.id)
	master.id += 1
	return nil
}
func (master *Master) GetTask(args Args, reply *Task) error {
	log.Printf("worker %s trying to get a task", args.Id)
	master.mutex.Lock()
	if len(master.tasks) > 0 {

		*reply = master.tasks[0]
		master.tasks = master.tasks[1:]
		master.assignTask(args.Id, reply)
		master.mutex.Unlock()
		return nil
	}
	fmt.Println("no waiting task ")
	taskchan := make(chan Task, 1)
	master.waiting = append(master.waiting, taskchan)
	master.mutex.Unlock()
	fmt.Println("waiting for a task to be available")
	*reply = <-taskchan
	master.mutex.Lock()
	master.assignTask(args.Id, reply)
	master.mutex.Unlock()
	return nil
}

func (master *Master) assignTask(id string, task *Task) {
	master.working.clients = append(master.working.clients, Client{id, *task, time.Now()})
	key := fmt.Sprintf("%s%d", task.JobName, task.TaskNumber)
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
	log.Print("task: ", args)
	master.mutex.Lock()
	defer master.mutex.Unlock()
	master.completed[fmt.Sprintf("%s%d", args.JobName, args.TaskNumber)] = true
	*reply = true
	remove(master, args.JobName, args.TaskNumber)
	if master.completedTasks() == master.numtasks {
		log.Printf("stage completed pasiing to next stage")
		master.stage <- 1
	}
	return nil
}

func remove(master *Master, jobName string, taskNumber int) {
	i := 0
	master.working.mutex.Lock()
	defer master.working.mutex.Unlock()
	for j, v := range master.working.clients {
		if v.task.TaskNumber == taskNumber && v.task.JobName == jobName {
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
	log.Println(args.Id)
	for i := range master.working.clients {
		if master.working.clients[i].id == args.Id {
			master.working.clients[i].t = time.Now()
			*reply = true
			return nil
		}
	}
	*reply = false
	return nil
}
func handleStart(w http.ResponseWriter, r *http.Request) {
	log.Print("working on starting master")
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
		fmt.Println("Error opening file:", err)
		return 0
	}
	defer file.Close()

	sc := bufio.NewScanner(file)
	part := 1
	line := 0
	var out *os.File
	var w *bufio.Writer
	newPart := func() {
		if out != nil {
			w.Flush()
			out.Close()
		}
		out, _ = os.Create(fmt.Sprintf("%s.part%d.txt", filename, part))
		w = bufio.NewWriter(out)
		part++
		line = 0
	}
	newPart()
	for sc.Scan() {
		if line >= lines {
			newPart()
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
