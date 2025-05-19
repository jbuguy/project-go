package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"project/dist/commons"
	"slices"
	"sort"
	"sync"
	"time"
)

type Master struct {
	tasks     []commons.Task
	waiting   []chan commons.Task
	mutex     sync.Mutex
	working   Clients
	id        int
	completed map[string]bool
	numtasks  int
	stage     chan int
	lifeStop  chan bool
}
type Clients struct {
	clients []Client
	mutex   sync.Mutex
}

var nReduce int

type Client struct {
	id   string
	task commons.Task
	t    time.Time
}

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
	master = Master{id: 0, stage: make(chan int), completed: make(map[string]bool), lifeStop: make(chan bool,1)}
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
			switch {
			case <-master.lifeStop:
				return
			default:
				if now.Sub(client.t) < time.Duration(timeout)*time.Second {
					fmt.Println("added client", client.id)
					clients = append(clients, client)
				} else {
					x := fmt.Sprintf("%s%d", client.task.JobName, client.task.TaskNumber)
					if !master.completed[x] {
						master.addTask(client.task)
					}
				}
			}
		}
		master.working.clients = clients
		master.working.mutex.Unlock()
	}
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

func (master *Master) run() {
	log.Print("going to split the file")
	count := split("../files/file.txt", 1000)
	log.Print("files splited to ", count)
	log.Print("starting mapping stage")
	master.numtasks = count
	go heartbeat(10)
	for i := range count {
		master.addTask(commons.Task{JobName: "wordcount", TaskNumber: i, InFile: fmt.Sprintf("%s.part%d.txt", "file.txt", i+1), TypeName: "map", Number: nReduce})
	}
	<-master.stage
	log.Print("mapping stage ended")
	master.init()
	log.Print("starting reduce stage")
	master.numtasks = nReduce
	for i := range nReduce {
		master.addTask(commons.Task{JobName: "wordcount", TaskNumber: i, TypeName: "reduce", Number: count})
	}
	<-master.stage
	master.lifeStop <- true
	log.Print("reduce stage ended")
	var resFiles []string
	for i := range nReduce {
		resFiles = append(resFiles, "."+commons.MergeName("wordcount", i))
	}
	commons.ConcatFiles("."+commons.AnsName("wordcount"), resFiles)
	commons.CleanIntermediary("wordcount", count, nReduce)

}

func (master *Master) init() {
	master.completed = make(map[string]bool)
	master.tasks = make([]commons.Task, 0)
	master.waiting = make([]chan commons.Task, 0)
}

func (master *Master) assignTask(id string, task *commons.Task) {
	master.working.clients = append(master.working.clients, Client{id, *task, time.Now()})
	key := fmt.Sprintf("%s%d", task.JobName, task.TaskNumber)
	master.completed[key] = false
	sort.Slice(master.working.clients, func(i, j int) bool {
		return master.working.clients[i].t.Before(master.working.clients[j].t)
	})
}

func (master *Master) addTask(task commons.Task) {
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
		if v {
			c++
		}
	}
	return c
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
