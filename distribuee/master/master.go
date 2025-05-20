package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"project/dist/commons"
	"sync"
	"time"
)

type Master struct {
	tasks          []commons.Task
	waitingWorkers []chan commons.Task
	mutex          sync.Mutex
	working        Clients
	id             int
	completed      map[string]bool
	numtasks       int
	stage          chan int
	lifeStop       chan bool
	stageO         StageObserver
	progO          ProgressObserver
	taskO          TaskObserver
}
type Clients struct {
	clients map[string]Client
	mutex   sync.Mutex
}

type Client struct {
	task commons.Task
	t    time.Time
}

type statusjson struct {
	TaskID string `json:"task_id"`
	Status string `json:"status"`
}

type Liststatus struct {
	Lstatus  []statusjson `json:"stats"`
	Stage    string       `json:"stage"`
	Progress float64      `json:"progress"`
}
type Par1 struct {
	Lines   int `json:"lines"`
	NReduce int `json:"nReduce"`
}
type StageObserver struct {
	ls    *Liststatus
	mutex sync.Mutex
}
type ProgressObserver struct {
	ls    *Liststatus
	mutex sync.Mutex
}
type TaskObserver struct {
	ls    *Liststatus
	mutex sync.Mutex
}

func (observer *TaskObserver) notify(id, state string) {
	observer.mutex.Lock()
	defer observer.mutex.Unlock()
	observer.ls.Lstatus = append(ls.Lstatus, statusjson{id, state})
}
func (observer *ProgressObserver) updateProgress(progress float64) {
	observer.mutex.Lock()
	defer observer.mutex.Unlock()
	observer.ls.Progress = progress
}

func (observer *StageObserver) updateStage(ch string) {
	observer.mutex.Lock()
	defer observer.mutex.Unlock()
	observer.ls.Stage = ch
}

var ls Liststatus
var gMaster Master

func main() {
	gMaster = Master{id: 0, stage: make(chan int),
		working:   Clients{clients: make(map[string]Client)},
		completed: make(map[string]bool),
		lifeStop:  make(chan bool, 1)}
	gMaster.stageO = StageObserver{&ls, sync.Mutex{}}
	gMaster.stageO.updateStage("inactive")
	gMaster.progO = ProgressObserver{&ls, sync.Mutex{}}
	gMaster.taskO = TaskObserver{&ls, sync.Mutex{}}
	gMaster.progO.updateProgress(0)
	go initHttp()
	listener := initRpc()
	listenWorkers(listener)
}
func heartbeat(timeout int) {
	ticker := time.NewTicker(time.Second * time.Duration(timeout))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			gMaster.mutex.Lock()
			now := time.Now()
			for id, client := range gMaster.working.clients {
				if now.Sub(client.t) > time.Duration(timeout)*time.Second {
					if !gMaster.completed[client.task.Name()] {
						gMaster.taskO.notify(client.task.Name(), "relocating")
						fmt.Println("reassigning task:", client.task.Name())
						gMaster.addTask(client.task)
						delete(gMaster.working.clients, id)
					}
				}
			}
			gMaster.mutex.Unlock()
		case <-gMaster.lifeStop:
			fmt.Println("shutting down heartbeat monitor")
			return
		}
	}
}

func listenWorkers(listener net.Listener) {
	for {
		fmt.Println("waiting for connection")
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		fmt.Println("serving client ", gMaster.id)
		go rpc.ServeConn(conn)
	}
}

func (master *Master) run(lines, nReduce int) {
	log.Print("going to split the file")
	count := split("../files/file.txt", lines)
	log.Print("files splited to ", count)
	log.Print("starting mapping stage")
	master.stageO.updateStage("map")
	master.numtasks = count
	master.lifeStop = make(chan bool)
	go heartbeat(10)
	for i := 0; i < count; i++ {
		master.addTask(commons.Task{JobName: "wordcount",
			TaskNumber: i,
			InFile:     fmt.Sprintf("%s.part%d.txt", "file.txt", i+1),
			TypeName:   "map",
			Number:     nReduce})
	}
	<-master.stage
	log.Print("mapping stage ended")
	master.stageO.updateStage("reduce")
	master.init()
	log.Print("starting reduce stage")
	master.numtasks = nReduce
	for i := 0; i < nReduce; i++ {
		master.addTask(commons.Task{JobName: "wordcount", TaskNumber: i, TypeName: "reduce", Number: count})
	}
	<-master.stage
	close(master.lifeStop)
	log.Print("reduce stage ended")
	master.stageO.updateStage("merge")
	log.Print("starting merge stage ")
	var resFiles []string
	for i := range nReduce {
		resFiles = append(resFiles, "."+commons.MergeName("wordcount", i))
	}
	commons.ConcatFiles("."+commons.AnsName("wordcount"), resFiles)
	log.Print("merge stage ended")
	commons.CleanIntermediary("wordcount", count, nReduce)
	log.Print("completed all stages")
	master.stageO.updateStage("done")
}

func (master *Master) init() {
	master.completed = make(map[string]bool)
	master.tasks = make([]commons.Task, 0)
	master.waitingWorkers = make([]chan commons.Task, 0)
}

func (master *Master) assignTask(id string, task *commons.Task) {
	master.working.clients[id] = Client{*task, time.Now()}
	master.completed[task.Name()] = false
	master.taskO.notify(task.Name(), "working")
}

func (master *Master) addTask(task commons.Task) {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	master.taskO.notify(task.Name(), "waiting")
	if len(master.waitingWorkers) > 0 {
		workerChan := master.waitingWorkers[0]
		master.waitingWorkers = master.waitingWorkers[1:]
		workerChan <- task
	} else {
		master.tasks = append(master.tasks, task)
	}
}

func remove(master *Master, id string) {
	client := master.working.clients[id]
	client.task = commons.Task{}
	master.working.clients[id] = client
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
