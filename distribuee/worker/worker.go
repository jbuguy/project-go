package dist

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type Worker struct {
	id string
}
type Args2 struct {
	jobName    string
	taskNumber int
}
type KeyValue struct {
	Key   string
	Value string
}
type Args struct {
	id string
}
type Reply1 struct {
	jobName    string
	taskNumber int
	inFile     string
	funcName   string
	nReduce    int
}

const prefix = "disttmp."

func ReduceName(jobName string, mapTask int, reduceTask int) string {
	return prefix + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func MergeName(jobName string, reduceTask int) string {
	return prefix + jobName + "-res-" + strconv.Itoa(reduceTask)
}

// ansName constructs the name of the output file of the final answer
func AnsName(jobName string) string {
	return prefix + jobName
}

func (simulate Worker) simulate(client *rpc.Client, p1, p2 float64) {
	for {
		var reply Reply1
		client.Call("master.getTask", Args{}, &reply)
		random := rand.Float64()
		if random < p1 {
			time.Sleep(15)
		}
		random = rand.Float64()
		if random < p2 {
			return
		}
		switch reply.jobName {
		case "map":
			doMap(reply.jobName, reply.taskNumber, reply.inFile, reply.nReduce, mapF)
			break
		case "reduce":
			doReduce(reply.jobName, reply.taskNumber, reply.inFile, reduceF)
			break
		}
		var tmp bool
		client.Call("master.ReportTaskDone", Args2{reply.jobName, reply.taskNumber}, tmp)
		if tmp {
			return
		}
	}
}
func main() {
	var worker Worker
	client, err := rpc.Dial("tcp", "localhost:1234")
	defer client.Close()
	if err != nil {
		log.Fatal("Dialing:", err)
	}
	var id int
	client.Call("master.getId", nil, id)
	os.Mkdir(fmt.Sprintf("worker%d", id), os.ModePerm)

	go worker.simulate(client, 0.1, 0.01)
}
func (worker Worker) pingMaster(client *rpc.Client) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		var reply bool
		client.Call("master.ping", Args{worker.id}, &reply)
	}
}

// TODO: complete the functions
func doMap(jobName string, mapTaskNumber int, inFile string, nReduce int, mapF func(
	file string, contents string) []KeyValue)
func mapF(document string, content string) []KeyValue
func reduceF(key string, values []string) string
func doReduce(jobName string, reduceTaskNumber int, inFile string, reduceF func(key string, values []string) string)
