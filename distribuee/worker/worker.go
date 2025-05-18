package worker

import (
	"log"
	"math/rand"
	"net"
	"net/rpc"
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

func (Worker Worker) Ping(args struct{}, reply *string) error {
	*reply = Worker.id
	return nil
}

func (simulate Worker) simulate(client *rpc.Client, listener net.Listener, p1, p2 float64) {
	for {
		var reply Reply1
		client.Call("master.getTask", Args{}, &reply)
		random := rand.Float64()
		if random < p1 {
			time.Sleep(15)
		}
		random = rand.Float64()
		if random < p2 {
			listener.Close()
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
	rpc.Register(worker)
	listener, err := net.Listen("tcp", ":1235")
	conn, err := listener.Accept()
	go rpc.ServeConn(conn)
	client, err := rpc.Dial("tcp", "localhost:1234")
	defer client.Close()
	if err != nil {
		log.Fatal("Dialing:", err)
	}
	worker := new(Worker)
	go worker.simulate(client, 0.1, 0.01)
}
func (worker Worker) pingMaster(client *rpc.Client) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		var reply bool
		client.Call("master.rpc", Args{worker.id}, &reply)
	}
}

// TODO: complete the functions
func doMap(jobName string, mapTaskNumber int, inFile string, nReduce int, mapF func(
	file string, contents string) []KeyValue)
func mapF(document string, content string) []KeyValue
func reduceF(key string, values []string) string
func doReduce(jobName string, reduceTaskNumber int, inFile string, reduceF func(key string, values []string) string)
