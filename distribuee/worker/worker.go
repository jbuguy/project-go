package main

import (
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
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
		client.Call("master.getTask", Args{worker.id}, &reply)
		random := rand.Float64()
		if random < p2 {
			return
		}
		switch reply.jobName {
		case "map":
			DoMap(reply.jobName, reply.taskNumber, reply.inFile, reply.nReduce, mapF)
			break
		case "reduce":
			doReduce(reply.jobName, reply.taskNumber, reply.inFile, reduceF)
			break
		}
		var tmp bool
		client.Call("master.ReportTaskDone", Args2{reply.jobName, reply.taskNumber}, &tmp)
		if tmp {
			return
		}
	}
}
func start() {
	var worker Worker
	client, err := rpc.Dial("tcp", "localhost:1234")
	defer client.Close()
	if err != nil {
		log.Fatal("Dialing:", err)
	}
	var id int
	client.Call("master.getId", nil, &id)
	worker.simulate(client, 0.1, 0.01)
}

func main() {
	for i := 0; i < 5; i++ {
		go start()
	}
}
func (worker Worker) pingMaster(client *rpc.Client) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		random := rand.Float64()
		if random < p {
			time.Sleep(time.Second * 10)
		}
		var reply bool
		client.Call("master.ping", Args{worker.id}, &reply)
	}
}
func DoMap(
	jobName string,
	mapTaskNumber int,
	inFile string,
	nReduce int,
	mapF func(string, string) []KeyValue,
) {
	data, _ := os.ReadFile(inFile)
	content := string(data)
	kvs := mapF(fmt.Sprintf("file.part%d", mapTaskNumber), string(content))
	files := make([]*os.File, nReduce)
	for i := range nReduce {
		name := ReduceName(jobName, mapTaskNumber, i)
		file, _ := os.Create(name)
		files = append(files, file)
		defer file.Close()
	}
	for _, v := range kvs {
		index := ihash(v.Key) % uint32(nReduce)
		name := ReduceName(jobName, mapTaskNumber, int(index))
		file, err := os.OpenFile(name, os.O_APPEND, 0644)
		if err != nil {
			continue
		}
		file.WriteString(fmt.Sprintf("%s\n", v.Value))
	}
	for i := range nReduce {
		name := ReduceName(jobName, mapTaskNumber, i)
		// Lire le contenu du fichier d'entrée
		data, _ := os.ReadFile(name)
		content := string(data)
		// Appliquer la fonction mapF pour obtenir des paires clé-valeur
		lines := strings.Split(string(content), "\n")
		sort.Strings(lines)
		sortedContent := strings.Join(lines, "\n")
		os.WriteFile(name, []byte(sortedContent), 0644)
	}

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
func mapF(document string, content string) []KeyValue {
	words := strings.Fields(content)
	kvs := []KeyValue{}

	for _, word := range words {
		cleaned := strings.ToLower(strings.Trim(word, ".,!?:"))
		if cleaned != "" {
			kvs = append(kvs, KeyValue{Key: cleaned, Value: "1"})
		}
	}

	return kvs
}
func reduceF(key string, values []string) string {
	return fmt.Sprintf("%d", len(values))
}
func doReduce(jobName string, reduceTaskNumber int, inFile string, reduceF func(key string, values []string) string) {

}
