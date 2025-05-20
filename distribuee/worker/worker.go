package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"project/dist/commons"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Worker struct {
	id string
}

const prefix = "./distribuee/files/"

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

func (worker Worker) simulate(client *rpc.Client, p1, p2 float64) {
	go worker.pingMaster(client, p1)
	for {
		var reply commons.Task
		workerId := commons.Args{Id: worker.id}
		err := client.Call("Master.GetTask", workerId, &reply)
		if err != nil {
			log.Fatal(err)
		}
		if reply.TypeName == "" {
			log.Println(reply)
			return
		}
		random := rand.Float64()
		if random < p2 {
			return
		}
		switch reply.TypeName {
		case "map":
			DoMap(reply.JobName, reply.TaskNumber, reply.InFile, reply.Number, mapF)
		case "reduce":
			DoReduce(reply.JobName, reply.TaskNumber, reply.Number, reduceF)
		}
		var tmp bool
		log.Printf("reporting to  master that the task %s of job %s %d is done", reply.TypeName, reply.JobName, reply.TaskNumber)
		err = client.Call("Master.ReportTaskDone", commons.Args2{JobName: reply.JobName, TaskNumber: reply.TaskNumber}, &tmp)
		if err != nil {
			log.Fatal(err)
		}
		if !tmp {
			return
		}
	}
}
func start() {
	var worker Worker
	for {
		client, err := rpc.Dial("tcp", "localhost:1234")
		if err != nil {
			log.Fatal("Dialing:", err)
		}
		var a struct{}
		client.Call("Master.GetId", a, &(worker.id))
		fmt.Println("id", worker.id)
		worker.simulate(client, 0.001, 0.001)
		client.Close()
	}
}

func main() {
	start()
}
func (worker Worker) pingMaster(client *rpc.Client, p float64) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		random := rand.Float64()
		if random < p {
			time.Sleep(time.Second * 1)
		}
		var reply bool
		workerId := commons.Args{Id: worker.id}
		client.Call("Master.Ping", workerId, &reply)
		if !reply {
			break
		}
	}

}
func DoMap(
	jobName string,
	mapTaskNumber int,
	inFile string,
	nReduce int,
	mapF func(string, string) []commons.KeyValue,
) {
	data, err := os.ReadFile(commons.Prefix + inFile)
	if err != nil {
		log.Fatal(err)
		return
	}
	content := string(data)
	kvs := mapF(inFile, string(content))
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	for i := range nReduce {
		file, _ := os.OpenFile(commons.ReduceName(jobName, mapTaskNumber, i), os.O_CREATE|os.O_TRUNC, 0644)
		file.Close()
	}
	for _, kv := range kvs {
		index := ihash(kv.Key) % uint32(nReduce)
		name := commons.ReduceName(jobName, mapTaskNumber, int(index))
		file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			continue
		}
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(kv); err != nil {
			fmt.Fprintf(os.Stderr, "Error encoding JSON for key %s: %v\n", kv, err)
		}
		file.Close()
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
func mapF(document string, content string) []commons.KeyValue {
	words := strings.Fields(content)
	kvs := []commons.KeyValue{}

	for _, word := range words {
		cleaned := strings.ToLower(strings.Trim(word, ".,!?:"))
		if cleaned != "" {
			kvs = append(kvs, commons.KeyValue{Key: cleaned, Value: "1"})
		}
	}

	return kvs
}
func reduceF(key string, values []string) string {
	return fmt.Sprintf("%d", len(values))
}
func DoReduce(
	jobName string,
	reduceTaskNumber int,
	nMap int,
	reduceF func(key string, values []string) string,
) {
	// Créer une map pour stocker les valeurs par clé
	m := make(map[string][]string)
	// Lire les fichiers intermédiaires produits par chaque tâche map
	for i := range nMap {
		// Ouvrir le fichier pour la tâche de mappage i
		file, _ := os.Open(commons.ReduceName(jobName, i, reduceTaskNumber))
		// Lire les paires clé-valeur du fichier
		decoder := json.NewDecoder(file)
		for decoder.More() {
			var kv commons.KeyValue
			if err := decoder.Decode(&kv); err != nil {
				fmt.Fprintf(os.Stderr, "Error decoding JSON in file %s: %v\n", file.Name(), err)
				break
			}
			m[kv.Key] = append(m[kv.Key], kv.Value)
		}
	}
	// Ouvrir le fichier de sortie pour la tâche de réduction
	// utiliser mergeName
	outName := commons.MergeName(jobName, reduceTaskNumber)
	file, _ := os.OpenFile(outName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	for key, values := range m {
		reducedValue := reduceF(key, values)
		kv := commons.KeyValue{Key: key, Value: reducedValue}
		if err := encoder.Encode(kv); err != nil {
			fmt.Fprintf(os.Stderr, "Error encoding JSON for key %s: %v\n", key, err)
		}
	}
}
