package main

import (
	"bufio"
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
	id         string
	shouldWork bool
}
type Args2 struct {
	JobName    string
	TaskNumber int
}
type KeyValue struct {
	Key   string
	Value string
}
type Args struct {
	Id string
}
type Task struct {
	JobName    string
	TaskNumber int
	InFile     string
	TypeName   string
	Number     int
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
		var reply Task
		workerId := Args{worker.id}
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
			break
		case "reduce":
			DoReduce(reply.JobName, reply.TaskNumber, reply.Number, reduceF)
			break
		}
		var tmp bool
		log.Printf("telling master that the task %s of job %s %d", reply.TypeName, reply.JobName, reply.TaskNumber)
		err = client.Call("Master.ReportTaskDone", Args2{reply.JobName, reply.TaskNumber}, &tmp)
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
		workerId := Args{worker.id}
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
	mapF func(string, string) []KeyValue,
) {
	log.Print("mapping")
	data, err := os.ReadFile(prefix + inFile)
	if err != nil {
		log.Printf("mapper %d  cant read file %s", mapTaskNumber, inFile)
	}
	content := string(data)
	kvs := mapF(inFile, string(content))
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	for _, v := range kvs {
		index := ihash(v.Key) % uint32(nReduce)
		name := ReduceName(jobName, mapTaskNumber, int(index))
		file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

		if err != nil {
			continue
		}
		file.WriteString(fmt.Sprintf("%s %s\n", v.Key, v.Value))
		file.Close()
	}
	log.Print("mapping avec sucess")
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
		file, _ := os.Open(ReduceName(jobName, i, reduceTaskNumber))
		// Lire les paires clé-valeur du fichier
		sc := bufio.NewScanner(file)
		for sc.Scan() {
			// Ajouter la valeur à la liste de valeurs pour cette clé
			line := strings.Split(sc.Text(), " ")
			m[line[0]] = append(m[line[0]], line[1])
		}
	}
	// Ouvrir le fichier de sortie pour la tâche de réduction
	// utiliser mergeName
	outName := MergeName(jobName, reduceTaskNumber)
	file, _ := os.OpenFile(outName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	// Appliquer la fonction de réduction à chaque clé
	for key, values := range m {
		// Appliquer reduceF pour réduire les valeurs associées à cette clé
		// Écrire la clé et la valeur réduite dans le fichier de sortie
		res := reduceF(key, values)
		file.WriteString(key + " " + res + "\n")
	}
}
