package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

const prefix = "mrtmp."

// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
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

// clean all intermediary files generated for a job
func CleanIntermediary(jobName string, nMap, nReduce int) {
	// Supprimer les fichiers intermédiaires produits les tâches map
	for reduceTNbr := 0; reduceTNbr < nReduce; reduceTNbr++ {
		for mapTNbr := 0; mapTNbr < nMap; mapTNbr++ {
			os.Remove(ReduceName(jobName, mapTNbr, reduceTNbr))
		}
		os.Remove(MergeName(jobName, reduceTNbr))
	}
}

// Is used to associate to each key a unique reduce file
func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// doMap applique la fonction mapF, et sauvegarde les résultats.
// A COMPLETER
func mapF(content string) []KeyValue {
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
		decoder := json.NewDecoder(file)
		for decoder.More() {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				fmt.Fprintf(os.Stderr, "Error decoding JSON in file %s: %v\n", file.Name(), err)
				break
			}
			m[kv.Key] = append(m[kv.Key], kv.Value)
		}
	}
	// Ouvrir le fichier de sortie pour la tâche de réduction
	// utiliser mergeName
	outName := MergeName(jobName, reduceTaskNumber)
	file, _ := os.OpenFile(outName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	for key, values := range m {
		reducedValue := reduceF(key, values)
		kv := KeyValue{Key: key, Value: reducedValue}
		if err := encoder.Encode(kv); err != nil {
			fmt.Fprintf(os.Stderr, "Error encoding JSON for key %s: %v\n", key, err)
		}
	}
}

func DoMap(
	jobName string,
	mapTaskNumber int,
	inFile string,
	nReduce int,
	mapF func(string) []KeyValue,
) {
	data, err := os.ReadFile(inFile)
	if err != nil {
		log.Fatal(err)
		return
	}
	content := string(data)
	kvs := mapF(string(content))
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	for i := range nReduce {
		os.Create(ReduceName(jobName, mapTaskNumber, int(i)))
	}
	for _, kv := range kvs {
		index := ihash(kv.Key) % uint32(nReduce)
		name := ReduceName(jobName, mapTaskNumber, int(index))
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
