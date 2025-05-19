package mapreduce

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"strconv"
	"strings"
)

const prefix = "mrtmp."

// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string
	Value string
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
	for i := 0; i < nMap; i++ {
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
	file, _ := os.OpenFile(outName, os.O_APPEND|os.O_CREATE, 0644)
	// Appliquer la fonction de réduction à chaque clé
	for key, values := range m {
		// Appliquer reduceF pour réduire les valeurs associées à cette clé
		// Écrire la clé et la valeur réduite dans le fichier de sortie
		res := reduceF(key, values)
		file.WriteString(res + "\n")
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
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
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
}
