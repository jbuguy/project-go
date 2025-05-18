package mapreduce

import (
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
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
func DoMap(
	jobName string,
	mapTaskNumber int,
	inFile string,
	nReduce int,
	mapF func(contents string) []KeyValue,
) {
	// Lire le contenu du fichier d'entrée
	data, err := os.ReadFile(inFile)
	CheckError(err, "err:%s")
	content := string(data)
	// Appliquer la fonction mapF pour obtenir des paires clé-valeur
	kvs := mapF(string(content))
	// Créer nReduce fichiers, un pour chaque tâche de réduction
	// utiliser reduceName
	for i := 0; i < nReduce; i++ {
		file, err := os.Create(fmt.Sprintf("reduce%d.txt", i))
		if err != nil {
			CheckError(err, "err:%s")
			continue
		}
		defer file.Close()
	}
	// Partitionner les paires clé-valeur en fonction du hachage de la clé
	// Calculer la tâche de réduction associée à chaque clé
	// Écrire la paire clé-valeur dans le fichier approprié
	for _, v := range kvs {
		hash := ihash(v.Key)
		index := hash % uint32(nReduce)
		file, err := os.OpenFile(fmt.Sprintf("reduce%d.txt", index), os.O_APPEND, 0644)
		if err != nil {
			CheckError(err, "err:%s")
			continue
		}
		_, err = file.WriteString(fmt.Sprintf("%s\n", v.Value))
		if err != nil {
			CheckError(err, "err:%s")
		}

	}

}

// doReduce effectue une tâche de réduction en lisant les fichiers
// intermédiaires, en regroupant les valeurs par clé, et en appliquant
// la fonction reduceF.
// A COMPLETER
func DoReduce(
	jobName string,
	reduceTaskNumber int,
	nMap int,
	reduceF func(key string, values []string) string,
) {
	// Créer une map pour stocker les valeurs par clé

	// Lire les fichiers intermédiaires produits par chaque tâche map
	for i := 0; i < nMap; i++ {

		// Ouvrir le fichier pour la tâche de mappage i

		// Lire les paires clé-valeur du fichier
		// Ajouter la valeur à la liste de valeurs pour cette clé

	}
	// Ouvrir le fichier de sortie pour la tâche de réduction
	// utiliser mergeName

	// Appliquer la fonction de réduction à chaque clé
	for _, key := range keys {
		// Appliquer reduceF pour réduire les valeurs associées à cette clé
		// Écrire la clé et la valeur réduite dans le fichier de sortie
	}
}
