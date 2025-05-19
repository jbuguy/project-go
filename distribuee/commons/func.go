package commons

import (
	"io"
	"os"
	"strconv"
)

const Prefix = "./files/"

func ConcatFiles(destination string, sources []string) error {
	// Créer ou ouvrir le fichier de destination
	destFile, err := os.Create(destination)
	if err != nil {
		return err
	}
	defer destFile.Close()

	// Copier le contenu de chaque fichier source
	for _, src := range sources {
		srcFile, err := os.Open(src)
		if err != nil {
			return err
		}
		_, err = io.Copy(destFile, srcFile)
		srcFile.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
func ReduceName(jobName string, mapTask int, reduceTask int) string {
	return Prefix + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func MergeName(jobName string, reduceTask int) string {
	return Prefix + jobName + "-res-" + strconv.Itoa(reduceTask)
}

// ansName constructs the name of the output file of the final answer
func AnsName(jobName string) string {
	return Prefix + jobName
}
func CleanIntermediary(jobName string, nMap, nReduce int) {
	// Supprimer les fichiers intermédiaires produits les tâches map
	for reduceTNbr := 0; reduceTNbr < nReduce; reduceTNbr++ {
		for mapTNbr := 0; mapTNbr < nMap; mapTNbr++ {
			os.Remove(ReduceName(jobName, mapTNbr, reduceTNbr))
		}
		os.Remove(MergeName(jobName, reduceTNbr))
	}
}
