package mapreduce

import (
	"fmt"
	"log"
	"os"
	"io"
)

// Debugging enabled?
const debugEnabled = true


// Debug will only print if the debugEnabled const has been set to true
func Debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// Propagate error if it exists
func CheckError(err error, format string, a ...interface{}) {
	if err != nil {
		fmt.Printf(format, a...)
		log.Fatal(err)
	}
}

func concatFiles(destination string, sources []string) error {
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
