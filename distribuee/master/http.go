package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func initHttp() {
	fmt.Println("init http")

	http.Handle("/", http.FileServer(http.Dir("./web")))
	http.HandleFunc("/status", handleStatus)
	http.HandleFunc("/start", handleStart)
	fmt.Println("finished init http")
	http.ListenAndServe(":8080", nil)
}
func handleStatus(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(ls)

}
func handleStart(w http.ResponseWriter, r *http.Request) {
	log.Print("working on starting master")
	if r.Method != http.MethodPost {
		return
	}
	var par Par1
	err := json.NewDecoder(r.Body).Decode(&par)
	if err != nil {
		return
	}
	lines := par.Lines
	nReduce := par.NReduce
	go gMaster.run(lines, nReduce)

}
