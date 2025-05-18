package main

import (
	"fmt"
	"strconv"
	"strings"
)

type KeyValue struct {
	key   string
	value string
}
type IWorker interface {
	doMap(jobName string, mapTaskNumber int, inFile string, nReduce int, mapF func(
		file string, contents string) []KeyValue)
}

func mapF(document, content string) []KeyValue {
	counts := make(map[string]int)
	words := strings.Fields(content)
	for _, word := range words {
		counts[strings.ToLower(word)] += 1
	}
	var result []KeyValue
	for k, v := range counts {
		result = append(result, KeyValue{k, fmt.Sprint(v)})
	}
	return result
}
func reduceF(key string, values []string) string {
	count := 0
	for _, v := range values {
		value, err := strconv.Atoi(v)
		if err == nil {
			count += value
		}
	}
	return fmt.Sprint(count)
}
func doMap(jobName string, mapTaskNumber int, inFile string, nReduce int, mapF func(
	file string, contents string) []KeyValue) {

		common
}
