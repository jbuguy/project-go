package main

import (
	"fmt"
	"strings"
)

type KeyValue struct {
	Key   string
	Value string
}

func mapF(document string, content string) []KeyValue {
	words := strings.Fields(content)
	kvs := []KeyValue{}

	for _, word := range words {
		cleaned := strings.ToLower(strings.Trim(word, ".,!?\"';:()[]{}"))
		if cleaned != "" {
			kvs = append(kvs, KeyValue{Key: cleaned, Value: "1"})
		}
	}

	return kvs
}

func reduceF(key string, values []string) string {
	return fmt.Sprintf("%d", len(values))
}

func doMap(document string, content string) []KeyValue {
	return mapF(document, content)
}

func doReduce(intermediate []KeyValue) map[string]string {
	grouped := make(map[string][]string)
	for _, kv := range intermediate {
		grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
	}

	result := make(map[string]string)
	for key, values := range grouped {
		result[key] = reduceF(key, values)
	}

	return result
}

func main() {
	doc := "doc1"
	content := "Hello world! Hello MapReduce. MapReduce is great."

	intermediate := doMap(doc, content)
	reduced := doReduce(intermediate)

	for k, v := range reduced {
		fmt.Printf("%s: %s\n", k, v)
	}
}
