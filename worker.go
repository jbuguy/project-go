package main

import "strings"

type KeyValue[Key, Value interface{}] struct {
	key   Key
	value Value
}

func mapF(document, content string) []KeyValue[string, int] {
	counts := make(map[string]int)
	words := strings.Fields(content)
	for _, word := range words {
		counts[strings.ToLower(word)] += 1
	}
	var result []KeyValue[string, int]
	for k, v := range counts {
		result = append(result, KeyValue[string, int]{k, v})
	}
	return result
}
