package main

import (
	"fmt"
	"strconv"
	"strings"
)

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
func reduceF(key string, values []string) string {
	count := 0
	for _, v := range values {
		value,err:= strconv.Atoi(v)
		if err==nil{
			count+=value
		}
	}
	return fmt.Sprint(count)
}
