package mapreduce

import (
	"fmt"
	"strconv"
	"strings"
)

// The mapping function is called once for each piece of the input.
// In this framework, the value is the contents of the file being
// processed. The return value should be a slice of key/value pairs,
// each represented by a mapreduce.KeyValue.
// A COMPLETER
func MapWordCount(value string) (res []KeyValue) {
	counts := make(map[string]int)
	words := strings.Fields(value)
	for _, word := range words {
		counts[strings.ToLower(word)] += 1
	}
	var result []KeyValue
	for k, v := range counts {
		result = append(result, KeyValue{k, fmt.Sprint(v)})
	}
	return result
}

// The reduce function is called once for each key generated by Map,
// with a list of that key's string value (merged across all
// inputs). The return value should be a single output value for that
// key.
// A COMPLETER
func ReduceWordCount(key string, values []string) string {
	count := 0
	for _, v := range values {
		value, err := strconv.Atoi(v)
		if err == nil {
			count += value
		}
	}
	return fmt.Sprint(count)
}
