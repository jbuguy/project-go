package tests

import (
	"mr/mapreduce"
	"os"
	"testing"
)

func TestMapReduceSequential(t *testing.T) {
	input := "input_test.txt"
	_ = os.WriteFile(input, []byte("foo bar foo baz foo bar"), 0644)
	defer os.Remove(input)
	mapreduce.Sequential("testjob", []string{input}, 2, mapF, reduceF)
	filename := "mrtmp.testjob"
	expected := map[string]string{}
	expected["foo"]= "3"
	expected["bar"]= "2"
	expected["baz"]= "1"
	
	got:=decodeMapFromFile(t, filename)
	defer os.Remove(filename)
	assertEqualMaps(t, got,expected)
	mapreduce.CleanIntermediary("testjob",1,2)
}
