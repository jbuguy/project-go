package tests

import (
	"testing"
	"reflect"
)


func TestMapF_Case(t *testing.T) {
	content := "ORANGE Banana bananA ApplE orange baNana"
	expectedKeys := map[string]string{}
	expectedKeys["banana"]="3"
	expectedKeys["orange"]="2"
	expectedKeys["apple"]="1"

	res := mapF(content)
	gotKeys := map[string]string{}
	for _, kv := range res {
		gotKeys[kv.Key]=kv.Value
	}
	if !reflect.DeepEqual(gotKeys, expectedKeys) {
		t.Errorf("mapF failed, got %v, want %v", res, expectedKeys)
	}
}

func TestMapF(t *testing.T) {
	content := "orange banana banana apple orange banana"
	expectedKeys := map[string]string{}
	expectedKeys["banana"]="3"
	expectedKeys["orange"]="2"
	expectedKeys["apple"]="1"

	res := mapF(content)
	gotKeys := map[string]string{}
	for _, kv := range res {
		gotKeys[kv.Key]=kv.Value
	}
	if !reflect.DeepEqual(gotKeys, expectedKeys) {
		t.Errorf("mapF failed, got %v, want %v", res, expectedKeys)
	}
}

func TestReduceF(t *testing.T) {
	values := []string{"1", "2", "1"}
	expected := "4"
	res := reduceF("dummy", values)
	if res != "4" {
		t.Errorf("reduceF failed, got %s , expected %s", res, expected)
	}
}
