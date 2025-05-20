package commons

import "fmt"

type Args2 struct {
	JobName    string
	TaskNumber int
	Id         string
	TypeName   string
}
type KeyValue struct {
	Key   string
	Value string
}
type Args struct {
	Id string
}
type Task struct {
	JobName    string
	TaskNumber int
	InFile     string
	TypeName   string
	Number     int
}

func (task Task) Name() string {
	return fmt.Sprintf("%s%s%d", task.TypeName, task.JobName, task.TaskNumber)
}
