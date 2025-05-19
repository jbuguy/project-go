package commons
type Args2 struct {
	JobName    string
	TaskNumber int
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