package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"project/dist/commons"
	"time"
)

func setuprpc() net.Listener {
	fmt.Println("init rpc")
	rpc.Register(&master)
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		return nil
	}
	fmt.Println("finished init rpc")
	return listener
}
func (master *Master) GetId(args *struct{}, id *string) error {
	*id = fmt.Sprintf("%d", master.id)
	master.id += 1
	return nil
}
func (master *Master) GetTask(args commons.Args, reply *commons.Task) error {
	log.Printf("worker %s trying to get a task", args.Id)
	master.mutex.Lock()
	if len(master.tasks) > 0 {

		*reply = master.tasks[0]
		master.working.clients[args.Id] = Client{*reply, time.Now()}
		x := fmt.Sprintf("%s%d", reply.JobName, reply.TaskNumber)
		master.completed[x] = false
		master.tasks = master.tasks[1:]
		master.assignTask(args.Id, reply)
		master.mutex.Unlock()
		return nil
	}
	fmt.Println("no waiting task ")
	taskchan := make(chan commons.Task, 1)
	master.waiting = append(master.waiting, taskchan)
	master.mutex.Unlock()
	fmt.Println("waiting for a task to be available")
	*reply = <-taskchan
	master.mutex.Lock()
	master.assignTask(args.Id, reply)
	master.mutex.Unlock()
	return nil
}
func (master *Master) ReportTaskDone(args commons.Args2, reply *bool) error {
	log.Print("task: ", args)
	master.mutex.Lock()
	defer master.mutex.Unlock()
	master.completed[fmt.Sprintf("%s%d", args.JobName, args.TaskNumber)] = true
	*reply = true
	remove(master, args.JobName, args.TaskNumber)
	if master.completedTasks() == master.numtasks {
		log.Printf("stage completed pasiing to next stage")
		master.stage <- 1
	}
	return nil
}
func (master *Master) Ping(args commons.Args, reply *bool) error {
	log.Println(args.Id)
	client, ok := master.working.clients[args.Id]
	if !ok {
		*reply = false
		return nil
	}
	client.t = time.Now()
	master.working.clients[args.Id] = client
	*reply = true
	return nil
}
