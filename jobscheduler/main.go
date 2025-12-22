package main

import (
	"fmt"
	"sync"
	"time"
)

const (
	COMPLETED = iota
	PENDING
	CANCELLED
)


type TaskStatus int

type Task struct {
	name string
	action func()
	scheduledTime time.Time
	status TaskStatus
}

func runJob(task *Task) {
	delay := time.Until(task.scheduledTime)
	if delay > 0 {
		time.Sleep(delay)
	}
	switch task.status {
	case PENDING:
		task.action()
		task.status = COMPLETED
	default:
	}
}

func runJobs(tasks []Task) {
	var wg sync.WaitGroup
	for i := 0; i < len(tasks); i++ {
		wg.Add(1)
		go func(task *Task) {
			defer wg.Done()
			runJob(task)
		}(&tasks[i])
	}
	wg.Wait()
	fmt.Println(tasks)
}

func cancelJobs(tasks []Task) {
	for i := 0; i < len(tasks); i++ {
		switch tasks[i].status {
		case PENDING:
			tasks[i].status = CANCELLED
		default:
		}
	}
}


func main() {
	var wg sync.WaitGroup

	tasks := []Task{
		{
			name: "Task - 01",
			scheduledTime: time.Now().Add(time.Second * 4),
			action: func() { fmt.Println("job 1 executed") },
			status: PENDING,
		},
		{
			name: "Task - 02",
			scheduledTime: time.Now().Add(time.Second * 8),
			action: func() { fmt.Println("job 2 executed") },
			status: PENDING,
		},
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		runJobs(tasks)
	}()

	time.Sleep(5 * time.Second)
	wg.Add(1)
	go func() {
		defer wg.Done()
		cancelJobs(tasks)
	}()
	wg.Wait()
}
