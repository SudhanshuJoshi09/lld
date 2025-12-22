package main

import (
	"fmt"
	"sync"
	"time"
	"context"
)

type Task struct {
	name string
	action func()
	scheduledTime time.Time
}

func runJob(ctx context.Context, task *Task) {
	select {
	case <- time.After(time.Until(task.scheduledTime)):
		task.action()
	case <- ctx.Done():
		return
	}
}

func runJobs(ctx context.Context, tasks []Task) {
	var wg sync.WaitGroup
	for i := 0; i < len(tasks); i++ {
		wg.Add(1)
		go func(task *Task) {
			defer wg.Done()
			runJob(ctx, task)
		}(&tasks[i])
	}
	wg.Wait()
}

func cancelJobs(cancel context.CancelFunc) {
	cancel()
}


func main() {
	var wg sync.WaitGroup
	tasks := []Task{
		{
			name: "Task - 01",
			scheduledTime: time.Now().Add(time.Second * 4),
			action: func() { fmt.Println("job 1 executed") },
		},
		{
			name: "Task - 02",
			scheduledTime: time.Now().Add(time.Second * 8),
			action: func() { fmt.Println("job 2 executed") },
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	wg.Add(1)
	go func() {
		defer wg.Done()
		runJobs(ctx, tasks)
	}()
	time.Sleep(time.Second * 6)

	wg.Add(1)
	go func() {
		defer wg.Done()
		cancelJobs(cancel)
	}()

	wg.Wait()
}
