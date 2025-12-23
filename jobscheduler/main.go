package main

import (
	"sort"
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

func runJobs(ctx context.Context, tasks []Task) {
	now := time.Now()
	for i := 0; i < len(tasks); i++ {
		wait := tasks[i].scheduledTime.Sub(now)

		if wait > 0 {
			select {
			case <- time.After(wait):
			case <- ctx.Done():
				return
			}
		}

		tasks[i].action()
		now = tasks[i].scheduledTime
	}
}

func cancelJobs(cancel context.CancelFunc) {
	cancel()
}


func orderTasks(tasks []Task) []Task {
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].scheduledTime.Before(tasks[j].scheduledTime)
	})
	return tasks
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
	tasks = orderTasks(tasks)

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
