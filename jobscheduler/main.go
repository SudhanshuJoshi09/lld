package main

import (
	"sort"
	"fmt"
	"sync"
	"time"
	"context"
)

type Scheduler struct {
	tasks []Task
	add chan Task

	ctx context.Context
	cancel context.CancelFunc
}


func NewScheduler(tasks []Task) *Scheduler {
	add := make(chan Task)
	ctx, cancel := context.WithCancel(context.Background())

	return &Scheduler{
		tasks: tasks,
		add: add,

		ctx: ctx,
		cancel: cancel,
	}
}


type Task struct {
	name string
	action func()
	scheduledTime time.Time
}

func (s *Scheduler) runJobs() {
	for true {
		if len(s.tasks) == 0 {
			select {
			case <- s.ctx.Done():
				fmt.Println("Cancel event triggered")
				return
			case v, ok := <- s.add:
				if !ok {
					fmt.Println("channel closed -> done")
					return
				}
				s.tasks = append(s.tasks, v)
				continue
			}
		}

		select {
		case <- time.After(time.Until(s.tasks[0].scheduledTime)):
			s.tasks[0].action()
			s.tasks = s.tasks[1:]
		case <- s.ctx.Done():
			return
		case v, ok := <- s.add:
			if !ok {
				fmt.Println("done")
				return
			}
			s.tasks = append(s.tasks, v)
			s.orderTasks()
			continue
		}
	}
}

func (s *Scheduler) addTask(task Task) {
	s.add <- task
}

func (s *Scheduler) cancelJobs() {
	s.cancel()
}


func (s *Scheduler) orderTasks() {
	sort.Slice(s.tasks, func(i, j int) bool {
		return s.tasks[i].scheduledTime.Before(s.tasks[j].scheduledTime)
	})
}

func main() {
	var wg sync.WaitGroup
	now := time.Now()
	tasks := []Task{
		{
			name: "Task - 01",
			scheduledTime: now.Add(time.Second * 4),
			action: func() { fmt.Println("job 1 executed") },
		},
		{
			name: "Task - 02",
			scheduledTime: now.Add(time.Second * 8),
			action: func() { fmt.Println("job 2 executed") },
		},
	}
	s := NewScheduler(tasks)
	s.orderTasks()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			fmt.Println(time.Now())
			time.Sleep(1 * time.Second)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runJobs()
	}()
	time.Sleep(time.Second * 1)

	newTask := Task{
		name: "Task - 03",
		scheduledTime: now.Add(time.Second * 3),
		action: func() { fmt.Println("job 3 executed") },
	}

	s.addTask(newTask)

	time.Sleep(time.Second * 8)

	close(s.add)

	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	s.cancelJobs()
	// }()

	wg.Wait()
}
