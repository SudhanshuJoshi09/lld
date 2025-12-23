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
	timer *time.Timer
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

	repeatCount int
	interval time.Duration
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
				s.orderTasks()
			}
			continue
		}

		wait := time.Until(s.tasks[0].scheduledTime)
		if wait < 0 {
			wait = 0
		}
		if s.timer == nil {
			s.timer = time.NewTimer(wait)
		} else {
			// Stop() tells you whether the timer had already fired
			if !s.timer.Stop() {
				// it must be running, just do away with the old value
				select {
				case <- s.timer.C:
				default:
				}
			}
			// And place the new value
			s.timer.Reset(wait)
		}


		select {
		case <- s.timer.C:
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

		currTask := s.tasks[0]
		currTask.action()
		s.tasks = s.tasks[1:]

		if currTask.repeatCount > 0 {
			currTask.scheduledTime = currTask.scheduledTime.Add(currTask.interval)
			currTask.repeatCount -= 1
			s.tasks = append(s.tasks, currTask)
			s.orderTasks()
		}
	}
}

// addTask can block forever if 1. the scheduler exists, or is stuck in waiting sleep or the channel is never read again.
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
		repeatCount: 3,
		interval: time.Second * 2,
	}

	s.addTask(newTask)

	time.Sleep(time.Second * 5)

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.cancelJobs()
	}()

	wg.Wait()
}
