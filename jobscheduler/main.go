package main

import (
	"sort"
	"fmt"
	"sync"
	"time"
	"context"
)

const poolSize = 1


type Result struct {
	task       Task
	finishedAt time.Time
	err        error
}

type Scheduler struct {
	tasks []Task
	add chan Task

	ctx context.Context
	cancel context.CancelFunc
	timer *time.Timer

	execute chan Task
	result chan Result
}

func NewScheduler(tasks []Task) *Scheduler {
	add := make(chan Task)
	execute := make(chan Task)
	result := make(chan Result)
	ctx, cancel := context.WithCancel(context.Background())

	return &Scheduler{
		tasks: tasks,
		add: add,

		ctx: ctx,
		cancel: cancel,

		execute: execute,
		result: result,
	}
}


type Task struct {
	name string
	action func()
	scheduledTime time.Time

	repeatCount int
	interval time.Duration
}

func (s *Scheduler) executeTasks() {
	var wg sync.WaitGroup

	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for {
				select {
				case v, ok := <- s.execute:
					if !ok {
						fmt.Println("Execute chan closed exiting...")
						return
					}
					v.action()
					s.result <- Result{task: v, finishedAt: time.Now(), err: nil}
				case <- s.ctx.Done():
					fmt.Println("Cancelled called exiting ...")
					return
				}
			}
		}(i)
	}

	wg.Wait()
}


func (s *Scheduler) generateRecuringTask(currTask Task) *Task {

	currTask.repeatCount -= 1
	if currTask.repeatCount > 0 {
		next := currTask.scheduledTime.Add(currTask.interval)
		now := time.Now()

		if next.Before(now) {
			missed := int(now.Sub(currTask.scheduledTime) / currTask.interval)
			next = currTask.scheduledTime.Add(time.Duration(missed+1) * currTask.interval)
		}

		currTask.scheduledTime = next
		fmt.Println(currTask.name, currTask.repeatCount)

		return &currTask
	}
	return nil
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
			case v, ok := <- s.result:
				if !ok {
					fmt.Println("closed the result channel exiting...")
					return
				}
				newTask := s.generateRecuringTask(v.task)
				if newTask != nil {
					s.tasks = append(s.tasks, *newTask)
					s.orderTasks()
				}
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
		case v, ok := <- s.result:
			if !ok {
				fmt.Println("closed the result channel exiting...")
				return
			}
			newTask := s.generateRecuringTask(v.task)
			if newTask != nil {
				s.tasks = append(s.tasks, *newTask)
				s.orderTasks()
			}
		}

		currTask := s.tasks[0]

		select {
		case s.execute <- currTask:
			// handoff succeeded, scheduler stays alive
			s.tasks = s.tasks[1:]
		case <-s.ctx.Done():
			// shutdown beats scheduling
			return
		default:
			fmt.Println("CHECKING 1234")
			fmt.Println("Dropping task with name :: ", s.tasks[0].name)
			s.tasks = s.tasks[1:]
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
		// {
		// 	name: "Task - 01",
		// 	scheduledTime: now.Add(time.Second * 4),
		// 	action: func() { fmt.Println("job 1 executed") },
		// },
		// {
		// 	name: "Task - 02",
		// 	scheduledTime: now.Add(time.Second * 8),
		// 	action: func() { fmt.Println("job 2 executed") },
		// },
	}
	s := NewScheduler(tasks)
	s.orderTasks()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			fmt.Println(time.Now())
			time.Sleep(1 * time.Second)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runJobs()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.executeTasks()
	}()
	// time.Sleep(time.Second * 1)

	newTask := Task{
		name: "Task - 03",
		scheduledTime: now.Add(time.Second * 3),
		action: func() {
			fmt.Println("job 3 executed")
			time.Sleep(5 * time.Second)
		},
		repeatCount: 10,
		interval: time.Second * 1,
	}
	newTask2 := Task{
		name: "Task - 02",
		scheduledTime: now.Add(time.Second * 3),
		action: func() {
			fmt.Println("job 2 executed")
			time.Sleep(5 * time.Second)
		},
		repeatCount: 10,
		interval: time.Second * 1,
	}

	s.addTask(newTask)
	s.addTask(newTask2)

	time.Sleep(time.Second * 20)

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.cancelJobs()
	}()

	wg.Wait()
}
