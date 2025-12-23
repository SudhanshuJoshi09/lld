package main

import (
	"os"
	"sort"
	"fmt"
	"sync"
	"time"
	"context"
	"encoding/json"
)



const poolSize = 2
const fileName = "content.json"



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
	stopped chan struct{}
	timer *time.Timer

	execute chan Task
	result chan Result
}

func NewScheduler(tasks []Task) *Scheduler {
	add := make(chan Task)
	execute := make(chan Task)
	result := make(chan Result)
	stopped := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	return &Scheduler{
		tasks: tasks,
		add: add,

		ctx: ctx,
		cancel: cancel,

		execute: execute,
		result: result,
		stopped: stopped,
	}
}


type Task struct {
	Name string `json:"task_name"`
	action func()
	ScheduledTime time.Time `json:"scheduled_time"`

	RepeatCount int `json:"repeat_count"`
	Interval time.Duration `json:"interval"`
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

	currTask.RepeatCount -= 1
	if currTask.RepeatCount > 0 {
		next := currTask.ScheduledTime.Add(currTask.Interval)
		now := time.Now()

		if next.Before(now) {
			missed := int(now.Sub(currTask.ScheduledTime) / currTask.Interval)
			next = currTask.ScheduledTime.Add(time.Duration(missed+1) * currTask.Interval)
		}
		currTask.ScheduledTime = next

		return &currTask
	}
	return nil
}


func (s *Scheduler) runJobs() {
	defer close(s.stopped)
	defer  fmt.Println("RUNJOBS Closed \n")
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

		wait := time.Until(s.tasks[0].ScheduledTime)
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
		// default:
		// 	fmt.Println("CHECKING 1234")
		// 	fmt.Println("Dropping task with name :: ", s.tasks[0].Name)
		// 	s.tasks = s.tasks[1:]
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
		return s.tasks[i].ScheduledTime.Before(s.tasks[j].ScheduledTime)
	})
}


func (s *Scheduler) startScheduler() {
	var wg sync.WaitGroup

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

	wg.Wait()
}

func clock() {
	for {
		fmt.Println(time.Now())
		time.Sleep(1 * time.Second)
	}
}

func (s *Scheduler) storeSchedulerState() {
	s.cancelJobs()
	<- s.stopped
	content, err := json.Marshal(s.tasks)
	if err != nil {
		fmt.Println(err)
	}
	os.WriteFile("content.json", content, 0644)
}

func (s *Scheduler) restoreTasks() []Task {
	content, err := os.ReadFile("content.json")
	if err != nil {
		fmt.Println(err)
	}

	result := make([]Task, 0)
	err = json.Unmarshal(content, &result)
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < len(result); i++ {
		result[i].action = fetchAction(result[i].Name)
	}
	return result
}


func fetchAction(taskName string) func() {
	switch taskName {
	case "Task-03":
		return func() {
			fmt.Println("job 3 executed")
			time.Sleep(2 * time.Second)
		}
	case "Task-02":
		return func() {
			fmt.Println("job 2 executed")
			time.Sleep(2 * time.Second)
		}
	}
	fmt.Println(taskName)
	return func() { fmt.Println("Task not found") }
}


func main() {
	var wg sync.WaitGroup
	s := NewScheduler([]Task{})
	now := time.Now()

	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	clock()
	// }()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.startScheduler()
	}()


	newTask := Task{
		Name: "Task-03",
		ScheduledTime: now.Add(time.Second * 2),
		action: fetchAction("Task-03"),
		RepeatCount: 10,
		Interval: time.Second * 1,
	}
	newTask2 := Task{
		Name: "Task-02",
		ScheduledTime: now.Add(time.Second * 1),
		action: fetchAction("Task-02"),
		RepeatCount: 10,
		Interval: time.Second * 1,
	}

	s.addTask(newTask)
	s.addTask(newTask2)

	time.Sleep(10 * time.Second)
	s.storeSchedulerState()

	tasks := s.restoreTasks()
	newS := NewScheduler(tasks)

	wg.Add(1)
	go func() {
		defer wg.Done()
		newS.startScheduler()
	}()

	wg.Wait()
}
