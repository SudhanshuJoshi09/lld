package main

import (
	"os"
	"sort"
	"fmt"
	"sync"
	"time"
	"context"
	"encoding/json"
	// "syscall"
	// "os/signal"
)

const (
	SCHEDULED = iota
	DISPATCHED
	EXECUTED
	SKIPPED_IDEMPOTENT
	DROPPED
	RESCHEDULED
	SHUTDOWN
)


const poolSize = 1
const taskFileName = "tasks.json"
const idempotencyFileName = "idempotency.json"
const eventsFileName = "events.json"


type EventType int

type Event struct {
	Time time.Time
	TaskID string
	Type EventType
	MetaData map[string]string
}

type EventStore struct {
	mu sync.Mutex
	// TaskID -> []Event
	file string
	EventMap map[string][]Event `json:"event_map"`
	EventMetrics map[EventType][]Event `json:"event_type_metrics"`
}

func (es *EventStore) store(e Event) {
	es.mu.Lock()
	es.EventMap[e.TaskID] = append(es.EventMap[e.TaskID], e)
	es.EventMetrics[e.Type] = append(es.EventMetrics[e.Type], e)
	es.mu.Unlock()
	content, err := json.Marshal(*es)
	if err != nil {
		fmt.Println("Error while storing the event values", err)
		return
	}
	err = os.WriteFile(es.file, content, 0644)
	if err != nil {
		fmt.Println("Error while storing the event values", err)
		return
	}
}



func (es *EventStore) load() {
	content, err := os.ReadFile(es.file)
	if err != nil {
		fmt.Println("Error while reading the event values", err)
		return
	}
	var loaded EventStore

	json.Unmarshal(content, &loaded)

	es.mu.Lock()
	es.EventMap = loaded.EventMap
	es.EventMetrics = loaded.EventMetrics
	defer es.mu.Unlock()
}


func (es *EventStore) PrintTimeLine(taskID string) {
	es.mu.Lock()
	defer es.mu.Unlock()
	for _, entry := range es.EventMap[taskID] {
		fmt.Println(entry)
	}
}

func (es *EventStore) PrintSummary() {
	fmt.Println("=== Scheduler Summary ===")

	fmt.Printf("%-30s %d\n", "Tasks scheduled:", len(es.EventMetrics[SCHEDULED]))
	fmt.Printf("%-30s %d\n", "Tasks dispatched:", len(es.EventMetrics[DISPATCHED]))
	fmt.Printf("%-30s %d\n", "Tasks executed:", len(es.EventMetrics[EXECUTED]))
	fmt.Printf("%-30s %d\n", "Tasks skipped (idempotent):", len(es.EventMetrics[SKIPPED_IDEMPOTENT]))
	fmt.Printf("%-30s %d\n", "Tasks dropped:", len(es.EventMetrics[DROPPED]))
	fmt.Printf("%-30s %d\n", "Tasks rescheduled:", len(es.EventMetrics[RESCHEDULED]))
}


func NewEventStore() *EventStore {
	return &EventStore {
		file: eventsFileName,
		EventMap: make(map[string][]Event),
		EventMetrics: make(map[EventType][]Event),
	}
}


type IdempotencyStore struct {
	mu sync.Mutex

	file string
	Seen map[string]bool `json:"executed_ids"`
}

func NewIdempotencyStore() *IdempotencyStore {
	return &IdempotencyStore{
		file: idempotencyFileName,
		Seen: make(map[string]bool),
	}
}

func (s *IdempotencyStore) isPresent(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.Seen[id]
	return val && ok
}

func (s *IdempotencyStore) load() {
	var loaded IdempotencyStore
	content, err := os.ReadFile(s.file)

	if err != nil {
		fmt.Println("Failed to read the file", err)
		return
	}

	err = json.Unmarshal(content, &loaded)
	if err != nil {
		fmt.Println("Failed to parse the input", err)
	}

	s.mu.Lock()
	s.Seen = loaded.Seen
	defer s.mu.Unlock()
}

func (s *IdempotencyStore) store(id string) {
	s.mu.Lock()
	s.Seen[id] = true
	content, err := json.Marshal(s)
	if err != nil {
		fmt.Println("Failed to marshal the map", err)
	}
	os.WriteFile(s.file, content, 0644)
	defer s.mu.Unlock()
}


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
	event chan Event

	idempotencyStore *IdempotencyStore
	eventStore *EventStore
}

func NewScheduler() *Scheduler {
	add := make(chan Task)
	execute := make(chan Task)
	result := make(chan Result)
	event := make(chan Event, 1000)
	stopped := make(chan struct{})

	idempotencyStore := NewIdempotencyStore()
	idempotencyStore.load()

	eventStore := NewEventStore()
	eventStore.load()

	tasks := restoreTasks()
	fmt.Println(tasks)

	ctx, cancel := context.WithCancel(context.Background())

	return &Scheduler{
		tasks: tasks,
		add: add,

		ctx: ctx,
		cancel: cancel,

		execute: execute,
		result: result,
		stopped: stopped,
		event: event,

		idempotencyStore: idempotencyStore,
		eventStore: eventStore,
	}
}


type Task struct {
	ID string `json:"id"`
	Name string `json:"task_name"`
	action func()
	ScheduledTime time.Time `json:"scheduled_time"`

	RepeatCount int `json:"repeat_count"`
	Interval time.Duration `json:"interval"`
}


func (s *Scheduler) monitorEvents() {
	for {
		val, ok := <- s.event
		if !ok {
			fmt.Println("Channel already closed")
			return
		}
		s.eventStore.store(val)
	}
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
					if s.idempotencyStore.isPresent(v.ID) {
						s.event <- Event{Time: time.Now(), TaskID: v.ID, Type: SKIPPED_IDEMPOTENT}
						fmt.Println("Already seen task skipping", v.ID)
						continue
					}
					if !ok {
						fmt.Println("Execute chan closed exiting...")
						return
					}
					v.action()
					s.idempotencyStore.store(v.ID)
					s.event <- Event{Time: time.Now(), TaskID: v.ID, Type: EXECUTED}
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
		currTask.ID = fmt.Sprintf("%s@%d", currTask.Name, next.Unix())

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
					s.event <- Event{Time: time.Now(), TaskID: newTask.ID, Type: RESCHEDULED}
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
				s.event <- Event{Time: time.Now(), TaskID: newTask.ID, Type: RESCHEDULED}
				s.orderTasks()
			}
		}

		currTask := s.tasks[0]

		select {
		case s.execute <- currTask:
			// handoff succeeded, scheduler stays alive
			s.event <- Event{Time: time.Now(), TaskID: currTask.ID, Type: DISPATCHED}
			s.tasks = s.tasks[1:]
		case <-s.ctx.Done():
			// shutdown beats scheduling
			return
		// default:
		// 	fmt.Println("CHECKING 1234")
		// 	fmt.Println("Dropping task with name :: ", s.tasks[0].Name)
		// 	s.event <- Event{Time: time.Now(), TaskID: currTask.ID, Type: DROPPED}
		// 	s.tasks = s.tasks[1:]
		}
	}
}

// addTask can block forever if 1. the scheduler exists, or is stuck in waiting sleep or the channel is never read again.
func (s *Scheduler) addTask(task Task) {
	s.event <- Event{Time: time.Now(), TaskID: task.ID, Type: SCHEDULED}
	s.add <- task
}

func (s *Scheduler) cancelJobs() {
	s.event <- Event{Time: time.Now(), Type: SHUTDOWN}
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
	fmt.Println(s.tasks)
	if err != nil {
		fmt.Println(err)
	}
	os.WriteFile("content.json", content, 0644)
}

func restoreTasks() []Task {
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
	case "Task-04":
		return func() {
			fmt.Println("start writing ...")
			os.WriteFile("scracth", []byte("content"), 0644)
			time.Sleep(2 * time.Second)
			fmt.Println("end writing ...")
		}
	}
	fmt.Println(taskName)
	return func() { fmt.Println("Task not found") }
}

func main() {
	var wg sync.WaitGroup
	s := NewScheduler()
	now := time.Now()

	fmt.Println("SCHEDULER - 01")
	wg.Add(1)
	go func() {
		s.monitorEvents()
		defer wg.Done()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.startScheduler()
	}()

	newTask := Task{
		Name: "Task-03",
		ScheduledTime: now.Add(time.Second * 2),
		action: fetchAction("Task-03"),
		RepeatCount: 20,
		Interval: time.Second * 1,
	}
	newTask2 := Task{
		Name: "Task-04",
		ScheduledTime: now.Add(time.Second * 1),
		action: fetchAction("Task-04"),
		Interval: time.Second * 1,
		RepeatCount: 20,
	}
	newTask2.ID = fmt.Sprintf("%s@%d", newTask2.Name, newTask2.ScheduledTime.Unix())
	newTask.ID = fmt.Sprintf("%s@%d", newTask.Name, newTask.ScheduledTime.Unix())

	s.addTask(newTask)
	s.addTask(newTask2)

	time.Sleep(10 * time.Second)

	s.eventStore.PrintTimeLine(newTask.ID)
	s.eventStore.PrintTimeLine(newTask2.ID)

	s.eventStore.PrintSummary()
	s.storeSchedulerState()
	s.eventStore.PrintSummary()

	newS := NewScheduler()

	wg.Add(1)
	go func() {
		defer wg.Done()
		newS.startScheduler()
	}()

	wg.Add(1)
	go func() {
		newS.monitorEvents()
		defer wg.Done()
	}()

	wg.Wait()
}
