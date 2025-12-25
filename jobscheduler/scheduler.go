package jobscheduler

import (
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"
	"context"
	"encoding/json"
	// "syscall"
	// "os/signal"
)

const (
	EventScheduled = iota
	EventDispatched
	EventExecuted
	EventSkippedIdempotent
	EventDropped
	EventRescheduled
	EventShutdown
	EventCancelled
	EventFailed
	EventRetryScheduled
)

const (
	TaskExecuted = iota
	TaskDropped
	TaskCancelled
	TaskFailed
)

const (
	TaskQueue = "TaskQueue"
	RetryQueue = "RetryQueue"
)

const poolSize = 2
const taskFileName = ".output/tasks.json"
const idempotencyFileName = ".output/idempotency.json"
const eventsFileName = ".output/events.json"
const taskStateFileName = ".output/tasks.json"


var defaultRetryPolicy RetryPolicy
var logger *log.Logger

func initLogger() error {
	file, err := os.OpenFile(
		".output/scheduler.log",
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		return err
	}

	logger = log.New(
		file,
		"[scheduler] ",
		log.LstdFlags|log.Lmicroseconds|log.Lshortfile,
	)
	return nil
}


// NOTE: Initialize Config before running the program
func initConfig() {
	defaultRetryPolicy = RetryPolicy {
		MaxRetries: 2,
		BaseDelay: time.Second * 2,
	}
	err := initLogger()
	if err != nil {
		log.Fatalf("\nFailed to initialize logger %v\n", err)
	}
}



type EventType int
type ExecutionStatus int

type Event struct {
	Time time.Time
	TaskID string
	Type EventType
	MetaData map[string]string
}

type EventStore struct {
	mu sync.Mutex `json:"-"`
	file string
	EventMap map[string][]Event `json:"event_map"`
	EventMetrics map[EventType][]Event `json:"event_type_metrics"`
}

type IdempotencyStore struct {
	mu sync.Mutex

	file string
	Seen map[string]bool `json:"executed_ids"`
}

type ExecutionResult struct {
	Task       Task
	FinishedAt time.Time
	Err        error
	Attempts int
	Result ExecutionStatus
}


type RetryPolicy struct {
	MaxRetries int
	/*
	NOTE: Current Policy for delay is a absolute delay. Would follow
	planning to expand on the delay policy (we would have an interface for retry policy) after this.
	- exponential Backoff
	- jitter
	- cumulative delay
	*/
	BaseDelay time.Duration
}

// NOTE: retry scheduling is synchronous and bounded by MaxRetries.
// No async buffering here by design.
func (p RetryPolicy) Decide(r ExecutionResult) (shouldRetry bool, delay time.Duration) {
	fmt.Printf("\n\n\nCOUNT ::: %d\n\n\n", r.Attempts)
	allowedResultStatus := r.Result == TaskDropped || r.Result == TaskCancelled || r.Result == TaskFailed
	shouldRetry = allowedResultStatus && (r.Attempts < p.MaxRetries)
	if shouldRetry {
		return true, p.BaseDelay
	}
	return false, p.BaseDelay
}


type Scheduler struct {
	tasks []Task
	retries []RetryRequest
	add chan Task

	ctx context.Context
	cancel context.CancelFunc
	stopped chan struct{}
	timer *time.Timer
	timerCh <- chan time.Time

	execute chan ExecutableTask
	result chan ExecutionResult
	event chan Event

	idempotencyStore *IdempotencyStore
	eventStore *EventStore

	inFlight  map[string]context.CancelFunc

}

type ExecutableTask struct {
	Task Task `json:"task"`

	Attempts int
	ctx context.Context
	cancel context.CancelFunc
}


type RetryRequest struct {
	Task Task
	Attempt int
	ReadyAt time.Time
}

type Task struct {
	ID string `json:"id"`
	Name string `json:"task_name"`
	action func(ctx context.Context) error
	ScheduledTime time.Time `json:"scheduled_time"`

	// NOTE: The number of times this task needs scheduling, retries are exempted.
	ScheduleCount int `json:"repeat_count"`
	Interval time.Duration `json:"interval"`

	RetryPolicy RetryPolicy `json:"retry_policy"`
}


func (t *Task) IsRecurring() bool {
	return t.ScheduleCount > 1
}



func NewTask(
	name string,
	scheduledTime time.Time,
	repeatCount int,
	interval time.Duration,
	retryPolicy RetryPolicy,
) Task {
	taskID := fmt.Sprintf("%s@%d", name, scheduledTime.UnixNano())
	return Task {
		ID: taskID,
		Name: name,
		action: fetchAction(name),
		ScheduledTime: scheduledTime,
		ScheduleCount: repeatCount,
		Interval: interval,
		RetryPolicy: retryPolicy,
	}
}



func (es *EventStore) store(e Event) {
	es.mu.Lock()
	es.EventMap[e.TaskID] = append(es.EventMap[e.TaskID], e)
	es.EventMetrics[e.Type] = append(es.EventMetrics[e.Type], e)
	es.mu.Unlock()
	content, err := json.Marshal(es)
	if err != nil {
		return
	}
	err = os.WriteFile(es.file, content, 0644)
	if err != nil {
		return
	}
}


func (es *EventStore) load() {
	content, err := os.ReadFile(es.file)
	if err != nil {
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
	es.mu.Lock()
	defer es.mu.Unlock()
	fmt.Println("=== Scheduler Summary ===")

	fmt.Printf("%-30s %d\n", "Tasks scheduled:", len(es.EventMetrics[EventScheduled]))
	fmt.Printf("%-30s %d\n", "Tasks dispatched:", len(es.EventMetrics[EventDispatched]))
	fmt.Printf("%-30s %d\n", "Tasks executed:", len(es.EventMetrics[EventExecuted]))
	fmt.Printf("%-30s %d\n", "Tasks skipped (idempotent):", len(es.EventMetrics[EventSkippedIdempotent]))
	fmt.Printf("%-30s %d\n", "Tasks dropped:", len(es.EventMetrics[EventDropped]))
	fmt.Printf("%-30s %d\n", "Tasks rescheduled:", len(es.EventMetrics[EventRescheduled]))
}

func (es *EventStore) FetchMetric(eventType EventType) []Event {
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.EventMetrics[eventType]
}


func NewEventStore() *EventStore {
	return &EventStore {
		file: eventsFileName,
		EventMap: make(map[string][]Event),
		EventMetrics: make(map[EventType][]Event),
	}
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
		return
	}

	err = json.Unmarshal(content, &loaded)
	if err != nil {
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
		logger.Printf("\nunable to marshal the idempotency store content %v\n", err)
	}
	os.WriteFile(s.file, content, 0644)
	defer s.mu.Unlock()
}

func NewScheduler() *Scheduler {
	add := make(chan Task, 1000)
	execute := make(chan ExecutableTask)
	result := make(chan ExecutionResult)
	event := make(chan Event, 1000)
	stopped := make(chan struct{})

	idempotencyStore := NewIdempotencyStore()
	idempotencyStore.load()

	eventStore := NewEventStore()
	eventStore.load()

	tasks := restoreTasks()

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

		inFlight: make(map[string]context.CancelFunc),
	}
}



func (s *Scheduler) monitorEvents() {
	for {
		val, ok := <- s.event
		if !ok {
			logger.Println("Event channel closed exiting... (monitorEvents)")
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
					fmt.Println("EXECUTABLE TASK :: ", v)
					task := v.Task
					fmt.Println("TASK ID :: ", v.Task.ID)
					/*
						NOTE: Future change would fix this with using the existing task IDs
						Current implmentation is closely tied the idempotency and retry policies.
						Will finish them after few iterations
					*/
					if s.idempotencyStore.isPresent(v.Task.ID) {
						fmt.Println("EXECUTE_TASK - 1")
						s.event <- Event{Time: time.Now(), TaskID: task.ID, Type: EventSkippedIdempotent}
						logger.Println("Already seen task skipping", task.ID)
						continue
					}
					if !ok {
						fmt.Println("EXECUTE_TASK - 2")
						logger.Println("Execute chan closed exiting... (executeTasks)")
						return
					}
					errCh := make(chan error, 1)

					go func() {
						errCh <- task.action(v.ctx)
						fmt.Println("EXECUTE_TASK - 3")
					}()

					select {
					case <- v.ctx.Done():
						s.event <- Event{Time: time.Now(), TaskID: task.ID, Type: EventCancelled}
						s.result <- ExecutionResult{Task: task, FinishedAt: time.Now(), Err: v.ctx.Err(), Result: TaskCancelled, Attempts: v.Attempts}
						fmt.Println("EXECUTE_TASK - 4")
					case err := <- errCh:
						// NOTE: Policy - mark the failed events as something that were executed.
						s.idempotencyStore.store(task.ID)
						if err != nil {
							s.event <- Event{Time: time.Now(), TaskID: task.ID, Type: EventFailed}
							s.result <- ExecutionResult{Task: task, FinishedAt: time.Now(), Err: err, Result: TaskFailed, Attempts: v.Attempts}
							fmt.Println("EXECUTE_TASK - 5")
						} else {
							s.event <- Event{Time: time.Now(), TaskID: task.ID, Type: EventExecuted}
							s.result <- ExecutionResult{Task: task, FinishedAt: time.Now(), Err: err, Result: TaskExecuted, Attempts: v.Attempts}
							fmt.Println("EXECUTE_TASK - 6")
						}
					}
				case <- s.ctx.Done():
					logger.Println("Cancelled called exiting ... (executeTasks)")
					fmt.Println("EXECUTE_TASK - 7")
					return
				}
			}
		}(i)
	}

	wg.Wait()
}


func (s *Scheduler) generateRecuringTask(currTask Task) *Task {

	currTask.ScheduleCount -= 1
	if currTask.ScheduleCount > 0 {
		next := currTask.ScheduledTime.Add(currTask.Interval)
		now := time.Now()

		if next.Before(now) {
			missed := int(now.Sub(currTask.ScheduledTime) / currTask.Interval)
			next = currTask.ScheduledTime.Add(time.Duration(missed+1) * currTask.Interval)
		}
		currTask.ScheduledTime = next
		currTask.ID = fmt.Sprintf("%s@%d", currTask.Name, next.UnixNano())

		return &currTask
	}
	return nil
}

func (s *Scheduler) cleanUpTasks(r ExecutionResult) {
	switch {
	case r.Result == TaskExecuted || r.Result == TaskCancelled || r.Result == TaskFailed || r.Result == TaskDropped:
		delete(s.inFlight, r.Task.ID)
	default:
	}
}

func (s *Scheduler) rescheduleTask(r ExecutionResult) {
	var scheduledTime time.Time
	now := time.Now()

	task := r.Task
	task.ScheduleCount -= 1

	scheduledTime = task.ScheduledTime.Add(task.Interval)
	if scheduledTime.Before(now) {
		missed := int(now.Sub(task.ScheduledTime) / task.Interval)
		scheduledTime = task.ScheduledTime.Add(time.Duration(missed+1) * task.Interval)
	}

	task.ScheduledTime = scheduledTime
	task.ID = fmt.Sprintf("%s@%d", task.Name, scheduledTime.UnixNano())

	event := Event{Time: time.Now(), TaskID: r.Task.ID, Type: EventRescheduled}
	s.addNewTaskInternal(task, event)
}

func (s *Scheduler) scheduleRetryTask(task Task, delay time.Duration, attempt int) {
	now := time.Now()
	scheduledTime := now.Add(delay)
	fmt.Println("DELAY :: ", delay)
	fmt.Println("CURRENT TIME :: ", now)

	task.ScheduledTime = scheduledTime
	// IMPORTANT: retries MUST generate a new Task ID.
	// Idempotency is execution-scoped, not logical-task scoped.

	task.ID = fmt.Sprintf("%s@%d", task.Name, scheduledTime.UnixNano())
	retry := RetryRequest{ Task: task, Attempt: attempt + 1, ReadyAt: task.ScheduledTime}
	s.event <- Event{Time: time.Now(), TaskID: task.ID, Type: EventRetryScheduled}
	s.retries = append(s.retries, retry)
	s.orderRetries()
}


// NOTE: scheduling side-effects are synchronous (single-thread assumption)
func (s *Scheduler) handleExecutionResult(r ExecutionResult) {
	s.cleanUpTasks(r)

	retryPolicy := r.Task.RetryPolicy
	shouldRetry, delay := retryPolicy.Decide(r)

	if shouldRetry {
		fmt.Println(shouldRetry, r)
		s.scheduleRetryTask(r.Task, delay, r.Attempts)
		return
	}
	if r.Task.IsRecurring() {
		s.rescheduleTask(r)
		return
	}
}

func (s *Scheduler) addNewTaskInternal(t Task, event Event) {
	s.event <- event
	s.tasks = append(s.tasks, t)
	s.orderTasks()
}

type RunningQueue string

func (s *Scheduler) findRunningQueueResetTimer() RunningQueue {
	// If both are empty then, there is no task that can be tried now.
	if len(s.tasks) == 0 && len(s.retries) == 0 {
		s.timerCh = nil
		return ""
	}

	var wait time.Duration
	var runningQueue RunningQueue

	// NOTE: logic intentionally overwrites wait/queue.
	// Final decision is made after considering both queues.
	if len(s.tasks) != 0 {
		wait = time.Until(s.tasks[0].ScheduledTime)
		runningQueue = TaskQueue
	}

	if len(s.retries) != 0 {
		wait = time.Until(s.retries[0].ReadyAt)
		runningQueue = RetryQueue
	}

	if len(s.tasks) != 0 && len(s.retries) != 0 {
		if s.tasks[0].ScheduledTime.Before(s.retries[0].ReadyAt) {
			wait = time.Until(s.tasks[0].ScheduledTime)
			runningQueue = TaskQueue
		} else {
			wait = time.Until(s.retries[0].ReadyAt)
			runningQueue = RetryQueue
		}
	}

	if wait < 0 {
		wait = 0
	}
	if s.timer == nil {
		s.timer = time.NewTimer(wait)
	} else {
		if !s.timer.Stop() {
			select {
			case <- s.timer.C:
			default:
			}
		}
		s.timer.Reset(wait)
	}
	s.timerCh = s.timer.C
	return runningQueue
}

func (s *Scheduler) processRunningQueueEntry(runningQueue RunningQueue) {
	var executableTask ExecutableTask
	switch runningQueue {
	case TaskQueue:
		ctx, cancel := context.WithCancel(s.ctx)
		task := s.tasks[0]
		executableTask = ExecutableTask{Task: task, ctx: ctx}
		s.inFlight[task.ID] = cancel

		select {
		case s.execute <- executableTask:
			s.event <- Event{Time: time.Now(), TaskID: task.ID, Type: EventDispatched}
			s.tasks = s.tasks[1:]
		case <-s.ctx.Done():
			return
		default:
			s.event <- Event{Time: time.Now(), TaskID: task.ID, Type: EventDropped}
			result :=  ExecutionResult{FinishedAt: time.Now(), Task: task, Result: TaskDropped, Attempts: 0}
			s.handleExecutionResult(result)
			s.tasks = s.tasks[1:]
		}
	case RetryQueue:
		ctx, cancel := context.WithCancel(s.ctx)
		retryTask := s.retries[0]
		task := retryTask.Task
		executableTask = ExecutableTask{Task: task, ctx: ctx, Attempts: retryTask.Attempt}
		s.inFlight[retryTask.Task.ID] = cancel

		select {
		case s.execute <- executableTask:
			fmt.Println("CHECKING -- 1")
			s.event <- Event{Time: time.Now(), TaskID: task.ID, Type: EventDispatched}
			s.retries = s.retries[1:]
		case <-s.ctx.Done():
			fmt.Println("CHECKING -- 2")
			return
		default:
			fmt.Println("CHECKING -- 3")
			s.event <- Event{Time: time.Now(), TaskID: task.ID, Type: EventDropped}
			s.retries = s.retries[1:]
			result := ExecutionResult{FinishedAt: time.Now(), Task: task, Result: TaskDropped, Attempts: retryTask.Attempt}
			s.handleExecutionResult(result)
		}
	}
}

func (s *Scheduler) runJobs() {
	// NOTE: Closing the function this is required.
	defer close(s.stopped)
	defer  logger.Printf("\n\n ----------- RUNJOBS Closed ------------ \n\n")
	i := 0

	fmt.Printf("\n\nITERATION: %d\n", -1)
	fmt.Println(s.retries)
	fmt.Println(s.tasks)

	for {
		fmt.Printf("\n\nITERATION: %d\n", i)
		fmt.Println(s.retries)
		fmt.Println(s.tasks)
		// Reset the timer at the start.
		runningQueue := s.findRunningQueueResetTimer()
		fmt.Println("HERE - 1", runningQueue)
		select {
		case <- s.timerCh:
			fmt.Println("HERE - 2")
			s.processRunningQueueEntry(runningQueue)
		case <- s.ctx.Done():
			fmt.Println("HERE - 3")
			logger.Println("Cancelled called for scheduler exiting... (runJobs)")
			return
		case v, ok := <- s.add:
			fmt.Println("HERE - 4")
			if !ok {
				logger.Println("Add channel closed exiting... (runJobs)")
				return
			}
			event := Event{Time: time.Now(), TaskID: v.ID, Type: EventScheduled}
			s.addNewTaskInternal(v, event)
			fmt.Println(s.tasks)
		case result, ok := <- s.result:
			fmt.Println("HERE - 5")
			if !ok {
				logger.Println("closed the result channel exiting... (runJobs)")
				return
			}
			s.handleExecutionResult(result)
		}
		i += 1
	}
}

// addTask can block forever if 1. the scheduler exists, or is stuck in waiting sleep or the channel is never read again.
func (s *Scheduler) AddNewTask(task Task) {
	s.add <- task
}

func (s *Scheduler) CancelJobs() {
	s.cancel()
}

func (s *Scheduler) cancelInFlightTask(taskID string) {
	if cancel, ok := s.inFlight[taskID]; ok {
		cancel()
	}
}


func (s *Scheduler) orderTasks() {
	sort.Slice(s.tasks, func(i, j int) bool {
		return s.tasks[i].ScheduledTime.Before(s.tasks[j].ScheduledTime)
	})
}

func (s *Scheduler) orderRetries() {
	sort.Slice(s.retries, func(i, j int) bool {
		return s.retries[i].ReadyAt.Before(s.retries[j].ReadyAt)
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
		logger.Println(time.Now())
		time.Sleep(1 * time.Second)
	}
}

func (s *Scheduler) StoreSchedulerState() {
	s.CancelJobs()
	<- s.stopped
	content, err := json.Marshal(s.tasks)
	logger.Println("state of tasks stored (StoreSchedulerState) ", s.tasks)
	if err != nil {
		logger.Println("failed in marshalling the task list storing empty task list (StoreSchedulerState)", err)
	}
	os.WriteFile(taskStateFileName, content, 0644)
}

func restoreTasks() []Task {
	content, err := os.ReadFile(taskStateFileName)
	if err != nil {
		logger.Println("failed to read the taskStateFile return empty tasks ", err)
		return []Task{}
	}

	result := make([]Task, 0)
	err = json.Unmarshal(content, &result)
	if err != nil {
		logger.Println("unmarshalling failed returning empty tasks (restoreTasks)", err)
		return []Task{}
	}

	for i := 0; i < len(result); i++ {
		result[i].action = fetchAction(result[i].Name)
	}
	return result
}


type ActionFactory func(name string) func(context.Context) error


func fetchAction(taskName string) func(ctx context.Context) error {
	var work func() error

	switch taskName {
	case "Task-03":
		work = func() error {
			logger.Println("job 3 executed")
			time.Sleep(2 * time.Second)
			return nil
		}
	case "Task-02":
		work = func() error {
			logger.Println("job 2 executed")
			time.Sleep(2 * time.Second)
			return nil
		}
	case "Task-04":
		work = func() error {
			logger.Println("start writing ...")
			os.WriteFile("scracth", []byte("content"), 0644)
			time.Sleep(2 * time.Second)
			logger.Println("end writing ...")
			return nil
		}
	case "Task-05":
		work = func() error {
			logger.Println("TASK - 05 start")
			time.Sleep(1 * time.Second)
			logger.Println("TASK - 05 end")
			return fmt.Errorf("ERROR in task - 05")
		}
	default:
		work = func() error {
			logger.Println("Default action triggered")
			return nil
		}
	}

	wrapperFunc := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return nil
		default:
			return work()
		}
	}

	return wrapperFunc
}

// func main() {
//
// 	initConfig()
//
// 	var wg sync.WaitGroup
// 	s := NewScheduler()
// 	now := time.Now()
//
// 	fmt.Println("SCHEDULER - 01")
// 	wg.Add(1)
// 	go func() {
// 		s.monitorEvents()
// 		defer wg.Done()
// 	}()
//
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		s.startScheduler()
// 	}()
//
// 	newTask := NewTask(
// 		"Task-03",
// 		now.Add(time.Second * 2),
// 		20,
// 		time.Second * 1,
// 		defaultRetryPolicy,
// 	)
//
// 	newTask2 := NewTask(
// 		"Task-04",
// 		now.Add(time.Second * 1),
// 		20,
// 		time.Second * 1,
// 		defaultRetryPolicy,
// 	)
//
// 	newTask3 := NewTask(
// 		"Task-05",
// 		now.Add(time.Second * 1),
// 		30,
// 		time.Second * 8,
// 		defaultRetryPolicy,
// 	)
//
// 	s.AddNewTask(newTask)
// 	s.AddNewTask(newTask2)
// 	s.AddNewTask(newTask3)
//
// 	time.Sleep(10 * time.Second)
//
// 	s.eventStore.PrintTimeLine(newTask.ID)
// 	s.eventStore.PrintTimeLine(newTask2.ID)
//
// 	s.eventStore.PrintSummary()
// 	s.StoreSchedulerState()
// 	s.eventStore.PrintSummary()
//
// 	newS := NewScheduler()
//
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		newS.startScheduler()
// 	}()
//
// 	wg.Add(1)
// 	go func() {
// 		newS.monitorEvents()
// 		defer wg.Done()
// 	}()
//
// 	wg.Wait()
// }
