package lease


import (
	"fmt"
	"time"
	"sync"
)





type Lease struct {
	leasedTasks map[TaskID]leasedTask
	leasedTaskHeap *LeasedTaskHeap
	taskList []Task

	ackEvents chan leasedTask
	timerChan chan *time.Time

	sync.Mutex
}

func NewLease() *Lease {
	return &Lease{
		leasedTasks: make(map[TaskID]leasedTask),
		leasedTaskHeap: NewLeasedTaskHeap(),
		taskList: []Task{},
		ackEvents: make(chan leasedTask),
		timerChan: make(chan *time.Time),
	}
}

func NewQueue() *Lease {
	return &Lease{}
}


/*
Mental note:
1. I need to have a logic to pick the nearest timer.
2. I need to use a heap implmentation for this.
*/

func (l *Lease) Enqueue(t Task) {
	l.Lock()
	defer l.Unlock()
	l.taskList = append(l.taskList, t)
}

func (l *Lease) ResetTimer(timeout time.Duration) {
}

func (l *Lease) Lease(timeout time.Duration) *Task {
	if len(l.taskList) == 0 {
		return nil
	}
	currTime := time.Now()

	l.Lock()
	defer l.Unlock()
	task := l.taskList[0]
	leasedTil := currTime.Add(timeout)
	leasedTask := NewLeasedTask(task, leasedTil)

	l.leasedTasks[task.ID] = leasedTask
	l.taskList = l.taskList[1:]

	return &task
}

func (l *Lease) Ack(taskID TaskID) error {
	l.Lock()
	defer l.Unlock()
	if _, ok := l.leasedTasks[taskID]; !ok {
		return fmt.Errorf("task is not being leased");
	}
	l.ackEvents <- l.leasedTasks[taskID]
	return nil
}
