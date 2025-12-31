package lease


import (
	"time"
)


type TaskID string

// Question: Why not have this generic any ?
type Task struct {
	ID TaskID
	Payload []byte
}

type leasedTask struct {
	task Task
	leasedUntil time.Time
}

func NewLeasedTask(task Task, leasedUntil time.Time) leasedTask {
	return leasedTask { task: task, leasedUntil: leasedUntil }
}


type LeasedTaskHeap []leasedTask

func (h LeasedTaskHeap) Len() int { return len(h) }
func (h LeasedTaskHeap) Less(i, j int) bool {
	return h[i].leasedUntil.Before(h[j].leasedUntil)
}
func (h LeasedTaskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}


func (h *LeasedTaskHeap) Push(lt leasedTask) {
	*h = append(*h, lt)
}

func ( h *LeasedTaskHeap) Pop() leasedTask {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0:n-1]
	return x
}

func NewLeasedTaskHeap() *LeasedTaskHeap {
	return &LeasedTaskHeap{}
}
