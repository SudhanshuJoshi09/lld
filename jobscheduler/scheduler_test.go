package jobscheduler

import (
	"context"
	// "fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

/*
	TEST-ONLY GLOBALS
	-----------------
	These exist to serialize scheduler tests and prevent goroutine overlap.
	This is intentional technical debt. Stub for later cleanup.
*/

var schedulerTestMu sync.Mutex

// -------------------- TEST UTILITIES --------------------

func withTempDir(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	err := os.MkdirAll(filepath.Join(dir, ".output"), 0755)
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
}

func startSchedulerForTest(t *testing.T) (*Scheduler, context.CancelFunc) {
	t.Helper()

	s := NewScheduler()
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel

	go s.monitorEvents()
	go s.startScheduler()

	t.Cleanup(func() {
		cancel()
		// give background goroutines time to exit
		time.Sleep(20 * time.Millisecond)
	})

	return s, cancel
}

// wait until event metrics stop changing
func waitForEventsToSettle(t *testing.T, s *Scheduler) {
	t.Helper()

	var last int
	deadline := time.After(2 * time.Second)

	for {
		cur :=
			len(s.eventStore.FetchMetric(EventScheduled)) +
				len(s.eventStore.FetchMetric(EventDispatched)) +
				len(s.eventStore.FetchMetric(EventExecuted)) +
				len(s.eventStore.FetchMetric(EventFailed)) +
				len(s.eventStore.FetchMetric(EventRetryScheduled))

		if cur == last {
			time.Sleep(20 * time.Millisecond)
			return
		}
		last = cur

		select {
		case <-deadline:
			t.Fatal("events never settled")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// -------------------- TEST MAIN --------------------

func TestMain(m *testing.M) {
	_ = os.MkdirAll(".output", 0755)

	if err := initLogger(); err != nil {
		panic(err)
	}

	code := m.Run()
	os.Exit(code)
}

// -------------------- TESTS --------------------

func Test_TaskExecutesOnce(t *testing.T) {
	schedulerTestMu.Lock()
	defer schedulerTestMu.Unlock()

	withTempDir(t)

	executed := make(chan struct{}, 1)

	task := Task{
		ID:            "single@1",
		Name:          "single",
		ScheduledTime: time.Now().Add(20 * time.Millisecond),
		ScheduleCount: 1,
		RetryPolicy:   defaultRetryPolicy,
		action: func(ctx context.Context) error {
			executed <- struct{}{}
			return nil
		},
	}

	s, _ := startSchedulerForTest(t)
	s.AddNewTask(task)

	select {
	case <-executed:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("task did not execute")
	}

	waitForEventsToSettle(t, s)

	if len(s.eventStore.FetchMetric(EventScheduled)) == 0 {
		t.Fatal("expected EventScheduled")
	}
	if len(s.eventStore.FetchMetric(EventDispatched)) == 0 {
		t.Fatal("expected EventDispatched")
	}
	if len(s.eventStore.FetchMetric(EventExecuted)) == 0 {
		t.Fatal("expected EventExecuted")
	}
}

func Test_RetryStopsAtMax(t *testing.T) {
	schedulerTestMu.Lock()
	defer schedulerTestMu.Unlock()

	withTempDir(t)

	var mu sync.Mutex
	attempts := 0

	task := Task{
		ID:            "retry@1",
		Name:          "retry",
		ScheduledTime: time.Now().Add(10 * time.Millisecond),
		ScheduleCount: 1,
		RetryPolicy: RetryPolicy{
			MaxRetries: 2,
			BaseDelay:  20 * time.Millisecond,
		},
		action: func(ctx context.Context) error {
			mu.Lock()
			attempts++
			mu.Unlock()
			return os.ErrInvalid
		},
	}

	s, _ := startSchedulerForTest(t)
	s.AddNewTask(task)

	deadline := time.After(2 * time.Second)
	for {
		if len(s.eventStore.FetchMetric(EventFailed)) >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf(
				"timeout: attempts=%d failed=%d",
				attempts,
				len(s.eventStore.FetchMetric(EventFailed)),
			)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	waitForEventsToSettle(t, s)

	dispatches := len(s.eventStore.FetchMetric(EventDispatched))
	if dispatches < 3 {
		t.Fatalf("expected at least 3 dispatches, got %d", dispatches)
	}
	if got := len(s.eventStore.FetchMetric(EventFailed)); got != 3 {
		t.Fatalf("expected 3 failures, got %d", got)
	}
	if got := len(s.eventStore.FetchMetric(EventRetryScheduled)); got != 2 {
		t.Fatalf("expected 2 retry schedules, got %d", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func Test_RecurringTaskContinuesAfterFailure(t *testing.T) {
	schedulerTestMu.Lock()
	defer schedulerTestMu.Unlock()

	withTempDir(t)

	var mu sync.Mutex
	count := 0

	task := Task{
		ID:            "recurring@1",
		Name:          "recurring",
		ScheduledTime: time.Now().Add(10 * time.Millisecond),
		ScheduleCount: 3,
		Interval:      40 * time.Millisecond,
		RetryPolicy: RetryPolicy{
			MaxRetries: 1,
			BaseDelay:  10 * time.Millisecond,
		},
		action: func(ctx context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			count++
			if count == 1 {
				return os.ErrInvalid
			}
			return nil
		},
	}

	s, _ := startSchedulerForTest(t)
	s.AddNewTask(task)

	time.Sleep(300 * time.Millisecond)
	waitForEventsToSettle(t, s)

	mu.Lock()
	defer mu.Unlock()
	if count < 3 {
		t.Fatalf("expected recurring task to continue, got %d", count)
	}
}

func Test_CancelInFlightTask(t *testing.T) {
	schedulerTestMu.Lock()
	defer schedulerTestMu.Unlock()

	withTempDir(t)

	started := make(chan struct{}, 1)

	task := Task{
		ID:            "cancel@1",
		Name:          "cancel",
		ScheduledTime: time.Now().Add(10 * time.Millisecond),
		ScheduleCount: 1,
		RetryPolicy:   defaultRetryPolicy,
		action: func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	s, _ := startSchedulerForTest(t)
	s.AddNewTask(task)

	select {
	case <-started:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("task never started")
	}

	s.cancelInFlightTask(task.ID)
	waitForEventsToSettle(t, s)
}

func Test_RestoreDoesNotDuplicateExecution(t *testing.T) {
	schedulerTestMu.Lock()
	defer schedulerTestMu.Unlock()

	withTempDir(t)

	var mu sync.Mutex
	count := 0

	task := Task{
		ID:            "persist@1",
		Name:          "persist",
		ScheduledTime: time.Now().Add(10 * time.Millisecond),
		ScheduleCount: 1,
		RetryPolicy:   defaultRetryPolicy,
		action: func(ctx context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			count++
			return nil
		},
	}

	s, cancel := startSchedulerForTest(t)
	s.AddNewTask(task)
	time.Sleep(100 * time.Millisecond)
	s.StoreSchedulerState()
	cancel()

	s2, _ := startSchedulerForTest(t)
	_ = s2

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Fatalf("expected exactly one execution after restart, got %d", count)
	}
}

