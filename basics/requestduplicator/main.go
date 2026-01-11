package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

//////////////////////
// Result Storage
//////////////////////

type ResultStorage struct {
	mu    sync.Mutex
	store map[string]Response
	ch    chan Response
}

func NewResultStorage() *ResultStorage {
	return &ResultStorage{
		store: make(map[string]Response),
		ch:    make(chan Response, 16),
	}
}

func (rs *ResultStorage) Run(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case resp := <-rs.ch:
				rs.mu.Lock()
				rs.store[resp.GetID()] = resp
				rs.mu.Unlock()
			}
		}
	}()
}

func (rs *ResultStorage) Push(resp Response) {
	rs.ch <- resp
}

func (rs *ResultStorage) Dump() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for _, v := range rs.store {
		fmt.Println(v.GetPayload())
	}
}

//////////////////////
// Requests / Responses
//////////////////////

type Request interface {
	GetID() string
	Process() Response
}

type DuplicableRequest interface {
	Request
	Clone() Request
}

type HttpRequest struct {
	id      string
	payload string
}

func (r *HttpRequest) GetID() string {
	return r.id
}

func (r *HttpRequest) Process() Response {
	time.Sleep(200 * time.Millisecond)
	return &HttpResponse{
		id:      r.id,
		payload: r.payload,
	}
}

func (r *HttpRequest) Clone() Request {
	return &HttpRequest{
		id:      r.id,
		payload: r.payload,
	}
}

type Response interface {
	GetID() string
	GetPayload() string
}

type HttpResponse struct {
	id      string
	payload string
}

func (r *HttpResponse) GetID() string {
	return r.id
}

func (r *HttpResponse) GetPayload() string {
	return r.payload
}

//////////////////////
// Request Observer (Worker Pool)
//////////////////////

type RequestObserver struct {
	queue chan Request
	store *ResultStorage
}

func NewRequestObserver(store *ResultStorage, queueSize int) *RequestObserver {
	return &RequestObserver{
		queue: make(chan Request, queueSize),
		store: store,
	}
}

func (ro *RequestObserver) Run(ctx context.Context, wg *sync.WaitGroup, workers int) {
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case req := <-ro.queue:
					ro.store.Push(req.Process())
				}
			}
		}()
	}
}

func (ro *RequestObserver) Enqueue(req Request) {
	select {
	case ro.queue <- req:
	default:
		// drop on overload (intentional)
	}
}

//////////////////////
// Request Duplicator
//////////////////////

type RequestDuplicator struct {
	observers []*RequestObserver
	store     *ResultStorage

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewRequestDuplicator() *RequestDuplicator {
	ctx, cancel := context.WithCancel(context.Background())

	store := NewResultStorage()
	observer := NewRequestObserver(store, 16)

	return &RequestDuplicator{
		observers: []*RequestObserver{observer},
		store:     store,
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (rd *RequestDuplicator) Start() {
	rd.store.Run(rd.ctx, &rd.wg)
	for _, o := range rd.observers {
		o.Run(rd.ctx, &rd.wg, 2)
	}
}

func (rd *RequestDuplicator) Stop() {
	rd.cancel()
	rd.wg.Wait()
}

func (rd *RequestDuplicator) Process(req DuplicableRequest) Response {
	for _, o := range rd.observers {
		o.Enqueue(req.Clone())
	}
	return req.Process()
}

//////////////////////
// Main
//////////////////////

func main() {
	rd := NewRequestDuplicator()
	rd.Start()

	resp := rd.Process(&HttpRequest{
		id:      "123",
		payload: "hello world",
	})

	fmt.Println("Primary response:", resp.GetPayload())

	time.Sleep(1 * time.Second)

	fmt.Println("Observer results:")
	rd.store.Dump()

	rd.Stop()
}

