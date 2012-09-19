// This package provides a priority queue implementation and
// scaffold interfaces.
//
// Copyright (C) 2011 by Krzysztof Kowalik <chris@nu7hat.ch>
package pqueue

import (
	"container/heap"
	"errors"
	"sync"
)

// Only Items implementing this interface can be enqueued
// on the priority queue.
type Interface interface {
	Less(other interface{}) bool
}

// Queue is a threadsafe priority queue exchange. Here's
// a trivial example of usage:
//
//     q := pqueue.New(0)
//     go func() {
//         for {
//             task := q.Dequeue()
//             println(task.(*CustomTask).Name)
//         }
//     }()
//     for i := 0; i < 100; i := 1 {
//         task := CustomTask{Name: "foo", priority: rand.Intn(10)}
//         q.Enqueue(&task)
//     }
//
type Queue struct {
	Limit int
	Items *Sorter
	cond  *sync.Cond
}

// New creates and initializes a new priority queue, taking
// a limit as a parameter. If 0 given, then queue will be
// unlimited. 
func New(max int) (q *Queue) {
	var locker sync.Mutex
	q = &Queue{Limit: max}
	q.Items = new(Sorter)
	q.cond = sync.NewCond(&locker)
	heap.Init(q.Items)
	return
}

// Enqueue puts given item to the queue.
func (q *Queue) Enqueue(item Interface) (err error) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.Limit > 0 && q.Len() >= q.Limit {
		return errors.New("Queue limit reached")
	}
	heap.Push(q.Items, item)
	q.cond.Signal()
	return
}

// Dequeue takes an item from the queue. If queue is empty
// then should block waiting for at least one item.
func (q *Queue) Dequeue() (item Interface) {
	q.cond.L.Lock()
start:
	x := heap.Pop(q.Items)
	if x == nil {
		q.cond.Wait()
		goto start
	}
	q.cond.L.Unlock()
	item = x.(Interface)
	return
}

func (q *Queue) Peek() Interface {
	return (*q.Items)[0]
}

func (q *Queue) Heapify() {
	heap.Init(q.Items)
}

// Safely changes enqueued Items limit. When limit is set
// to 0, then queue is unlimited.
func (q *Queue) ChangeLimit(newLimit int) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.Limit = newLimit
}

// Len returns number of enqueued elemnents.
func (q *Queue) Len() int {
	return q.Items.Len()
}

// IsEmpty returns true if queue is empty.
func (q *Queue) IsEmpty() bool {
	return q.Len() == 0
}

type Sorter []Interface

func (s *Sorter) Push(i interface{}) {
	item, ok := i.(Interface)
	if !ok {
		return
	}
	*s = append((*s)[:], item)
}

func (s *Sorter) Pop() (x interface{}) {
	if s.Len() > 0 {
		l := s.Len() - 1
		x = (*s)[l]
		(*s)[l] = nil
		*s = (*s)[:l]
	}
	return
}

func (s *Sorter) Len() int {
	return len((*s)[:])
}

func (s *Sorter) Less(i, j int) bool {
	return (*s)[i].Less((*s)[j])
}

func (s *Sorter) Swap(i, j int) {
	if s.Len() > 0 {
		(*s)[i], (*s)[j] = (*s)[j], (*s)[i]
	}
}
