package broker

import (
	"container/list"
	"fmt"
	"sync"
)

type Queue struct {
	Name     string
	messages *list.List
	lock     sync.Mutex
}

func NewQueue(name string) Queue {
	return Queue{name, list.New(), sync.Mutex{}}
}

func (q *Queue) Push(msg Message) (bool, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.messages.PushFront(msg)
	return true, nil
}

func (q *Queue) Pop() (*Message, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.safePop()
}

// func (q *Queue) Take(n int) ([]*Message, error) {
// 	q.lock.Lock()
// 	defer q.lock.Unlock()
// }

func (q *Queue) safePop() (*Message, error) {
	if q.messages.Len() == 0 {
		return nil, fmt.Errorf("Can't pop from an empty queue!")
	}

	m := q.messages.Front()
	q.messages.Remove(m)
	msg := m.Value.(Message)
	return &msg, nil
}
