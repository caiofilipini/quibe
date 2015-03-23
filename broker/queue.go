package broker

import (
	"container/list"
	"errors"
	"fmt"
	"sync"

	"github.com/caiofilipini/quibe/transport"
)

var (
	ErrEmptyQueue = errors.New("The queue is empty.")
)

type Queue struct {
	Name     string
	messages *list.List
	lock     sync.Mutex
}

func NewQueue(name string) Queue {
	return Queue{name, list.New(), sync.Mutex{}}
}

func (q *Queue) Push(msg transport.Message) error {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.messages.PushBack(msg)
	return nil
}

func (q *Queue) Pop() (*transport.Message, error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.safePop()
}

// func (q *Queue) Take(n int) ([]*transport.Message, error) {
// 	q.lock.Lock()
// 	defer q.lock.Unlock()
// }

func (q *Queue) safePop() (*transport.Message, error) {
	if q.messages.Len() == 0 {
		return nil, ErrEmptyQueue
	}

	m := q.messages.Front()
	q.messages.Remove(m)
	msg := m.Value.(transport.Message)
	return &msg, nil
}

func (q *Queue) peek() {
	fmt.Println("****** BEGIN PEEK *****")
	for e := q.messages.Front(); e != nil; e = e.Next() {
		m := e.Value.(transport.Message)
		fmt.Println(m.BodyString())
	}
	fmt.Println("****** END PEEK *****")
}
