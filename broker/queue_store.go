package broker

import (
	"fmt"
	"sync"

	"github.com/caiofilipini/quibe/logger"
)

type queueStore struct {
	queues map[string]*Queue
	lock   sync.Mutex
	log    logger.Logger
}

func newQueueStore() *queueStore {
	return &queueStore{
		queues: make(map[string]*Queue),
		lock:   sync.Mutex{},
		log:    logger.NewLogger("queueStore"),
	}
}

func (s *queueStore) getOrCreate(name string) (*Queue, error) {
	if name == "" {
		return nil, fmt.Errorf("Invalid queue name: %s", name)
	}

	q, found := s.get(name)
	if found {
		return q, nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	newQ := NewQueue(name)
	s.queues[name] = &newQ
	s.log.Info(fmt.Sprintf("Created queue %s.", name))
	return &newQ, nil
}

func (s *queueStore) get(name string) (*Queue, bool) {
	q, found := s.queues[name]
	return q, found
}
