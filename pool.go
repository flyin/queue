package queue

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// Pool represents collection of nodes
type Pool struct {
	isShutdown int32
	nodes      []*Node
	tasks      chan TaskRunner
}

// NewPool creates pool and run nodes
func NewPool(workers int) *Pool {
	pool := &Pool{tasks: make(chan TaskRunner, 100)}

	for idx := 0; idx < workers; idx++ {
		pool.nodes = append(pool.nodes, NewNode(idx, pool.tasks))
	}

	return pool
}

// Shutdown all nodes. It is usefull for graceful complete all tasks
func (p *Pool) Shutdown(ctx context.Context) error {
	atomic.AddInt32(&p.isShutdown, 1)

	for _, node := range p.nodes {
		node.Stop()
	}

	t := time.NewTicker(100 * time.Microsecond)
	defer t.Stop()

	for {
		if p.idle() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// AddTask to pool
func (p *Pool) AddTask(task TaskRunner) error {
	if atomic.LoadInt32(&p.isShutdown) != 0 {
		return errors.New("pool is shutdown")
	}

	p.tasks <- task
	return nil
}

func (p *Pool) idle() bool {
	for _, node := range p.nodes {
		if node.IsRunning() {
			return false
		}
	}

	return true
}
