package queue

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// Pool represents collection of nodes
type Pool struct {
	state struct {
		shutdown int32
	}

	nodes []*Node
	tasks chan TaskRunner
}

func (p *Pool) idle() bool {
	for _, node := range p.nodes {
		if node.Running() {
			return false
		}
	}

	return true
}

// NewPool creates pool and run nodes
func NewPool(workers int) *Pool {
	pool := &Pool{
		tasks: make(chan TaskRunner, 100),
		nodes: make([]*Node, 0, workers),
	}

	for idx := 0; idx < workers; idx++ {
		pool.nodes = append(pool.nodes, NewNode(idx, pool.tasks))
	}

	return pool
}

// AddTask to pool
func (p *Pool) AddTask(task TaskRunner) error {
	if atomic.LoadInt32(&p.state.shutdown) != 0 {
		return errors.New("pool is in shutdown state")
	}

	p.tasks <- task
	return nil
}

// Shutdown turns pool in shutdown state. It is usefull for graceful complete all tasks
func (p *Pool) Shutdown(ctx context.Context) error {
	atomic.AddInt32(&p.state.shutdown, 1)

	for _, node := range p.nodes {
		go node.Stop()
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
