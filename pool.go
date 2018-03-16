package queue

import (
	"context"
	"time"
)

type Pool struct {
	isShutdown int32
	nodes      []*Node
	tasks      chan TaskRunner
}

func (p *Pool) Start(workers int) {
	p.tasks = make(chan TaskRunner, 100)

	for idx := 0; idx < workers; idx++ {
		p.nodes = append(p.nodes, NewNode(idx, p.tasks))
	}
}

func (p *Pool) Shutdown(ctx context.Context) error {
	for _, node := range p.nodes {
		node.Stop()
	}

	t := time.NewTicker(100 * time.Microsecond)
	defer t.Stop()

	for {
		if p.IsIdle() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (p *Pool) AddTask(task TaskRunner) {
	// TODO Add shutdown check
	p.tasks <- task
}

func (p *Pool) IsIdle() bool {
	for _, node := range p.nodes {
		if node.IsRunning() {
			return false
		}
	}

	return true
}
