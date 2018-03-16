package queue

import (
	"context"
	"log"
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
		node := &Node{ID: idx}
		p.nodes = append(p.nodes, node)
		go node.Start(p.tasks)
	}
}

func (p *Pool) Shutdown(ctx context.Context) error {
	log.Printf("[%T] Receive shuthdow", p)

	for _, node := range p.nodes {
		go node.Stop()
	}

	t := time.NewTicker(100 * time.Microsecond)
	defer t.Stop()

	for {
		if p.IsIdle() {
			log.Printf("Pool is idle, exit then")
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
	log.Printf("Add task: %v", task)
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
