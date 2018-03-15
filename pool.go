package queue

import (
	"context"
	"log"
	"time"
)

type Pool struct {
	isShutdown int32
	workers    []*Worker
	jobsCh     chan jobber
}

func (p *Pool) Start(numWorkers int) {
	p.jobsCh = make(chan jobber, 100)

	for idx := 0; idx < numWorkers; idx++ {
		worker := &Worker{ID: idx}
		p.workers = append(p.workers, worker)
		go worker.Start(p.jobsCh)
	}
}

func (p *Pool) Shutdown(ctx context.Context) error {
	log.Printf("[%T] Receive shuthdow", p)

	for _, worker := range p.workers {
		go worker.Stop()
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

func (p *Pool) IsIdle() bool {
	for _, worker := range p.workers {
		if worker.IsRunning() {
			return false
		}
	}

	return true
}

func (p *Pool) AddJob(job jobber) {
	log.Printf("Add job: %v", job)
	p.jobsCh <- job
}
