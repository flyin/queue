package queue

import (
	"log"
	"sync/atomic"
)

type jobber interface {
	Run(worker *Worker) error
	String() string
}

type Worker struct {
	isWorking  int32
	isShutdown int32
	ID         int
}

func (w *Worker) Start(jobsCh <-chan jobber) {
	log.Printf("[%v-worker] Start loop", w.ID)

	for {
		if atomic.LoadInt32(&w.isShutdown) != 0 {
			log.Printf("[%v-worker] Close loop", w.ID)
			return
		}

		select {
		case j := <-jobsCh:
			w.do(j)
		}
	}
}

func (w *Worker) do(job jobber) error {
	log.Printf("[%v-worker] Receive job: %s Type: %T", w.ID, job, job)
	defer log.Printf("[%v-worker] Complete job: %s", w.ID, job)

	atomic.AddInt32(&w.isWorking, 1)
	defer atomic.AddInt32(&w.isWorking, -1)

	return job.Run(w)
}

func (w *Worker) Stop() {
	atomic.AddInt32(&w.isShutdown, 1)
	log.Printf("[%v-worker] Receive Stop", w.ID)
}

func (w *Worker) IsRunning() bool {
	return atomic.LoadInt32(&w.isWorking) != 0
}
