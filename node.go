package queue

import (
	"log"
	"sync/atomic"
	"fmt"
)

type TaskRunner interface {
	Run(node *Node) error
	String() string
}

type Node struct {
	isWorking  int32
	isShutdown int32
	ID         int
}

func (n *Node) runTask(task TaskRunner) error {
	log.Printf("[%v] Receive task: %s Type: %T", n, task, task)
	defer log.Printf("[%v] Complete task: %s", n, task)

	atomic.AddInt32(&n.isWorking, 1)
	defer atomic.AddInt32(&n.isWorking, -1)

	return task.Run(n)
}

func (n *Node) Start(tasks <-chan TaskRunner) {
	log.Printf("[%v] Start loop", n)

	for {
		if atomic.LoadInt32(&n.isShutdown) != 0 {
			log.Printf("[%v-node] Close loop", n.ID)
			return
		}

		select {
		case task := <-tasks:
			n.runTask(task)
		}
	}
}

func (n *Node) String() string {
	return fmt.Sprintf("%v-node", n.ID)
}

func (n *Node) Stop() {
	atomic.AddInt32(&n.isShutdown, 1)
	log.Printf("[%v] Receive Stop", n)
}

func (n *Node) IsRunning() bool {
	return atomic.LoadInt32(&n.isWorking) != 0
}
