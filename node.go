package queue

import (
	"fmt"
	"log"
	"sync/atomic"
)

// Runner is a foreign task runner contract
type Runner interface {
	Run() error
}

// Node represents one task runner thread
type Node struct {
	state struct {
		busy     int32
		shutdown int32
	}

	ID    int
	tasks <-chan Runner
}

// NewNode returns new Node and start listening tasks
func NewNode(id int, tasks <-chan Runner) *Node {
	node := &Node{ID: id, tasks: tasks}

	go func(n *Node) {
		for {
			if atomic.LoadInt32(&n.state.shutdown) != 0 {
				return
			}

			task := <-n.tasks
			atomic.AddInt32(&n.state.busy, 1)
			err := task.Run()
			atomic.AddInt32(&n.state.busy, -1)

			if err != nil {
				log.Printf("[%v] error occured: %v", n, err)
				continue
			}
		}
	}(node)

	return node
}

func (n *Node) String() string {
	return fmt.Sprintf("%v-node", n.ID)
}

// Stop turns node into shutdown state
func (n *Node) Stop() {
	atomic.AddInt32(&n.state.shutdown, 1)
}

// Running returns actual node busy state
func (n *Node) Running() bool {
	return atomic.LoadInt32(&n.state.busy) != 0
}
