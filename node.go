package queue

import (
	"fmt"
	"sync/atomic"
)

// TaskRunner is foreign task runner contract
type TaskRunner interface {
	Run(node *Node) error
}

// Node represent one task runner thread
type Node struct {
	isWorking  int32
	isShutdown int32
	ID         int
	tasks      <-chan TaskRunner
}

// NewNode returns new Node and start listening tasks
func NewNode(id int, tasks <-chan TaskRunner) *Node {
	node := &Node{ID: id, tasks: tasks}
	go node.start()
	return node
}

func (n *Node) runTask(task TaskRunner) error {
	atomic.AddInt32(&n.isWorking, 1)
	defer atomic.AddInt32(&n.isWorking, -1)

	return task.Run(n)
}

func (n *Node) start() {
	for {
		if atomic.LoadInt32(&n.isShutdown) != 0 {
			return
		}

		select {
		case task := <-n.tasks:
			n.runTask(task)
		}
	}
}

func (n *Node) String() string {
	return fmt.Sprintf("%v-node", n.ID)
}

// Stop sets isShudown flag
func (n *Node) Stop() {
	atomic.AddInt32(&n.isShutdown, 1)
}

// IsRunning returns actual node state
func (n *Node) IsRunning() bool {
	return atomic.LoadInt32(&n.isWorking) != 0
}
