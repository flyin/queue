package queue

import (
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
	tasks      <-chan TaskRunner
}

func NewNode(id int, tasks <-chan TaskRunner) *Node {
	node := &Node{ID: id, tasks:tasks}
	go node.Start()
	return node
}

func (n *Node) runTask(task TaskRunner) error {
	atomic.AddInt32(&n.isWorking, 1)
	defer atomic.AddInt32(&n.isWorking, -1)

	return task.Run(n)
}

func (n *Node) Start() {
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

func (n *Node) Stop() {
	atomic.AddInt32(&n.isShutdown, 1)
}

func (n *Node) IsRunning() bool {
	return atomic.LoadInt32(&n.isWorking) != 0
}
