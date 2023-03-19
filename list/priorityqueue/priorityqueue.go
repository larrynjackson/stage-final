package priorityqueue

import (
	"lnj.com/unix/sockets/list"
	"lnj.com/unix/sockets/message-handlers"
)

type PQueue struct {
	list list.DListNode
}

func CreatePQueue() list.DListNode {
	list := list.CreateDList()
	return list
}

func (pq *PQueue) PriorityPut(payload *message.Transport, p int) {
	pq.list.PriorityPut(payload, p)
}

func (pq *PQueue) Take() (ok bool, payload *message.Transport) {
	return pq.list.Take()
}

func (pq *PQueue) Head() (ok bool, payload *message.Transport) {
	ok, pl := pq.list.Head()

	if ok {
		return ok, pl
	}
	return false, &message.Transport{}
}

func (pq *PQueue) IsEmpty() (isEmpty bool) {
	return pq.list.IsEmpty()
}

func (pq *PQueue) GetNodeCount() (nodeCount int) {
	return pq.list.GetNodeCount()
}
