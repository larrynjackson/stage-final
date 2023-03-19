package list

import (
	"fmt"

	"lnj.com/unix/sockets/message-handlers"
)

type DListNode struct {
	name      string
	next      *DListNode
	prev      *DListNode
	payload   *message.Transport
	priority  int
	nodeCount int
}

func CreateDList() DListNode {

	head := &DListNode{}
	head.name = "HEAD"
	head.next = head
	head.prev = head
	head.payload = &message.Transport{}
	head.priority = -1
	head.nodeCount = 0
	return *head
}

func (h *DListNode) Push(pl *message.Transport) {
	h.InsertRight(pl, 0, "node")
}

func (h *DListNode) Put(pl *message.Transport) {
	h.InsertRight(pl, 0, "node")
}

func (h *DListNode) InsertRight(pl *message.Transport, p int, n string) {
	h.nodeCount += 1
	nn := &DListNode{
		payload:  pl,
		priority: p,
		name:     n,
	}
	nn.next = h.next
	h.next = nn
	nn.prev = h
	if h.prev.name == "HEAD" {
		h.prev = nn
	} else {
		nn.next.prev = nn
	}

}

func (h *DListNode) PriorityPut(pl *message.Transport, p int) {
	h.InsertRightPriority(pl, p, "node")
}

func (h *DListNode) InsertRightPriority(pl *message.Transport, p int, n string) {
	h.nodeCount += 1
	nn := &DListNode{
		payload:  pl,
		priority: p,
		name:     n,
	}
	tmp := h

	if tmp.prev.name == "HEAD" {
		nn.next = tmp.next
		tmp.next = nn
		nn.prev = tmp
		tmp.prev = nn
	} else if nn.priority > tmp.next.priority {
		nn.next = tmp.next
		tmp.next = nn
		nn.prev = tmp
		nn.next.prev = nn
	} else if nn.priority < tmp.prev.priority {
		tmp.prev.next = nn
		nn.prev = tmp.prev
		tmp.prev = nn
		nn.next = tmp
	} else {
		lag := tmp
		tmp = tmp.next
		for nn.priority < tmp.priority {
			lag = tmp
			tmp = tmp.next
		}
		lag.next = nn
		nn.prev = lag
		tmp.prev = nn
		nn.next = tmp
	}
}

func (h *DListNode) Pop() (ok bool, payload *message.Transport) {
	return h.RemoveRight()
}

func (h *DListNode) RemoveRight() (ok bool, payload *message.Transport) {
	if h.next.name == "HEAD" {
		return false, &message.Transport{}
	}
	h.nodeCount -= 1
	list := h.next
	h.next = list.next
	list.next.prev = h
	list.next = nil
	list.prev = nil
	return true, list.payload
}

func (h *DListNode) Top() (ok bool, payload *message.Transport) {
	return h.LookRight()
}

func (h *DListNode) LookRight() (ok bool, payload *message.Transport) {
	if h.next.name == "HEAD" {
		return false, &message.Transport{}
	}
	list := h.next
	return true, list.payload
}

func (h *DListNode) Take() (ok bool, payload *message.Transport) {
	return h.RemoveLeft()
}

func (h *DListNode) RemoveLeft() (ok bool, payload *message.Transport) {
	if h.prev.name == "HEAD" {
		return false, &message.Transport{}
	}
	h.nodeCount -= 1
	list := h.prev
	h.prev = list.prev
	list.prev.next = h
	list.prev = nil
	list.next = nil
	return true, list.payload
}

func (h *DListNode) Head() (ok bool, payload *message.Transport) {
	return h.LookLeft()
}

func (h *DListNode) LookLeft() (ok bool, payload *message.Transport) {
	if h.prev.name == "HEAD" {
		return false, &message.Transport{}
	}
	list := h.prev
	return true, list.payload
}

func (h *DListNode) PrintStack() {
	h.DisplayRight()
}

func (h *DListNode) DisplayRight() {
	list := h.next
	for list.name != "HEAD" {
		fmt.Printf("(%+v P %+v) -> ", list.payload.Data, list.priority)
		list = list.next
	}
	fmt.Println()
}

func (h *DListNode) PrintQueue() {
	h.DisplayLeft()
}

func (h *DListNode) PrintPQueue() {
	h.DisplayLeft()
}

func (h *DListNode) DisplayLeft() {
	list := h.prev
	for list.name != "HEAD" {
		fmt.Printf("(%+v P %+v) -> ", list.payload.Data, list.priority)
		list = list.prev
	}
	fmt.Println()
}

func (h *DListNode) IsEmpty() bool {
	return h.nodeCount == 0
}

func (h *DListNode) GetNodeCount() int {
	return h.nodeCount
}
