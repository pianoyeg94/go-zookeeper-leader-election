package leaderelection

import "github.com/go-zookeeper/zk"

const invalidSessionId = ^int64((1 << 63) - 1) // min value for int64

type sessionStatus int8

const (
	sessionStatusDisconnected sessionStatus = iota
	sessionStatusLost
	sessionStatusReacquired
	sessionStatusAcquired
)

type sessionEvent struct {
	sessionId     int64
	sessionStatus sessionStatus
	err           error
}

type (
	sessionEventsQueue struct {
		head, tail *sessionEventsQueueElem
		length     int
	}

	sessionEventsQueueElem struct {
		event *zk.Event
		next  *sessionEventsQueueElem
	}
)

func (q *sessionEventsQueue) peek() *zk.Event {
	if q.head == nil {
		return nil
	}
	return q.head.event
}

func (q *sessionEventsQueue) enqueue(event *zk.Event) {
	q.length++
	elem := &sessionEventsQueueElem{event: event}
	if q.tail == nil {
		q.tail, q.head = elem, elem
		return
	}
	q.tail.next, q.tail = elem, elem
}

func (q *sessionEventsQueue) dequeue() *zk.Event {
	if q.head == nil {
		return nil
	}
	q.length--
	elem := q.head
	q.head, elem.next = elem.next, nil
	if q.head == nil {
		q.tail = nil
	}

	return elem.event
}

func (q *sessionEventsQueue) len() int {
	return q.length
}
