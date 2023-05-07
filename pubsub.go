package observer

import (
	"sync"
)

type Subscriber[M comparable] struct {
	topic string
	ch    chan M
	ready chan struct{}
	p     *Pubsub[M]
}

type pubsubMessage[M comparable] struct {
	topic   string
	message M
}

type Pubsub[M comparable] struct {
	shutdown    chan struct{}
	wg          sync.WaitGroup
	ready       chan struct{}
	input       chan pubsubMessage[M]
	clients     map[string][]Subscriber[M]
	subscribe   chan Subscriber[M]
	unsubscribe chan Subscriber[M]
	// Used to gather termination information
	remaining int
}

func NewPubsub[M comparable]() *Pubsub[M] {
	var p Pubsub[M]
	p.shutdown = make(chan struct{})
	p.clients = map[string][]Subscriber[M]{}
	p.input = make(chan pubsubMessage[M])
	p.subscribe = make(chan Subscriber[M])
	p.unsubscribe = make(chan Subscriber[M])
	p.wg = sync.WaitGroup{}
	p.ready = make(chan struct{}, 1)
	p.runpubsub()
	<-p.ready
	return &p
}

func (p *Pubsub[M]) Shutdown() {
	close(p.shutdown)
	p.wg.Wait()
}

func (p *Pubsub[M]) Publish(topic string, message M) {
	p.input <- pubsubMessage[M]{topic: topic, message: message}
}

func (p *Pubsub[M]) runpubsub() {
	p.wg.Add(1)
	go func() {
		p.ready <- struct{}{}
		defer p.wg.Done()
		for running := true; running; {
			select {
			case <-p.shutdown:
				// No more subcription handling
				close(p.subscribe)
				close(p.unsubscribe)
				// Close any remaining client channels
				for _, subsSet := range p.clients {
					for _, sub := range subsSet {
						p.remaining++
						close(sub.ch)
					}
				}
				// Deref original map
				p.clients = map[string][]Subscriber[M]{}
				running = false
			case sub := <-p.subscribe:
				list, has := p.clients[sub.topic]
				if has {
					list = append(list, sub)
				} else {
					list = []Subscriber[M]{sub}
				}
				p.clients[sub.topic] = list
				sub.ready <- struct{}{}
			case sub := <-p.unsubscribe:
				if list, has := p.clients[sub.topic]; has {
					for i := 0; i < len(list); i++ {
						if list[i] == sub {
							p.clients[sub.topic] = append(list[:i], list[i+1:]...)
							break
						}
					}
					if len(p.clients[sub.topic]) == 0 {
						delete(p.clients, sub.topic)
					}
				}
				close(sub.ready)
			case msg := <-p.input:
				if list, has := p.clients[msg.topic]; has {
					for _, sub := range list {
						p.safeWrite(sub.ch, msg.message)
					}
				}
			}
		}
	}()
}

// Unsubscribing client closes channel
// this is necessary to avoid deadlocks sending to a client
// which would then in turn block the main routine handler
func (p *Pubsub[M]) safeWrite(ch chan M, message M) {
	defer func() {
		recover()
	}()
	ch <- message
}

// Client calls

func (p *Pubsub[M]) Subscribe(topic string, bufLen ...int) *Subscriber[M] {
	var s Subscriber[M]
	s.topic = topic
	if len(bufLen) < 1 {
		s.ch = make(chan M)
	} else {
		s.ch = make(chan M, bufLen[0])
	}
	s.ready = make(chan struct{})
	s.p = p
	p.subscribe <- s
	<-s.ready
	return &s
}

func (s *Subscriber[M]) CH() chan M {
	return s.ch
}

func (s *Subscriber[M]) Unsubscribe() {
	close(s.ch)
	s.p.unsubscribe <- *s
	<-s.ready
}
