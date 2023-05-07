package observer

import (
	"sync"
)

type subscriber[M comparable] struct {
	topic string
	ch    chan M
	ready chan struct{}
	p     *pubsub[M]
}

type pubsubMessage[M comparable] struct {
	topic   string
	message M
}

type pubsub[M comparable] struct {
	shutdown    chan struct{}
	wg          sync.WaitGroup
	ready       chan struct{}
	input       chan pubsubMessage[M]
	clients     map[string][]subscriber[M]
	subscribe   chan subscriber[M]
	unsubscribe chan subscriber[M]
	// Used to gather termination information
	remaining int
}

func NewPubsub[M comparable]() *pubsub[M] {
	var p pubsub[M]
	p.shutdown = make(chan struct{})
	p.clients = map[string][]subscriber[M]{}
	p.input = make(chan pubsubMessage[M])
	p.subscribe = make(chan subscriber[M])
	p.unsubscribe = make(chan subscriber[M])
	p.wg = sync.WaitGroup{}
	p.ready = make(chan struct{}, 1)
	p.runpubsub()
	<-p.ready
	return &p
}

func (p *pubsub[M]) Shutdown() {
	close(p.shutdown)
	p.wg.Wait()
}

func (p *pubsub[M]) Publish(topic string, message M) {
	p.input <- pubsubMessage[M]{topic: topic, message: message}
}

func (p *pubsub[M]) runpubsub() {
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
				p.clients = map[string][]subscriber[M]{}
				running = false
			case sub := <-p.subscribe:
				list, has := p.clients[sub.topic]
				if has {
					list = append(list, sub)
				} else {
					list = []subscriber[M]{sub}
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
func (p *pubsub[M]) safeWrite(ch chan M, message M) {
	defer func() {
		recover()
	}()
	ch <- message
}

// Client calls

func (p *pubsub[M]) Subscribe(topic string) *subscriber[M] {
	var s subscriber[M]
	s.topic = topic
	s.ch = make(chan M)
	s.ready = make(chan struct{})
	s.p = p
	p.subscribe <- s
	<-s.ready
	return &s
}

func (s *subscriber[M]) CH() chan M {
	return s.ch
}

func (s *subscriber[M]) Unsubscribe() {
	close(s.ch)
	s.p.unsubscribe <- *s
	<-s.ready
}
