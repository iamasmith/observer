package observer

import (
	"context"
	"sync"
	"time"
)

type Subscriber[M comparable] struct {
	topic  string
	ch     chan M
	ready  chan struct{}
	active bool
	l      sync.RWMutex
	p      *Pubsub[M]
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
	clients     map[string][]*Subscriber[M]
	l           sync.RWMutex
	subscribe   chan *Subscriber[M]
	unsubscribe chan *Subscriber[M]
	active      bool
	// Used to gather termination information
	remaining int
}

func NewPubsub[M comparable]() *Pubsub[M] {
	var p Pubsub[M]
	p.shutdown = make(chan struct{})
	p.clients = map[string][]*Subscriber[M]{}
	p.l = sync.RWMutex{}
	p.input = make(chan pubsubMessage[M])
	p.subscribe = make(chan *Subscriber[M])
	p.unsubscribe = make(chan *Subscriber[M])
	p.wg = sync.WaitGroup{}
	p.ready = make(chan struct{}, 1)
	p.runSubscription()
	p.runPublish()
	<-p.ready
	return &p
}

func (p *Pubsub[M]) Shutdown() {
	close(p.shutdown)
	p.wg.Wait()
}

func (p *Pubsub[M]) Publish(topic string, message M) {
	if p.active {
		p.input <- pubsubMessage[M]{topic: topic, message: message}
	}
}

func (p *Pubsub[M]) runSubscription() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for running := true; running; {
			select {
			case <-p.shutdown:
				// No more subcription handling
				close(p.subscribe)
				close(p.unsubscribe)
				// Close any remaining client channels
				p.l.Lock()
				for _, subsSet := range p.clients {
					for _, sub := range subsSet {
						p.remaining++
						sub.active = false
						close(sub.ch)
					}
				}
				// Deref original map
				p.clients = map[string][]*Subscriber[M]{}
				p.l.Unlock()
				running = false
			case sub := <-p.subscribe:
				p.l.Lock()
				list, has := p.clients[sub.topic]
				if has {
					list = append(list, sub)
				} else {
					list = []*Subscriber[M]{sub}
				}
				p.clients[sub.topic] = list
				p.l.Unlock()
				sub.l.Lock()
				sub.active = true
				sub.l.Unlock()
				sub.ready <- struct{}{}
			case sub := <-p.unsubscribe:
				p.l.Lock()
				if list, has := p.clients[sub.topic]; has {
					for i := 0; i < len(list); i++ {
						if list[i] == sub {
							p.clients[sub.topic] = append(list[:i], list[i+1:]...)
							sub.l.Lock()
							sub.active = false
							sub.l.Unlock()
							close(sub.ch)
							break
						}
					}
					if len(p.clients[sub.topic]) == 0 {
						delete(p.clients, sub.topic)
					}
				}
				close(sub.ready)
				p.l.Unlock()
			}
		}
	}()
}

func (p *Pubsub[M]) runPublish() {
	p.wg.Add(1)
	go func() {
		p.active = true
		p.ready <- struct{}{}
		defer p.wg.Done()
		var working []*Subscriber[M]
		for running := true; running; {
			select {
			case <-p.shutdown:
				p.active = false
				running = false
			case msg := <-p.input:
				p.l.RLock()
				list, has := p.clients[msg.topic]
				if c, l := cap(working), len(list); c < l {
					working = append(working, (make([]*Subscriber[M], l-c))...)
				}
				copy(working, list)
				p.l.RUnlock()
				if has {
					for _, sub := range working {
						sub.l.RLock()
						if sub.active {
							sub.ch <- msg.message
						}
						sub.l.RUnlock()
					}
				}
			}
		}
	}()
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
	p.subscribe <- &s
	<-s.ready
	return &s
}

func (s *Subscriber[M]) CH() chan M {
	return s.ch
}

func (s *Subscriber[M]) Unsubscribe() {
	s.p.unsubscribe <- s
}

// Needed for high frequency send
func (s *Subscriber[M]) Drain(timeout time.Duration) {
	// defer func() {
	// 	if err := recover(); err != nil {
	// 		log.Printf("Drain error: %#v", err)
	// 	}
	// }()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for running := true; running; {
		select {
		case <-s.ch:
		case <-ctx.Done():
			running = false
		}
	}
}
