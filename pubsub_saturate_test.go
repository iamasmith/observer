//go:build saturate
// +build saturate

package observer

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRampUpDown(t *testing.T) {
	const maxClients = 1000
	const topic = "test"
	p := NewPubsub[string]()
	defer p.Shutdown()
	current := 0
	messageCounts := make([]int, maxClients)
	l := sync.RWMutex{}
	wgS := sync.WaitGroup{}
	wgC := sync.WaitGroup{}
	clientShutdown := make(chan struct{})
	up := true
	wgS.Add(1)
	go func() {
		defer wgS.Done()
		for running := true; running; {
			message := uuid.New().String()
			p.Publish(topic, message)
			l.RLock()
			if up && current == maxClients {
				wgS.Add(1)
				up = false
				// Delay telling the clients so they can all get a few messsages
				go func() {
					defer wgS.Done()
					time.Sleep(time.Duration(2) * time.Second)
					close(clientShutdown)
				}()
			}
			if !up && current == 0 {
				running = false
			}
			l.RUnlock()
		}
		wgC.Wait()
	}()
	for c := 0; c < maxClients; c++ {
		wgC.Add(1)
		go func() {
			defer wgC.Done()
			s := p.Subscribe(topic)
			l.Lock()
			id := current
			current++
			l.Unlock()
			for running := true; running; {
				select {
				case <-clientShutdown:
					running = false
				case msg := <-s.CH():
					messageCounts[id]++
					t.Logf("%v got %v", &s, msg)
				}
			}
			l.Lock()
			current--
			l.Unlock()
			s.Unsubscribe()
			s.Drain(time.Duration(5) * time.Second)
		}()
	}
	wgS.Wait()
	t.Logf("Message counts during test %#v", messageCounts)
}
