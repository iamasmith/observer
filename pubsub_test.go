package observer

import (
	"testing"

	"github.com/google/uuid"
)

func TestSimpleSendReceive(t *testing.T) {
	p := NewPubsub[string]()
	defer p.Shutdown()
	const topic = "test"
	s := p.Subscribe(topic)
	defer s.Unsubscribe()
	message := "test1"
	p.Publish(topic, message)
	msg := <-s.CH()
	if msg != message {
		t.Errorf("Expected %s got #%s", message, msg)
	}
	message = "test2"
	p.Publish(topic, message)
	msg = <-s.CH()
	if msg != message {
		t.Errorf("Expected %s got #%s", message, msg)
	}
}

func TestSubscribeNoUnsubscribe(t *testing.T) {
	p := NewPubsub[string]()
	p.Subscribe("foo")
	p.Subscribe("foo")
	p.Subscribe("bar")
	p.Shutdown()
	if p.remaining != 3 {
		t.Errorf("Expected %d subscribed clients have %d", 3, p.remaining)
	}
}

func TestPlainStructType(t *testing.T) {
	const topic = "test"
	message := struct{}{}
	p := NewPubsub[struct{}]()
	defer p.Shutdown()
	s := p.Subscribe(topic, 1)
	defer s.Unsubscribe()
	p.Publish(topic, message)
	msg := <-s.CH()
	if msg != message {
		t.Errorf("Expected %#v got %#v", msg, message)
	}
}

type testUserStruct struct {
	id    uuid.UUID
	value string
}

func TestUserStructType(t *testing.T) {
	const topic = "test"
	u, _ := uuid.NewUUID()
	message := testUserStruct{id: u, value: "testing"}
	p := NewPubsub[testUserStruct]()
	defer p.Shutdown()
	s := p.Subscribe(topic)
	defer s.Unsubscribe()
	p.Publish(topic, message)
	msg := <-s.CH()
	if msg != message {
		t.Errorf("Expected %#v got %#v", msg, message)
	}
}

func TestBoolType(t *testing.T) {
	const topic = "test"
	message := false
	p := NewPubsub[bool]()
	defer p.Shutdown()
	s := p.Subscribe(topic)
	defer s.Unsubscribe()
	p.Publish(topic, message)
	msg := <-s.CH()
	if msg != message {
		t.Errorf("Expected %#v got %#v", msg, message)
	}
	message = true
	p.Publish(topic, message)
	msg = <-s.CH()
	if msg != message {
		t.Errorf("Expected %#v got %#v", msg, message)
	}
}

func TestAnyType(t *testing.T) {
	const topic = "test"
	p := NewPubsub[any]()
	defer p.Shutdown()
	s := p.Subscribe(topic)
	defer s.Unsubscribe()
	u, _ := uuid.NewUUID()
	mA := testUserStruct{id: u, value: "testing"}
	p.Publish(topic, mA)
	vA := <-s.CH()
	if mA != vA.(testUserStruct) {
		t.Errorf("Expected %#v got %#v", mA, vA)
	}
	mB := true
	p.Publish(topic, mB)
	vB := <-s.CH()
	if mB != vB.(bool) {
		t.Errorf("Expected %#v got %#v", mB, vB)
	}
	mC := 123
	p.Publish(topic, mC)
	vC := <-s.CH()
	if mC != vC.(int) {
		t.Errorf("Expected %#v got %#v", mC, vC)
	}
}
