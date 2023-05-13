package observer

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestSimpleSendReceive(t *testing.T) {
	assert := assert.New(t)
	p := NewPubsub[string]()
	defer p.Shutdown()
	const topic = "test"
	s := p.Subscribe(topic)
	defer s.Unsubscribe()
	message := "test1"
	p.Publish(topic, message)
	msg := <-s.CH()
	assert.Equal(msg, message, "should match")
	message = "test2"
	p.Publish(topic, message)
	msg = <-s.CH()
	assert.Equal(message, msg, "should match")
}

func TestSubscribeNoUnsubscribe(t *testing.T) {
	assert := assert.New(t)
	p := NewPubsub[string]()
	p.Subscribe("foo")
	p.Subscribe("foo")
	p.Subscribe("bar")
	p.Shutdown()
	assert.Equal(3, p.remaining, "has 3 clients")
}

func TestPlainStructType(t *testing.T) {
	assert := assert.New(t)
	const topic = "test"
	message := struct{}{}
	p := NewPubsub[struct{}]()
	defer p.Shutdown()
	s := p.Subscribe(topic, 1)
	defer s.Unsubscribe()
	p.Publish(topic, message)
	msg := <-s.CH()
	assert.Equal(message, msg, "should be equal")
}

type testUserStruct struct {
	id    uuid.UUID
	value string
}

func TestUserStructType(t *testing.T) {
	assert := assert.New(t)
	const topic = "test"
	u, _ := uuid.NewUUID()
	message := testUserStruct{id: u, value: "testing"}
	p := NewPubsub[testUserStruct]()
	defer p.Shutdown()
	s := p.Subscribe(topic)
	defer s.Unsubscribe()
	p.Publish(topic, message)
	msg := <-s.CH()
	assert.Equal(message, msg, "should be equal")
}

func TestBoolType(t *testing.T) {
	assert := assert.New(t)
	const topic = "test"
	message := false
	p := NewPubsub[bool]()
	defer p.Shutdown()
	s := p.Subscribe(topic)
	defer s.Unsubscribe()
	p.Publish(topic, message)
	msg := <-s.CH()
	assert.Equal(message, msg, "should be equal")
	message = true
	p.Publish(topic, message)
	msg = <-s.CH()
	assert.Equal(message, msg, "should be equal")
}

func TestAnyType(t *testing.T) {
	assert := assert.New(t)
	const topic = "test"
	p := NewPubsub[any]()
	defer p.Shutdown()
	s := p.Subscribe(topic)
	defer s.Unsubscribe()
	u, _ := uuid.NewUUID()
	mA := testUserStruct{id: u, value: "testing"}
	p.Publish(topic, mA)
	vA := <-s.CH()
	assert.Equal(mA, vA, "should be equal")
	mB := true
	p.Publish(topic, mB)
	vB := <-s.CH()
	assert.Equal(mB, vB, "should be equal")
	mC := 123
	p.Publish(topic, mC)
	vC := <-s.CH()
	assert.Equal(mC, vC, "should be equal")
}
