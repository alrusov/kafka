package kafka

import (
	"testing"
)

//----------------------------------------------------------------------------------------------------------------------------//

func Test1(t *testing.T) {
	// TODO
}

//----------------------------------------------------------------------------------------------------------------------------//

func TestEventHandlerManipulation(t *testing.T) {
	f1 := func(c *Consumer, assigned bool, partitions TopicPartitions) {
	}

	f2 := func(c *Consumer, assigned bool, partitions TopicPartitions) {
	}

	c := &Consumer{}
	c.AddEventHandler(f1)
	if len(c.eventHandlers) != 1 {
		t.Fatalf("[1] len is %d, expected %d", len(c.eventHandlers), 1)
	}

	c.AddEventHandler(f2)
	if len(c.eventHandlers) != 2 {
		t.Fatalf("[2] len is %d, expected %d", len(c.eventHandlers), 2)
	}

	c.AddEventHandler(f1)
	if len(c.eventHandlers) != 2 {
		t.Fatalf("[3] len is %d, expected %d", len(c.eventHandlers), 2)
	}

	c.AddEventHandler(f2)
	if len(c.eventHandlers) != 2 {
		t.Fatalf("[4] len is %d, expected %d", len(c.eventHandlers), 2)
	}

	c.DelEventHandler(f1)
	if len(c.eventHandlers) != 1 {
		t.Fatalf("[5] len is %d, expected %d", len(c.eventHandlers), 1)
	}

	c.DelEventHandler(f2)
	if len(c.eventHandlers) != 0 {
		t.Fatalf("[6] len is %d, expected %d", len(c.eventHandlers), 0)
	}

}

//----------------------------------------------------------------------------------------------------------------------------//
