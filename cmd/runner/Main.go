package main

import (
	"fmt"
	"math/rand"

	"github.com/google/uuid"
	"github.com/rowland66/actor/pkg/eventprocessor"
	"github.com/rowland66/actor/pkg/ringbuffer"
)

var eventIdBuffer ringbuffer.RingBuffer[int64]

func main() {
	eventIdBuffer = ringbuffer.New[int64](50)
	eventProcessor := eventprocessor.Start()

	for i := 0; i < 50; i++ {
		eventSlice := createEventSlice()
		addEvents(eventSlice)
		subscribe(eventProcessor, eventSlice[rand.Int()%len(eventSlice)].Id)
		eventProcessor.SubmitEvents(eventSlice)
		subscribe(eventProcessor, eventIdBuffer.Get(rand.Int()%eventIdBuffer.Length()))
	}

	eventProcessor.Stop()
}

func subscribe(processor eventprocessor.EventProcessor, id int64) {
	processor.SubscribeAndThen(id, func(responseEvent *eventprocessor.Event) {
		fmt.Printf("Client received event data: %s\n", responseEvent.Data)
	})
}

func createEvent() *eventprocessor.Event {
	return &eventprocessor.Event{
		Id:   rand.Int63(),
		Data: uuid.New().String() + uuid.New().String() + uuid.New().String() + uuid.New().String() + uuid.New().String(),
	}
}

func createEventSlice() []*eventprocessor.Event {
	size := 5 + (rand.Int() % 5)
	rtrnSlice := make([]*eventprocessor.Event, size)
	for i := 0; i < size; i++ {
		rtrnSlice[i] = createEvent()
	}
	return rtrnSlice
}

func addEvents(events []*eventprocessor.Event) {
	for i := 0; i < len(events); i++ {
		eventIdBuffer.Add(events[i].Id)
	}
}
