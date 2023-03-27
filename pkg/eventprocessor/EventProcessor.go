package eventprocessor

import (
	"container/list"
	"container/ring"
	"fmt"
)

const (
	MAX_EVENTS = 120
)

type Event struct {
	Id   int64
	Data string
}

type Sub struct {
	Id                  int64
	NotificationChannel chan<- *Event
}

type EventProcessor interface {
	SubmitEvents([]*Event)
	Subscribe(id int64) *Event
	SubscribeAndThen(id int64, exec func(event *Event))
	Stop()
}

type eventProcessor struct {
	eventChannel        chan<- []*Event
	subscriptionChannel chan<- Sub
	shutdownChannel     chan<- bool
}

type EventProcessorActor struct {
	events              *ring.Ring
	subscriptions       *list.List
	eventChannel        <-chan []*Event
	subscriptionChannel <-chan Sub
	shutdownChannel     chan bool
	lastLink            *ring.Ring
	eventCount          int
}

func (ad *EventProcessorActor) addEvents(newEvents []*Event) {
	newLink := ring.New(1)
	newLink.Value = newEvents

	if ad.events != nil {
		ad.events.Link(newLink)
	} else {
		ad.events = newLink
	}

	ad.lastLink = newLink
	ad.eventCount = ad.eventCount + len(newEvents)

	ad.trimEvents()
}

func (ad *EventProcessorActor) trimEvents() {
	if ad.eventCount > MAX_EVENTS {
		unlinkEventCount := len(ad.lastLink.Next().Value.([]*Event))
		ad.lastLink.Unlink(1)
		ad.eventCount = ad.eventCount - unlinkEventCount
		ad.trimEvents()
	}
}

func (ad *EventProcessorActor) addSubscription(s *Sub) {
	ad.subscriptions.PushBack(s)
}

func (ad *EventProcessorActor) removeSubscription(removeId int64) {
	for element := ad.subscriptions.Front(); element != nil; element = element.Next() {
		if element.Value.(Sub).Id == removeId {
			prevElement := element.Prev()
			ad.subscriptions.Remove(element)
			element = prevElement
		}
	}
}

/**
 * For each event in the batch, look for a matching subscription, and for each subscription
 * found, send the event down its notificationChannel and remove the subscription from the
 * subscriptions list.
 */
func (ad *EventProcessorActor) triggerSubscriptions(events []*Event) {
	for element := ad.subscriptions.Front(); element != nil; element = element.Next() {
		for i := 0; i < len(events); i++ {
			if element.Value.(*Sub).Id == events[i].Id {
				element.Value.(*Sub).NotificationChannel <- events[i]
				prevElement := element.Prev()
				ad.subscriptions.Remove(element)
				element = prevElement
				break
			}
		}
		if element == nil {
			break
		}
	}
}

func (ad *EventProcessorActor) matchEvent(s Sub) bool {
	if ad.events == nil {
		return false
	}

	traversedOne := false
	element := ad.events
	for element != ad.events || !traversedOne {
		events := element.Value.([]*Event)
		for i := 0; i < len(events); i++ {
			if events[i].Id == s.Id {
				s.NotificationChannel <- events[i]
				return true
			}
		}
		element = element.Next()
		traversedOne = true
	}
	return false
}

func (ep *eventProcessor) SubmitEvents(events []*Event) {
	ep.eventChannel <- events
}

func (ep *eventProcessor) Subscribe(id int64) *Event {
	notificationChannel := make(chan *Event)
	subscription := Sub{
		Id:                  id,
		NotificationChannel: notificationChannel,
	}
	ep.subscriptionChannel <- subscription
	returnEvent := <-notificationChannel
	return returnEvent
}

func (ep *eventProcessor) SubscribeAndThen(id int64, exec func(*Event)) {
	notificationChannel := make(chan *Event)
	subscription := Sub{
		Id:                  id,
		NotificationChannel: notificationChannel,
	}
	ep.subscriptionChannel <- subscription
	go func() {
		returnEvent := <-notificationChannel
		exec(returnEvent)
	}()
}

func (ep *eventProcessor) Stop() {
	ep.shutdownChannel <- true
}

/**
 * Create and start an EventProcessor and return an eventProcessor that implements
 * the EventProcessor interface that provides a way of interacting with the EventProcessor
 */
func Start() EventProcessor {
	eventC := make(chan []*Event)
	subC := make(chan Sub)
	shutdownC := make(chan bool)

	ad := EventProcessorActor{
		events:              nil,
		subscriptions:       list.New(),
		eventChannel:        eventC,
		subscriptionChannel: subC,
		shutdownChannel:     shutdownC,
		lastLink:            nil,
		eventCount:          0,
	}

	go func() {
		for {
			select {
			case e := (<-ad.eventChannel):
				fmt.Println("Actor received event data")
				ad.addEvents(e)
				ad.triggerSubscriptions(e)
			case s := (<-ad.subscriptionChannel):
				fmt.Println("Actor received subscription")
				isMatch := ad.matchEvent(s)
				if !isMatch {
					ad.addSubscription(&s)
				}
			case _ = (<-ad.shutdownChannel):
				fmt.Print("Actor shutting down\n")
				break
			}
		}
	}()

	return &eventProcessor{
		eventChannel:        eventC,
		subscriptionChannel: subC,
		shutdownChannel:     shutdownC,
	}
}
