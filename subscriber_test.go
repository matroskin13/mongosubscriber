package mongosubscriber

import (
	"log"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func newTestOptions() *Options {
	return &Options{
		ConsumerName: "example",
		Database:     "example",
		Host:         "mongodb://localhost:27017",
	}
}

func TestSubscriber(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	opts := newTestOptions()

	client, err := getClient(opts)
	if err != nil {
		log.Fatal(err)
	}

	tests.TestPubSub(
		t,
		features,
		func(t *testing.T) (message.Publisher, message.Subscriber) {
			sub := NewSubscriberWithDatabase(client.Database("services"), &Options{ConsumerName: "test"})
			pub := NewPublisherWithDatabase(client.Database("services"))

			return pub, sub
		},
		func(t *testing.T, group string) (message.Publisher, message.Subscriber) {
			sub := NewSubscriberWithDatabase(client.Database("services"), &Options{ConsumerName: "test_" + group})
			pub := NewPublisherWithDatabase(client.Database("services"))

			return pub, sub
		},
	)
}
