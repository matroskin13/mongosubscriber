package mongosubscriber

import (
	"log"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
)

func newTestOptions() *subscriberOptions {
	return &subscriberOptions{
		consumerName: "example",
		dbOptions: dbOptions{
			name: "example",
			host: "mongodb://localhost:27017",
		},
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

	client, err := getClient(opts.dbOptions.host)
	if err != nil {
		log.Fatal(err)
	}

	tests.TestPubSub(
		t,
		features,
		func(t *testing.T) (message.Publisher, message.Subscriber) {
			sub, err := NewSubscriber(
				WithSubscriberDB(client.Database("services")),
				WithConsumerName("test"),
			)
			if err != nil {
				log.Fatal(err)
			}

			pub, err := NewPublisher(
				WithPublisherDB(client.Database("services")),
			)
			if err != nil {
				log.Fatal(err)
			}

			return pub, sub
		},
		func(t *testing.T, group string) (message.Publisher, message.Subscriber) {
			sub, err := NewSubscriber(
				WithSubscriberDB(client.Database("services")),
				WithConsumerName("test_"+group),
			)
			if err != nil {
				log.Fatal(err)
			}

			pub, err := NewPublisher(
				WithPublisherDB(client.Database("services")),
			)
			if err != nil {
				log.Fatal(err)
			}

			return pub, sub
		},
	)
}
