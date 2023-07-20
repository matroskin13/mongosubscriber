package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/matroskin13/mongosubscriber/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	sub, err := mongosubscriber.NewSubscriber(
		mongosubscriber.WithConsumerName("example"),
		mongosubscriber.WithSubscriberDBHost("mongodb://localhost:27017"),
		mongosubscriber.WithSubscriberDBName("events"),
	)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		http.ListenAndServe(":3009", promhttp.Handler())
	}()

	defer sub.Close()

	pub, err := mongosubscriber.NewPublisher(
		mongosubscriber.WithPublisherDBHost("mongodb://localhost:27017"),
		mongosubscriber.WithPublisherDBName("events"),
		mongosubscriber.WithPublisherTTL(5*time.Minute),
	)
	if err != nil {
		log.Fatal(err)
	}

	defer pub.Close()

	go func() {
		for {
			<-time.After(time.Second * 2)
			if err := pub.Publish("example_topic", message.NewMessage(watermill.NewUUID(), []byte("Hello world!"))); err != nil {
				log.Fatal(err)
			}
		}
	}()

	ch, err := sub.Subscribe(context.Background(), "example_topic")
	if err != nil {
		log.Fatal(err)
	}

	for msg := range ch {
		fmt.Println("receive message: ", string(msg.Payload))
		msg.Ack()
	}
}
