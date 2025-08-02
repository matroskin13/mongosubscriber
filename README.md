# MongoDB Subscriber

[![Go Report Card](https://goreportcard.com/badge/github.com/matroskin13/mongosubscriber)](https://goreportcard.com/report/github.com/matroskin13/mongosubscriber)
[![GoDoc](https://godoc.org/github.com/matroskin13/mongosubscriber?status.svg)](https://godoc.org/github.com/matroskin13/mongosubscriber)

A MongoDB-based message streaming library that implements the [Watermill](https://watermill.io/) interface, providing Kafka-like functionality using MongoDB collections. This library allows you to produce and consume messages from MongoDB tables with offset-based message ordering and consumer group management.

## Features

- **Kafka-like message streaming** using MongoDB collections
- **Watermill interface implementation** for seamless integration with Watermill ecosystem
- **Consumer groups** with offset tracking and distributed processing
- **Message ordering** with automatic offset management
- **TTL support** for automatic message expiration
- **Message collision prevention** with upsert capabilities
- **Prometheus metrics** for monitoring consumer lag and freshness
- **Distributed locking** for safe concurrent processing
- **Flexible publishing options** with collapse and TTL controls

## Installation

```bash
go get github.com/matroskin13/mongosubscriber
```

## Quick Start

### Basic Producer and Consumer

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/ThreeDotsLabs/watermill"
    "github.com/ThreeDotsLabs/watermill/message"
    "github.com/matroskin13/mongosubscriber"
)

func main() {
    opts := &mongosubscriber.Options{
        Host:         "mongodb://localhost:27017",
        ConsumerName: "my-consumer",
        Database:     "events",
    }

    // Create publisher
    pub, err := mongosubscriber.NewPublisher(opts)
    if err != nil {
        log.Fatal(err)
    }
    defer pub.Close()

    // Create subscriber
    sub, err := mongosubscriber.NewSubscriber(opts)
    if err != nil {
        log.Fatal(err)
    }
    defer sub.Close()

    // Publish a message
    err = pub.Publish("user-events", message.NewMessage(watermill.NewUUID(), []byte("Hello, World!")))
    if err != nil {
        log.Fatal(err)
    }

    // Subscribe to messages
    ch, err := sub.Subscribe(context.Background(), "user-events")
    if err != nil {
        log.Fatal(err)
    }

    for msg := range ch {
        fmt.Printf("Received: %s\n", string(msg.Payload))
        msg.Ack()
    }
}
```

## How It Works

MongoDB Subscriber creates a Kafka-like streaming system using MongoDB:

- **Events Collections**: Messages are stored in collections named `events_{topic}`
- **Consumer Tracking**: Consumer offsets are tracked in the `events_consumers` collection
- **Offset-based Ordering**: Each message gets a unique offset (Unix timestamp in nanoseconds)
- **Consumer Groups**: Multiple consumers can process messages with automatic load balancing

## Advanced Usage

### Publishing with TTL

```go
// Set TTL for automatic message expiration
mongosubscriber.WithProps(msg, mongosubscriber.WithTTL(time.Hour*24))
pub.Publish("topic", msg)

// Or use context
ctx := mongosubscriber.PublishWithTTL(context.Background(), time.Hour)
msg.SetContext(ctx)
pub.Publish("topic", msg)
```

### Publishing with Collapse

Prevent duplicate messages by using the same UUID:

```go
// Messages with the same UUID will be upserted
mongosubscriber.WithProps(msg, mongosubscriber.WithCollapse())
pub.Publish("topic", msg)

// Or use context
ctx := mongosubscriber.PublishWithCollapse(context.Background())
msg.SetContext(ctx)
pub.Publish("topic", msg)
```

### Using Existing MongoDB Database

```go
// Use an existing MongoDB database connection
db := client.Database("mydb")
pub := mongosubscriber.NewPublisherWithDatabase(db)
sub := mongosubscriber.NewSubscriberWithDatabase(db, opts)
```

### Configuration Options

```go
opts := &mongosubscriber.Options{
    Host:                "mongodb://localhost:27017",
    Database:            "events",
    ConsumerName:        "my-service",
    AlwaysStartFromZero: false, // Set to true to always start from beginning
}
```

## Monitoring

The library exposes Prometheus metrics for monitoring:

```go
import (
    "net/http"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

// Expose metrics endpoint
go func() {
    http.ListenAndServe(":3009", promhttp.Handler())
}()
```

Available metrics:
- `mongosubscriber_freshness_milliseconds`: Consumer lag in milliseconds per topic/consumer

## Watermill Integration

This library fully implements the Watermill publisher and subscriber interfaces, making it compatible with the entire Watermill ecosystem:

```go
import (
    "github.com/ThreeDotsLabs/watermill/message"
    "github.com/ThreeDotsLabs/watermill/message/router"
)

// Use with Watermill router
r, err := message.NewRouter(message.RouterConfig{}, logger)

// Add handlers using the MongoDB subscriber
r.AddHandler(
    "user_handler",
    "user-events",
    sub,
    "processed-users",
    pub,
    func(msg *message.Message) ([]*message.Message, error) {
        // Process message
        return []*message.Message{msg}, nil
    },
)
```

## MongoDB Schema

The library automatically creates the following collections:

### Events Collection (`events_{topic}`)
```javascript
{
  _id: ObjectId,
  partition: Number,    // Always 0 (single partition)
  topic: String,        // Topic name
  offset: NumberLong,   // Unix timestamp in nanoseconds
  payload: BinData,     // Message payload
  uuid: String,         // Message UUID
  metadata: Object,     // Key-value metadata
  expired_at: Date      // TTL expiration (optional)
}
```

### Consumer Collection (`events_consumers`)
```javascript
{
  _id: ObjectId,
  topic: String,        // Topic name
  consumer: String,     // Consumer group name
  partition: Number,    // Always 0
  offset: NumberLong,   // Last processed offset
  take: NumberLong,     // Lock timestamp (optional)
  take_id: NumberLong   // Lock ID (optional)
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.

## Related Projects

- [Watermill](https://watermill.io/) - Go library for building event-driven applications
- [MongoDB Go Driver](https://github.com/mongodb/mongo-go-driver) - The official MongoDB driver for Go