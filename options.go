package mongosubscriber

import (
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

type PublisherOptionFn func(*publisherOptions)

func WithPublisherDBHost(host string) PublisherOptionFn {
	return func(o *publisherOptions) {
		o.dbOptions.host = host
	}
}

func WithPublisherDBName(databaseName string) PublisherOptionFn {
	return func(o *publisherOptions) {
		o.dbOptions.name = databaseName
	}
}

func WithPublisherDB(db *mongo.Database) PublisherOptionFn {
	return func(o *publisherOptions) {
		o.dbOptions.db = db
	}
}

func WithPublisherTTL(ttl time.Duration) PublisherOptionFn {
	return func(o *publisherOptions) {
		o.ttl = ttl
	}
}

type SubscriberOptionFn func(*subscriberOptions)

func WithSubscriberDBHost(host string) SubscriberOptionFn {
	return func(o *subscriberOptions) {
		o.dbOptions.host = host
	}
}

func WithSubscriberDBName(databaseName string) SubscriberOptionFn {
	return func(o *subscriberOptions) {
		o.dbOptions.name = databaseName
	}
}

func WithSubscriberDB(db *mongo.Database) SubscriberOptionFn {
	return func(o *subscriberOptions) {
		o.dbOptions.db = db
	}
}

func WithConsumerName(name string) SubscriberOptionFn {
	return func(o *subscriberOptions) {
		o.consumerName = name
	}
}

func WithAlwaysStartFromZero() SubscriberOptionFn {
	return func(o *subscriberOptions) {
		o.alwaysStartFromZero = true
	}
}

type dbOptions struct {
	host string
	name string
	db   *mongo.Database
}

type publisherOptions struct {
	dbOptions

	ttl time.Duration
}

func makePublisherOptions(fns ...PublisherOptionFn) publisherOptions {
	opts := publisherOptions{}
	for _, fn := range fns {
		fn(&opts)
	}
	return opts
}

type subscriberOptions struct {
	dbOptions

	consumerName        string
	alwaysStartFromZero bool
}

func makeSubscriberOptions(fns ...SubscriberOptionFn) subscriberOptions {
	opts := subscriberOptions{}
	for _, fn := range fns {
		fn(&opts)
	}
	return opts
}
