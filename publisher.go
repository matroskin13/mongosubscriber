package mongosubscriber

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type PublishSettings struct {
	TTL      time.Duration
	Collapse bool
}

type Publisher struct {
	opts                 publisherOptions
	topicPublishSettings map[string]PublishSettings
	topicsInit           *sync.Map
	db                   *mongo.Database
}

func NewPublisher(optFns ...PublisherOptionFn) (*Publisher, error) {
	opts := makePublisherOptions(optFns...)

	pub := Publisher{
		opts:                 opts,
		topicPublishSettings: map[string]PublishSettings{},
		topicsInit:           &sync.Map{},
	}

	if opts.db == nil {
		client, err := getClient(opts.dbOptions.dbHost)
		if err != nil {
			return nil, err
		}

		pub.db = client.Database(opts.dbOptions.dbName)
	} else {
		pub.db = opts.db
	}

	return &pub, nil
}

func (p *Publisher) Close() error {
	return nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		item := Message{
			Partition: 0,
			Topic:     topic,
			Offset:    time.Now().UnixNano(),
			Payload:   msg.Payload,
			UUID:      msg.UUID,
			Metadata:  msg.Metadata,
		}

		settings := p.topicPublishSettings[topic]

		msgPublishProps, _ := msg.Context().Value(PublishProp("_props")).(publishProps)

		var ttl time.Duration

		if msgPublishProps.ttl != 0 {
			ttl = msgPublishProps.ttl
		} else if settings.TTL != 0 {
			ttl = settings.TTL
		} else if p.opts.ttl != 0 {
			ttl = p.opts.ttl
		}

		if ttl != 0 {
			item.ExpiredAt = time.Now().Add(ttl)
		}

		if _, ok := p.topicsInit.Load(topic); !ok {
			indexes := []mongo.IndexModel{
				{
					Keys:    bson.M{"expired_at": 1},
					Options: options.Index().SetBackground(true).SetExpireAfterSeconds(60),
				},
				{
					Keys:    bson.M{"offset": 1},
					Options: options.Index().SetBackground(true),
				},
			}

			if _, err := p.db.Collection("events_"+topic).Indexes().CreateMany(context.Background(), indexes); err != nil {
				return fmt.Errorf("cannot create indexes for members: %w", err)
			}

			p.topicsInit.Store(topic, struct{}{})
		}

		collapseProp := msg.Context().Value(PublishProp("_collapse"))

		if msgPublishProps.withCollapse != nil {
			collapseProp = msgPublishProps.withCollapse
		}

		if !settings.Collapse && collapseProp == nil {
			if _, err := p.db.Collection("events_"+topic).InsertOne(context.Background(), item); err != nil {
				return err
			}
		} else {
			filter := bson.M{"uuid": item.UUID}

			if _, err := p.db.Collection("events_"+topic).ReplaceOne(context.Background(), filter, item, options.Replace().SetUpsert(true)); err != nil {
				return nil
			}
		}
	}

	return nil
}

type PublishProp string

func PublishWithCollapse(ctx context.Context) context.Context {
	return context.WithValue(ctx, PublishProp("_collapse"), true)
}

func PublishWithTTL(ctx context.Context, duration time.Duration) context.Context {
	return context.WithValue(ctx, PublishProp("_ttl"), duration)
}

type publishProps struct {
	ttl          time.Duration
	withCollapse *bool
}

type PropOption func(*publishProps)

func WithTTL(dur time.Duration) PropOption {
	return func(p *publishProps) {
		p.ttl = dur
	}
}

func WithCollapse() PropOption {
	return func(p *publishProps) {
		value := true
		p.withCollapse = &value
	}
}

func WithProps(msg *message.Message, props ...PropOption) {
	p := publishProps{}

	for _, propSetter := range props {
		propSetter(&p)
	}

	msg.SetContext(
		context.WithValue(msg.Context(), PublishProp("_props"), p),
	)
}
