package mongosubscriber

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Subscriber struct {
	db       *mongo.Database
	consumer string
	cancel   context.CancelFunc
	ctx      context.Context
	ch       chan *message.Message
	isClosed bool
	wg       *sync.WaitGroup
	client   *mongo.Client
	opts     subscriberOptions
}

func NewSubscriber(optFns ...SubscriberOptionFn) (*Subscriber, error) {
	opts := makeSubscriberOptions(optFns...)

	ctx, cancel := context.WithCancel(context.Background())

	sub := &Subscriber{
		consumer: opts.consumerName,
		ctx:      ctx,
		cancel:   cancel,
		wg:       &sync.WaitGroup{},
		opts:     opts,
	}

	if opts.db == nil {
		client, err := getClient(opts.dbOptions.host)
		if err != nil {
			return nil, err
		}

		sub.client = client
		sub.db = client.Database(opts.dbOptions.name)
	} else {
		sub.db = opts.db
	}

	return sub, nil
}

func (s *Subscriber) Close() error {
	if s.isClosed {
		return nil
	}

	s.isClosed = true
	s.cancel()

	if s.ch != nil {
		close(s.ch)
	}

	s.wg.Wait()

	return nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	ch := make(chan *message.Message)
	// s.ch = ch
	partition := 0

	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{"topic", 1}, {"partition", 1}, {"consumer", 1}},
			Options: options.Index().SetBackground(false).SetUnique(true),
		},
	}

	if _, err := s.db.Collection("events_consumers").Indexes().CreateMany(ctx, indexes); err != nil {
		return nil, fmt.Errorf("cannot create indexes: %w", err)
	}

	if err := s.db.Collection("events_consumers").FindOneAndUpdate(ctx, bson.M{
		"topic":     topic,
		"consumer":  s.consumer,
		"partition": partition,
	}, bson.M{
		"$setOnInsert": bson.M{
			"offset": 0,
		},
	}, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Err(); err != nil {
		return nil, fmt.Errorf("cannot upsert consumer: %w", err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second * 10):
				s.collectMetrics(ctx)
			}
		}
	}()

	s.wg.Add(1)

	go func() {
		var lastOffset int64
		after := time.Second

		defer s.wg.Done()

		for {
			select {
			case <-s.ctx.Done():
				close(ch)
				return
			case <-ctx.Done():
				close(ch)
				return
			case <-time.After(after):
				currentOffset, err := s.getMaxOffset(ctx, topic, lastOffset)

				if err == nil && lastOffset == currentOffset {
					break
				}

				handler := func(ctx context.Context, offset int64) error {
					msgs, err := s.GetMessagesByOffset(ctx, topic, partition, 80, offset)
					if err != nil {
						fmt.Println("cannot get messages", err)
						return err
					}

					/*if len(msgs) == 0 {
						after = time.Second
					} else {
						after = 0
					}*/

					for _, msg := range msgs {
						select {
						case <-s.ctx.Done():
							return nil
						case <-ctx.Done():
							return nil
						default:
						}

						msgContext, cancel := context.WithCancel(ctx)
						watermillMsg := message.NewMessage(msg.UUID, msg.Payload)

						watermillMsg.Metadata = msg.Metadata

						watermillMsg.SetContext(msgContext)

						ch <- watermillMsg

						select {
						case <-s.ctx.Done():
							cancel()
						case <-watermillMsg.Acked():
							if err := s.incOffset(watermillMsg.Context(), topic, partition, msg.Offset); err != nil {
								cancel()
								return err
							}

							cancel()
						case <-watermillMsg.Nacked():
							cancel()
							return nil
						}

						lastOffset = msg.Offset
					}

					if len(msgs) == 0 {
						lastOffset = currentOffset
					}

					return nil
				}

				if !s.opts.alwaysStartFromZero {
					if err := s.lock(ctx, topic, partition, handler); err != nil {
						fmt.Println(err)
						break
					}
				} else {
					if err := handler(ctx, lastOffset); err != nil {
						fmt.Println(err)
						break
					}
				}
			}
		}
	}()

	return ch, nil
}

func (s *Subscriber) collectMetrics(ctx context.Context) error {
	consumers, err := s.getConsumers(ctx)
	if err != nil {
		return err
	}

	for _, consumer := range consumers {
		for _, topic := range consumer.Topics {
			freshness := time.Duration(topic.Offset)

			freshnessMetric.WithLabelValues(consumer.Consumer, topic.Topic).Set(float64(freshness.Milliseconds()))
		}
	}

	return nil
}

func (s *Subscriber) getConsumers(ctx context.Context) ([]*GroupWithTopic, error) {
	var groups []*GroupWithTopic

	cur, err := s.db.Collection("events_consumers").Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	defer cur.Close(ctx)

	uniqueGroup := map[string]*GroupWithTopic{}

	for cur.Next(ctx) {
		var group Group

		if err := cur.Decode(&group); err != nil {
			return nil, err
		}

		groupWithTopic, ok := uniqueGroup[group.Consumer]
		if !ok {
			groupWithTopic = &GroupWithTopic{Group: group}
			uniqueGroup[group.Consumer] = groupWithTopic
		}

		maxOffset, err := s.getMaxOffset(ctx, group.Topic, 0)
		if err != nil {
			return nil, err
		}

		groupWithTopic.Topics = append(groupWithTopic.Topics, TopicInfo{
			Offset: int64(math.Max(1, float64(maxOffset-group.Offset))),
			Topic:  group.Topic,
		})
	}

	if cur.Err() != nil {
		return nil, err
	}

	for _, group := range uniqueGroup {
		groups = append(groups, group)
	}

	return groups, nil
}

func (s *Subscriber) lock(ctx context.Context, topic string, partition int, callback func(context.Context, int64) error) error {
	id := time.Now().UnixNano()
	filter := bson.M{
		"topic":     topic,
		"partition": partition,
		"consumer":  s.consumer,
		"$or": []bson.M{
			{"take": nil},
			{"take": ""},
			{"take": bson.M{
				"$exists": false,
			}},
			{"take": bson.M{
				"$lte": time.Now().Add(time.Second * -10).UnixNano(),
			}},
		},
	}
	update := bson.M{"$set": bson.M{"take": time.Now().UnixNano(), "take_id": id}}

	var consumer *Group

	err := s.db.Collection("events_consumers").FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&consumer)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil
		}

		return err
	}

	if consumer.TakeId != id {
		return nil
	}

	defer func() {
		filter := bson.M{
			"topic":     topic,
			"partition": partition,
			"consumer":  s.consumer,
			"take_id":   id,
		}
		update := bson.M{"$set": bson.M{"take": nil, "take_id": nil}}

		if _, err := s.db.Collection("events_consumers").UpdateOne(ctx, filter, update); err != nil {
			return
		}
	}()

	if err := callback(ctx, consumer.Offset); err != nil {
		return err
	}

	return nil
}

func (s *Subscriber) incOffset(ctx context.Context, topic string, partition int, offset int64) error {
	filter := bson.M{"topic": topic, "partition": partition, "consumer": s.consumer}
	update := bson.M{"$set": bson.M{"offset": offset}}

	if _, err := s.db.Collection("events_consumers").UpdateOne(ctx, filter, update); err != nil {
		return err
	}

	return nil
}

func (s *Subscriber) GetMessagesByOffset(ctx context.Context, topic string, partition int, limit int, offset int64) ([]*Message, error) {
	filter := bson.M{
		"offset": bson.M{
			"$gt": offset,
		},
		"partition": partition,
	}

	opts := options.Find().SetLimit(int64(limit)).SetSort(bson.M{"offset": 1})

	cur, err := s.db.Collection("events_"+topic).Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}

	defer cur.Close(ctx)

	var messages []*Message

	for cur.Next(ctx) {
		var msg Message

		if err := cur.Decode(&msg); err != nil {
			return nil, err
		}

		messages = append(messages, &msg)
	}

	return messages, nil
}

func (s *Subscriber) getMaxOffset(ctx context.Context, topic string, offset int64) (int64, error) {
	collection := s.db.Collection("events_" + topic)
	sort := bson.M{"offset": -1}
	filter := bson.M{"offset": bson.M{"$gt": offset}}
	filter = bson.M{}

	// var res Message
	var res bson.M

	opts := options.FindOne().SetSort(sort).SetProjection(bson.M{"_id": 0, "offset": 1})

	if err := collection.FindOne(ctx, filter, opts).Decode(&res); err != nil {
		if err == mongo.ErrNoDocuments {
			return offset, nil
		}

		return 0, err
	}

	return res["offset"].(int64), nil
}

func (s *Subscriber) GetMessages(ctx context.Context, topic string, partition int, limit int) ([]*Message, error) {
	collection := s.db.Collection("events_consumers")

	pipe := bson.D{
		{"$lookup", bson.M{
			"from": "events_" + topic,
			"as":   "events",
			"let": bson.M{
				"offset": "$offset",
			},
			"pipeline": mongo.Pipeline{
				bson.D{{"$match", bson.M{
					"$expr": bson.M{
						"$gt": []string{"$offset", "$$offset"},
					},
				}}},
				bson.D{{"$sort", bson.M{"$$offset": 1}}},
				bson.D{{"$limit", limit}},
			},
		}},
	}

	cur, err := collection.Aggregate(ctx, mongo.Pipeline{pipe})
	if err != nil {
		return nil, err
	}

	defer cur.Close(ctx)

	var messages []GroupWithMessages

	for cur.Next(ctx) {
		var msg GroupWithMessages

		if err := cur.Decode(&msg); err != nil {
			return nil, err
		}

		messages = append(messages, msg)
	}

	if len(messages) > 0 {
		return messages[0].Messages, nil
	}

	return nil, nil
}
