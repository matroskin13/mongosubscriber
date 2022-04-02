package mongosubscriber

import "time"

type Message struct {
	Partition int               `bson:"partition"`
	Topic     string            `bson:"topic"`
	Offset    int64             `bson:"offset"`
	Payload   []byte            `bson:"payload"`
	UUID      string            `bson:"uuid"`
	Metadata  map[string]string `bson:"metadata"`
	CreatedAt time.Time         `bson:"created_at,omitempty"`
	ExpiredAt time.Time         `bson:"expired_at,omitempty"`
}
