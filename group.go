package mongosubscriber

type Group struct {
	Consumer string `bson:"consumer"`
	Topic    string `bson:"topic"`
	Offset   int64  `bson:"offset"`
	TakeId   int64  `bson:"take_id,omitempty"`
}

type GroupWithMessages struct {
	Group
	Messages []*Message `bson:"events"`
}

type GroupWithTopic struct {
	Group
	Topics []TopicInfo
}

type TopicInfo struct {
	Topic  string `bson:"topic"`
	Offset int64  `bson:"offset"`
}
