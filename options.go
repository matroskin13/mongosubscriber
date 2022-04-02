package mongosubscriber

type Options struct {
	ConsumerName        string
	Database            string
	Host                string
	TTL                 int
	AlwaysStartFromZero bool
}
