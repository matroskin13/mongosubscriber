package mongosubscriber

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	freshnessMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "go_mongosubscriber_freshness",
		Help: "Can be used for finding problems with lag",
	}, []string{"group", "topic"})
)
