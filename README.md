# send-metric-to-prometheus
send metric to prometheus

1. prometheus must enable remote write receiver(https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations)
```
The built-in remote write receiver can be enabled by setting the --web.enable-remote-write-receiver command line flag. When enabled, the remote write receiver endpoint is /api/v1/write.
```
for example
```
./prometheus --web.enable-remote-write-receiver
```
2. send date to prometheus
```go
package main

import (
	"github.com/AlaxLee/send-metric-to-prometheus/sender"
	"github.com/prometheus/prometheus/prompb"
	"time"
)

func main() {
	url := "http://127.0.0.1:9090/api/v1/write"
	s, err := sender.NewSender(url)
	if err != nil {
		panic(err)
	}

	d := sender.Data{
		Metric: prompb.MetricMetadata{Type: prompb.MetricMetadata_GAUGE, MetricFamilyName: "lalilulelo"},
		Lables: []prompb.Label{
			{Name: "kaka", Value: "keke"},
		},
		Samples: []prompb.Sample{
			{Value: 7, Timestamp: time.Now().UnixMilli()},
		},
	}
	err = s.Send(d)
	if err != nil {
		panic(err)
	}
}
```