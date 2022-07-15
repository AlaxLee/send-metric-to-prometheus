package sender

import (
	"github.com/prometheus/prometheus/prompb"
	"sort"
)

type Data struct {
	Metric  prompb.MetricMetadata
	Lables  []prompb.Label
	Samples []prompb.Sample
}

func (d *Data) Len() int {
	return len(d.Samples)
}
func (d *Data) Less(i, j int) bool {
	if d.Samples[i].Timestamp < d.Samples[j].Timestamp {
		return true
	} else {
		return false
	}
}
func (d *Data) Swap(i, j int) {
	d.Samples[i].Timestamp, d.Samples[j].Timestamp = d.Samples[j].Timestamp, d.Samples[i].Timestamp
	d.Samples[i].Value, d.Samples[j].Value = d.Samples[j].Value, d.Samples[i].Value
}
func (d *Data) Sort() {
	sort.Sort(d)
}
