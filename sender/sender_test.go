package sender

import (
	"errors"
	"fmt"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"
)

var baseTimestamp = time.Now().Unix() - 60*60

func ts(i int64) int64 {
	return baseTimestamp + i*60
}

func getTestDatas() []Data {
	testDatas := []Data{
		{
			Metric: prompb.MetricMetadata{
				Type:             prompb.MetricMetadata_COUNTER,
				MetricFamilyName: "test1",
			},
			Lables: []prompb.Label{
				{Name: "t1n1", Value: "t1v1"}, {Name: "t1n2", Value: "t1v2"}, {Name: "t1n3", Value: "t1v3"},
			},
			Samples: []prompb.Sample{
				{Value: 0.1, Timestamp: ts(2)}, {Value: 0.2, Timestamp: ts(1)},
			},
		},
		{
			Metric: prompb.MetricMetadata{
				Type:             prompb.MetricMetadata_GAUGE,
				MetricFamilyName: "test2",
			},
			Lables: []prompb.Label{
				{Name: "t2n1", Value: "t2v1"},
			},
			Samples: []prompb.Sample{
				{Value: 0.1, Timestamp: ts(2)}, {Value: 0.3, Timestamp: ts(1)}, {Value: 0.2, Timestamp: ts(3)},
			},
		},
		{
			Metric: prompb.MetricMetadata{
				Type:             prompb.MetricMetadata_HISTOGRAM,
				MetricFamilyName: "test3",
			},
			Lables: []prompb.Label{
				{Name: "t3n1", Value: "t3v1"}, {Name: "t3n2", Value: "t3v2"},
			},
			Samples: []prompb.Sample{
				{Value: 0.1, Timestamp: ts(2)}, {Value: 0.2, Timestamp: ts(4)}, {Value: 0.4, Timestamp: ts(3)}, {Value: 0.3, Timestamp: ts(1)},
			},
		},
	}
	return testDatas
}

func Test_cutDatasToBatch(t *testing.T) {
	testDatas := getTestDatas()

	type args struct {
		datas    []Data
		batchNum int
	}
	tests := []struct {
		name string
		args args
		want [][]Data
	}{
		{
			"不限制批次",
			args{testDatas, 0},
			[][]Data{
				{
					{
						testDatas[0].Metric,
						testDatas[0].Lables,
						[]prompb.Sample{
							testDatas[0].Samples[1],
							testDatas[0].Samples[0]},
					},
					{
						testDatas[1].Metric,
						testDatas[1].Lables,
						[]prompb.Sample{
							testDatas[1].Samples[1],
							testDatas[1].Samples[0],
							testDatas[1].Samples[2]},
					},
					{
						testDatas[2].Metric,
						testDatas[2].Lables,
						[]prompb.Sample{
							testDatas[2].Samples[3],
							testDatas[2].Samples[0],
							testDatas[2].Samples[2],
							testDatas[2].Samples[1]},
					},
				},
			},
		},
		{
			"数据量小于批次限制",
			args{testDatas, 10},
			[][]Data{
				{
					{
						testDatas[0].Metric,
						testDatas[0].Lables,
						[]prompb.Sample{
							testDatas[0].Samples[1],
							testDatas[0].Samples[0]},
					},
					{
						testDatas[1].Metric,
						testDatas[1].Lables,
						[]prompb.Sample{
							testDatas[1].Samples[1],
							testDatas[1].Samples[0],
							testDatas[1].Samples[2]},
					},
					{
						testDatas[2].Metric,
						testDatas[2].Lables,
						[]prompb.Sample{
							testDatas[2].Samples[3],
							testDatas[2].Samples[0],
							testDatas[2].Samples[2],
							testDatas[2].Samples[1]},
					},
				},
			},
		},
		{
			"数据量不小于批次限制，且能被批次整除",
			args{testDatas, 3},
			[][]Data{
				{
					{
						testDatas[0].Metric,
						testDatas[0].Lables,
						[]prompb.Sample{
							testDatas[0].Samples[1],
							testDatas[0].Samples[0]},
					},
					{
						testDatas[1].Metric,
						testDatas[1].Lables,
						[]prompb.Sample{
							testDatas[1].Samples[1]},
					},
				},
				{
					{
						testDatas[1].Metric,
						testDatas[1].Lables,
						[]prompb.Sample{
							testDatas[1].Samples[0],
							testDatas[1].Samples[2]},
					},
					{
						testDatas[2].Metric,
						testDatas[2].Lables,
						[]prompb.Sample{
							testDatas[2].Samples[3]},
					},
				},
				{
					{
						testDatas[2].Metric,
						testDatas[2].Lables,
						[]prompb.Sample{
							testDatas[2].Samples[0],
							testDatas[2].Samples[2],
							testDatas[2].Samples[1]},
					},
				},
			},
		},
		{
			"数据量不小于批次限制，且不能被批次整除",
			args{testDatas, 4},
			[][]Data{
				{
					{
						testDatas[0].Metric,
						testDatas[0].Lables,
						[]prompb.Sample{
							testDatas[0].Samples[1],
							testDatas[0].Samples[0]},
					},
					{
						testDatas[1].Metric,
						testDatas[1].Lables,
						[]prompb.Sample{
							testDatas[1].Samples[1],
							testDatas[1].Samples[0]},
					},
				},
				{
					{
						testDatas[1].Metric,
						testDatas[1].Lables,
						[]prompb.Sample{
							testDatas[1].Samples[2]},
					},
					{
						testDatas[2].Metric,
						testDatas[2].Lables,
						[]prompb.Sample{
							testDatas[2].Samples[3],
							testDatas[2].Samples[0],
							testDatas[2].Samples[2]},
					},
				},
				{
					{
						testDatas[2].Metric,
						testDatas[2].Lables,
						[]prompb.Sample{
							testDatas[2].Samples[1]},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cutDatasToBatch(tt.args.datas, tt.args.batchNum); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("cutDatasToBatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSender_Send(t *testing.T) {
	serverUrl := "http://127.0.0.1:12345/receive"
	u, err := url.Parse(serverUrl)
	if err != nil {
		t.Errorf("parse url %s failed: %v", serverUrl, err)
	}
	server := &http.Server{Addr: u.Host}
	serverErr := make(chan error)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

LOOP:
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second)
		select {
		case err := <-serverErr:
			t.Errorf("Listen %s failed: %v", u.Host, err)
		default:
			_, err := net.Dial("tcp", u.Host)
			if err == nil {
				break LOOP
			} else if i == 2 {
				t.Errorf("connect %s failed: %v", u.Host, err)
			}
		}
	}

	testDatas := getTestDatas()

	type args struct {
		datas []Data
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		want    []Data
	}{
		{
			"test",
			args{testDatas},
			false,
			[]Data{
				{
					testDatas[0].Metric,
					testDatas[0].Lables,
					[]prompb.Sample{
						testDatas[0].Samples[1],
						testDatas[0].Samples[0]},
				},
				{
					testDatas[1].Metric,
					testDatas[1].Lables,
					[]prompb.Sample{
						testDatas[1].Samples[1],
						testDatas[1].Samples[0],
						testDatas[1].Samples[2]},
				},
				{
					testDatas[2].Metric,
					testDatas[2].Lables,
					[]prompb.Sample{
						testDatas[2].Samples[3],
						testDatas[2].Samples[0],
						testDatas[2].Samples[2],
						testDatas[2].Samples[1]},
				},
			},
		},
	}

	for i, tt := range tests {
		currentPath := u.Path + strconv.Itoa(i)
		http.HandleFunc(currentPath, func(w http.ResponseWriter, r *http.Request) {
			req, err := remote.DecodeWriteRequest(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if len(req.Metadata) != len(req.Timeseries) {
				http.Error(w, errors.New("metadata number not equal to timeseries number").Error(), http.StatusInternalServerError)
				return
			}

			got := make([]Data, len(req.Metadata))
			for i := 0; i < len(req.Metadata); i++ {
				got[i].Metric = req.Metadata[i]
				got[i].Lables = req.Timeseries[i].Labels
				got[i].Samples = req.Timeseries[i].Samples
			}
			if !reflect.DeepEqual(got, tt.want) {
				http.Error(w,
					errors.New(fmt.Sprintf("receive data %v not equal want data %v", got, tt.want)).Error(),
					http.StatusInternalServerError)
				return
			}
		})

		currentUrl := u.String() + strconv.Itoa(i)
		s, err := NewSender(currentUrl)
		if err != nil {
			t.Errorf("create sender failed %v", err)
		}
		if err := s.Send(tt.args.datas...); (err != nil) != tt.wantErr {
			t.Errorf("Send() error = %v, wantErr %v", err, tt.wantErr)
		}
	}

}
