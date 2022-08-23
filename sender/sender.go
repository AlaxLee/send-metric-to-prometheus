package sender

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"net/url"
	"time"
)

const maxBatchNum int = 7 * 24 * 60

type Sender struct {
	BatchNum int
	Client   remote.WriteClient
}

func NewSender(receiverUrl string) (*Sender, error) {
	sUrl, err := url.Parse(receiverUrl)
	if err != nil {
		return nil, err
	}

	conf := &remote.ClientConfig{
		URL:              &config.URL{URL: sUrl},
		Timeout:          model.Duration(time.Second),
		RetryOnRateLimit: false,
	}

	c, err := remote.NewWriteClient("sender", conf)
	if err != nil {
		return nil, err
	}

	s := &Sender{maxBatchNum, c}

	return s, nil
}

func (s *Sender) Send(datas ...Data) error {

	batchDatas := cutDatasToBatch(datas, s.BatchNum)

	for i := 0; i < len(batchDatas); i++ {
		wr := addDatasToWriteRequest(batchDatas[i])
		data, err := proto.Marshal(wr)
		if err != nil {
			return err
		}
		compressed := snappy.Encode(nil, data)
		err = s.Client.Store(context.Background(), compressed)
		if err != nil {
			return err
		}
	}

	return nil
}

func cutDatasToBatch(datas []Data, batchNum int) [][]Data {
	datasNumber := len(datas)
	if datasNumber == 0 {
		return nil
	}

	samplesTotalNumber := 0
	for i := 0; i < datasNumber; i++ {
		datas[i].Sort()
		samplesTotalNumber += datas[i].Len()
	}

	if batchNum <= 0 || samplesTotalNumber <= batchNum {
		return [][]Data{datas}
	}

	batchDatas := make([][]Data, int((samplesTotalNumber-1)/batchNum)+1)

	needSamplesNumber := batchNum
	batchDatasIndex := 0

	for i := 0; i < len(datas); i++ {
		if needSamplesNumber > datas[i].Len() {
			batchDatas[batchDatasIndex] = append(batchDatas[batchDatasIndex], datas[i])
			needSamplesNumber -= datas[i].Len()
			continue
		}

		startIndex := 0
		endIndex := needSamplesNumber
		for endIndex <= datas[i].Len() {
			batchDatas[batchDatasIndex] = append(batchDatas[batchDatasIndex], Data{
				Metric:  datas[i].Metric,
				Lables:  datas[i].Lables,
				Samples: datas[i].Samples[startIndex:endIndex],
			})
			batchDatasIndex++
			startIndex = endIndex
			endIndex += batchNum
		}
		needSamplesNumber = batchNum

		if startIndex < datas[i].Len() {
			batchDatas[batchDatasIndex] = append(batchDatas[batchDatasIndex], Data{
				Metric:  datas[i].Metric,
				Lables:  datas[i].Lables,
				Samples: datas[i].Samples[startIndex:],
			})
			needSamplesNumber = batchNum - batchDatas[batchDatasIndex][0].Len()
		}
	}

	return batchDatas
}

func addDatasToWriteRequest(datas []Data) *prompb.WriteRequest {
	datasNumber := len(datas)
	wr := &prompb.WriteRequest{
		Timeseries: make([]prompb.TimeSeries, datasNumber),
		Metadata:   make([]prompb.MetricMetadata, datasNumber),
	}
	for i := 0; i < datasNumber; i++ {
		datas[i].AddMetricNameToLable()
		wr.Metadata[i] = datas[i].Metric
		wr.Timeseries[i] = prompb.TimeSeries{Labels: datas[i].Lables, Samples: datas[i].Samples}
	}
	return wr
}
