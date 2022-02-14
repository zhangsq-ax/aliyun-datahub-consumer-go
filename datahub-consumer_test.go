package datahub_consumer

import (
	"os"
	"testing"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
	"github.com/stretchr/testify/assert"
)

var consumer *DataHubConsumer

func TestNewDataHubConsumer(t *testing.T) {
	var err error
	var (
		endpoint        = os.Getenv("ENDPOINT")
		accessKeyId     = os.Getenv("ACCESS_KEY_ID")
		accessKeySecret = os.Getenv("ACCESS_KEY_SECRET")
		projectName     = os.Getenv("PROJECT_NAME")
		topicName       = os.Getenv("TOPIC_NAME")
		subId           = os.Getenv("SUBSCRIPTION_ID")
	)
	consumer, err = NewDataHubConsumer(&DataHubConsumerOptions{
		Endpoint:        endpoint,
		AccessKeyId:     accessKeyId,
		AccessKeySecret: accessKeySecret,
		ProjectName:     projectName,
		TopicName:       topicName,
		SubscriptionId:  subId,
	})
	assert.NoError(t, err)
}

func TestDataHubConsumer_GetActiveShardIds(t *testing.T) {
	shardIds := consumer.GetActiveShardIds()
	assert.Greater(t, len(shardIds), 0)
}

func TestDataHubConsumer_GetShardCursor(t *testing.T) {
	cursor, err := consumer.GetShardCursor(consumer.GetActiveShardIds()[0], datahub.OLDEST)
	assert.NoError(t, err)
	t.Log(cursor)
	cursor, err = consumer.GetShardCursor(consumer.GetActiveShardIds()[0], datahub.LATEST)
	assert.NoError(t, err)
	t.Log(cursor)
}

func TestDataHubConsumer_StartConsume(t *testing.T) {
	recordChan, errChan := consumer.StartConsume(&ConsumeOptions{
		LimitPerTime:      10,
		CommitOffsetLimit: 100,
		DefaultCursorType: datahub.OLDEST,
	})

	go func() {
		for record := range recordChan {
			r := (*record).(*datahub.BlobRecord)
			// t.Log(*record)
			t.Log(string(r.RawData))
		}
	}()
	go func() {
		for err := range errChan {
			t.Error(err)
		}
	}()

	ch := make(chan int)
	<-ch
}
