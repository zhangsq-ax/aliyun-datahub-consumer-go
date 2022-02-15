package datahub_consumer

import (
	"fmt"
	"log"
	"time"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

// ConsumerOptions 消费设置
type ConsumeOptions struct {
	LimitPerTime      int                // 单次消费数量
	CommitOffsetLimit int                // 提交消费 Offset 的数量
	DefaultCursorType datahub.CursorType // 初次消费默认 cursor 类型
	DefaultParam      int64              // 初次消费参数，只有当 DefaultCursorType 为 SEQUENCE 或 SYSTEM_TIME 时有效
}

type DataHubConsumerOptions struct {
	Endpoint        string
	AccessKeyId     string
	AccessKeySecret string
	ProjectName     string
	TopicName       string
	SubscriptionId  string
}

type DataHubConsumer struct {
	opts          *DataHubConsumerOptions
	client        datahub.DataHubApi
	shards        []datahub.ShardEntry
	session       *datahub.OpenSubscriptionSessionResult
	topic         *datahub.GetTopicResult
	lastRecords   map[string]*datahub.IRecord // 记录每个 shard 最后消费的 record
	consumeCounts map[string]int              // 记录每个 shard 的当前已消费数量
}

func NewDataHubConsumer(opts *DataHubConsumerOptions) (helper *DataHubConsumer, err error) {
	helper = &DataHubConsumer{
		opts:          opts,
		client:        datahub.New(opts.AccessKeyId, opts.AccessKeySecret, opts.Endpoint),
		lastRecords:   make(map[string]*datahub.IRecord),
		consumeCounts: make(map[string]int),
	}
	err = helper.init()
	return
}

func (consumer *DataHubConsumer) init() (err error) {
	var lr *datahub.ListShardResult
	lr, err = consumer.client.ListShard(consumer.opts.ProjectName, consumer.opts.TopicName)
	if err != nil {
		return
	}
	consumer.shards = lr.Shards

	err = consumer.createSubscriptionSession()
	if err != nil {
		return
	}

	_, err = consumer.GetTopic()
	return
}

func (consumer *DataHubConsumer) createSubscriptionSession() error {
	shardIds := consumer.GetActiveShardIds()
	session, err := consumer.client.OpenSubscriptionSession(consumer.opts.ProjectName, consumer.opts.TopicName, consumer.opts.SubscriptionId, shardIds)
	if err != nil {
		return err
	}
	consumer.session = session
	fmt.Println("##################")
	fmt.Println(session.Offsets["0"].SessionId)
	return nil
}

// GetActiveShardIds 获取活动的 shard id 列表
func (consumer *DataHubConsumer) GetActiveShardIds() []string {
	var shardIds []string
	for _, shard := range consumer.shards {
		if shard.State == datahub.ACTIVE {
			shardIds = append(shardIds, shard.ShardId)
		}
	}
	return shardIds
}

// GetShardCursor 获取指定 shard 的消费 cursor
func (consumer *DataHubConsumer) GetShardCursor(shardId string, defaultCursorType datahub.CursorType, defaultParam ...int64) (cursor string, err error) {
	opts := consumer.opts
	client := consumer.client

	offset := consumer.session.Offsets[shardId]
	var gc *datahub.GetCursorResult
	if offset.Sequence < 0 {
		//----- 首次消费
		gc, err = client.GetCursor(opts.ProjectName, opts.TopicName, shardId, defaultCursorType, defaultParam...)
		if err != nil {
			return
		}
	} else {
		//----- 非首次继续消费
		gc, err = client.GetCursor(opts.ProjectName, opts.TopicName, shardId, datahub.SEQUENCE, offset.Sequence+1)
		if err != nil {
			// 如果错误是 SeekOutOfRange，表示当前 cursor 的数据已过期
			if _, ok := err.(*datahub.SeekOutOfRangeError); ok {
				// 按默认方式重新尝试获取
				gc, err = client.GetCursor(opts.ProjectName, opts.TopicName, shardId, defaultCursorType, defaultParam...)
				if err != nil {
					return
				}
			} else {
				return
			}
		}
	}

	cursor = gc.Cursor
	return
}

func (consumer *DataHubConsumer) GetTopic() (*datahub.GetTopicResult, error) {
	if consumer.topic == nil {
		client := consumer.client
		opts := consumer.opts
		var err error
		consumer.topic, err = client.GetTopic(opts.ProjectName, opts.TopicName)
		if err != nil {
			return nil, err
		}
	}
	return consumer.topic, nil
}

func (consumer *DataHubConsumer) GetRecordSchema() (schema *datahub.RecordSchema, err error) {
	topic := consumer.topic

	if topic.RecordType == datahub.BLOB {
		err = fmt.Errorf("the record type of topic is BLOB")
		return
	}

	schema = topic.RecordSchema
	return
}

func (consumer *DataHubConsumer) StartConsume(opts *ConsumeOptions) (chan *datahub.IRecord, chan error) {
	recordChan := make(chan *datahub.IRecord, 100)
	errChan := make(chan error, 100)

	for _, shardId := range consumer.GetActiveShardIds() {
		go consumer.startConsumeShard(shardId, opts, recordChan, errChan)
	}

	return recordChan, errChan
}

func (consumer *DataHubConsumer) startConsumeShard(shardId string, opts *ConsumeOptions, recordChan chan *datahub.IRecord, errChan chan error) {
	var (
		cursor string
		err    error
	)
	consumer.lastRecords[shardId] = nil
	consumer.consumeCounts[shardId] = 0

	if opts.DefaultCursorType == datahub.LATEST || opts.DefaultCursorType == datahub.OLDEST {
		cursor, err = consumer.GetShardCursor(shardId, opts.DefaultCursorType)
	} else {
		cursor, err = consumer.GetShardCursor(shardId, opts.DefaultCursorType, opts.DefaultParam)
	}
	if err != nil {
		errChan <- err
		return
	}
	consumedCount := 0
	for {
		// 消费指定数量的记录
		gr, err := consumer.consumeShard(shardId, cursor, opts.LimitPerTime)
		if err != nil {
			errChan <- err
			return
		}

		// 如果未取到则等待 5s
		if gr.RecordCount == 0 {
			if consumer.lastRecords[shardId] != nil {
				err := consumer.commitSubscriptionOffset(shardId)
				if err != nil {
					// 如果 offset 提交失败则停止循环
					errChan <- err
					break
				}
			}
			time.Sleep(5 * time.Second)
			continue
		}

		// 循环处理消费到的记录
		for _, record := range gr.Records {
			recordChan <- &record // 推送到 Channel

			consumer.lastRecords[shardId] = &record // 记录最后消费的 record
			consumer.consumeCounts[shardId] += 1    // 更新记数

			// 如果记录到达上限则提交 offset
			if consumedCount%opts.CommitOffsetLimit == 0 {
				err := consumer.commitSubscriptionOffset(shardId)
				if err != nil {
					// 如果 offset 提交失败则停止循环
					errChan <- err
					break
				}
			}
		}

		// 获取下次消费的 cursor
		cursor = gr.NextCursor
	}
}

func (consumer *DataHubConsumer) commitSubscriptionOffset(shardId string) error {
	client := consumer.client
	opts := consumer.opts

	record := consumer.lastRecords[shardId]
	if record == nil {
		// 如果没有最后消费的 record 信息表示没有未提交 offset 的消费，无需提交 Offset
		return nil
	}

	// 生成要提交的 offsetMap
	offset := consumer.session.Offsets[shardId]
	offset.Sequence = (*record).GetSequence()
	offset.Timestamp = (*record).GetSystemTime()
	offsetMap := map[string]datahub.SubscriptionOffset{shardId: offset}

	// 提交 offset
	log.Println("commit subscription offset", offsetMap)
	_, err := client.CommitSubscriptionOffset(opts.ProjectName, opts.TopicName, opts.SubscriptionId, offsetMap)
	if err != nil {
		if _, ok := err.(*datahub.SubscriptionOffsetResetError); ok {
			// 点位被重置，需要重新打开 session
			err = consumer.createSubscriptionSession()
			if err != nil {
				log.Println("reopen subscription session failed", err)
				return err
			}
		} else {
			log.Println("commit offset failed", err)
			return err
		}
	} else {
		// 重置 shard 的消费信息
		consumer.consumeCounts[shardId] = 0
		consumer.lastRecords[shardId] = nil
	}

	return nil
}

func (consumer *DataHubConsumer) consumeShard(shardId string, cursor string, limit int) (gr *datahub.GetRecordsResult, err error) {
	client := consumer.client
	opts := consumer.opts

	if consumer.topic.RecordType == datahub.TUPLE {
		gr, err = client.GetTupleRecords(opts.ProjectName, opts.TopicName, shardId, cursor, limit, consumer.topic.RecordSchema)
	} else {
		gr, err = client.GetBlobRecords(opts.ProjectName, opts.TopicName, shardId, cursor, limit)
	}

	return
}
