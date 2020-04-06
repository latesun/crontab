package worker

import (
	"context"
	"time"

	"github.com/latesun/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// mongodb存储日志
type logSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var LogSink *logSink

// 批量写入日志
func (s *logSink) saveLogs(batch *common.LogBatch) {
	s.logCollection.InsertMany(context.TODO(), batch.Logs)
}

// 日志存储协程
func (s *logSink) writeLoop() {
	// var (
	// 	log          *common.JobLog
	var logBatch *common.LogBatch // 当前的批次
	var commitTimer *time.Timer
	// 	timeoutBatch *common.LogBatch // 超时批次
	// )

	for {
		select {
		case log := <-s.logChan:
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				// 让这个批次超时自动提交(给1秒的时间）
				commitTimer = time.AfterFunc(
					time.Duration(Config.JobLogCommitTimeout)*time.Millisecond,
					func(batch *common.LogBatch) func() {
						return func() {
							s.autoCommitChan <- batch
						}
					}(logBatch),
				)
			}

			// 把新日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)

			// 如果批次满了, 就立即发送
			if len(logBatch.Logs) >= Config.JobLogBatchSize {
				// 发送日志
				s.saveLogs(logBatch)
				// 清空logBatch
				logBatch = nil
				// 取消定时器
				commitTimer.Stop()
			}
		case timeoutBatch := <-s.autoCommitChan: // 过期的批次
			// 判断过期批次是否仍旧是当前的批次
			if timeoutBatch != logBatch {
				continue // 跳过已经被提交的批次
			}
			// 把批次写入到mongo中
			s.saveLogs(timeoutBatch)
			// 清空logBatch
			logBatch = nil
		}
	}
}

func InitLogSink() (err error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(Config.MongodbURI))
	if err != nil {
		return
	}

	//   选择db和collection
	LogSink = &logSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}

	// 启动一个mongodb处理协程
	go LogSink.writeLoop()
	return
}

// 发送日志
func (s *logSink) Append(jobLog *common.JobLog) {
	select {
	case s.logChan <- jobLog:
	default:
		// 队列满了就丢弃
	}
}
