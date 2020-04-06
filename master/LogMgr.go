package master

import (
	"context"
	"time"

	"github.com/latesun/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// mongodb日志管理
type logMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var LogMgr *logMgr

func InitLogMgr() (err error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(Config.MongodbUri))
	if err != nil {
		return
	}

	LogMgr = &logMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}

// 查看任务日志
func (l *logMgr) ListLog(name string, skip int, limit int) (logs []*common.JobLog, err error) {
	logs = make([]*common.JobLog, 0)

	// 过滤条件
	filter := &common.JobLogFilter{JobName: name}

	// 按照任务开始时间倒排
	findOption := options.Find()
	findOption.SetSort(common.SortLogByStartTime{SortOrder: -1})

	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	cur, err := l.logCollection.Find(ctx, filter, findOption)
	if err != nil {
		return
	}

	// 延迟释放游标
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		jobLog := &common.JobLog{}

		// 反序列化BSON
		if err = cur.Decode(jobLog); err != nil {
			continue // 有日志不合法
		}

		logs = append(logs, jobLog)
	}
	return
}
