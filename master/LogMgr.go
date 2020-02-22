package master

import (
	"context"
	"time"

	"github.com/latesun/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// mongodb日志管理
type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

func InitLogMgr() (err error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(G_config.MongodbUri))
	if err != nil {
		return
	}

	G_logMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}

// 查看任务日志
func (logMgr *LogMgr) ListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error) {
	var (
		filter *common.JobLogFilter
		// logSort *common.SortLogByStartTime
		cur    *mongo.Cursor
		jobLog *common.JobLog
	)

	// len(logArr)
	logArr = make([]*common.JobLog, 0)

	// 过滤条件
	filter = &common.JobLogFilter{JobName: name}

	// 按照任务开始时间倒排
	// logSort = &common.SortLogByStartTime{SortOrder: -1}

	// 查询
	// if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, findopt.Sort(logSort), findopt.Skip(int64(skip)), findopt.Limit(int64(limit))); err != nil {
	// 	return
	// }
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	cur, err = logMgr.logCollection.Find(ctx, filter)
	if err != nil {
		return
	}

	// 延迟释放游标
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		jobLog = &common.JobLog{}

		// 反序列化BSON
		if err = cur.Decode(jobLog); err != nil {
			continue // 有日志不合法
		}

		logArr = append(logArr, jobLog)
	}
	return
}
