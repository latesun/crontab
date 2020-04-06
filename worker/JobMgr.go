package worker

import (
	"context"
	"time"

	"github.com/latesun/crontab/common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

// 任务管理器
type jobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var JobMgr *jobMgr

// 监听任务变化
func (jm *jobMgr) watchJobs() (err error) {
	// var (
	// 	getResp            *clientv3.GetResponse
	// 	kvpair             *mvccpb.KeyValue
	// 	job                *common.Job
	// 	watchStartRevision int64
	// 	watchChan          clientv3.WatchChan
	// 	watchResp          clientv3.WatchResponse
	// 	watchEvent         *clientv3.Event
	// 	jobName            string
	// 	jobEvent           *common.JobEvent
	// )

	// 1, get一下/cron/jobs/目录下的所有任务，并且获知当前集群的revision
	getResp, err := jm.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix())
	if err != nil {
		return
	}

	// 当前有哪些任务
	for _, kvpair := range getResp.Kvs {
		// 反序列化json得到Job
		job, err := common.UnpackJob(kvpair.Value)
		if err == nil {
			jobEvent := common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
			// 同步给scheduler(调度协程)
			Scheduler.PushJobEvent(jobEvent)
		}
	}

	// 2, 从该revision向后监听变化事件
	go func() { // 监听协程
		// 从GET时刻的后续版本开始监听变化
		watchStartRevision := getResp.Header.Revision + 1
		// 监听/cron/jobs/目录的后续变化
		watchChan := jm.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		// 处理监听事件
		for watchResp := range watchChan {
			for _, watchEvent := range watchResp.Events {
				var jobEvent *common.JobEvent
				switch watchEvent.Type {
				case mvccpb.PUT: // 任务保存事件
					job, err := common.UnpackJob(watchEvent.Kv.Value)
					if err != nil {
						continue
					}
					// 构建一个更新Event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
				case mvccpb.DELETE: // 任务被删除了
					// Delete /cron/jobs/job10
					jobName := common.ExtractJobName(string(watchEvent.Kv.Key))
					job := &common.Job{Name: jobName}

					// 构建一个删除Event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
				}
				// 变化推给scheduler
				Scheduler.PushJobEvent(jobEvent)
			}
		}
	}()
	return
}

// 监听强杀任务通知
func (jm *jobMgr) watchKiller() {
	// 监听/cron/killer目录
	go func() { // 监听协程
		// 监听/cron/killer/目录的变化
		watchChan := jm.watcher.Watch(context.TODO(), common.JOB_KILLER_DIR, clientv3.WithPrefix())
		// 处理监听事件
		for watchResp := range watchChan {
			for _, watchEvent := range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: // 杀死任务事件
					jobName := common.ExtractKillerName(string(watchEvent.Kv.Key))
					job := &common.Job{Name: jobName}
					jobEvent := common.BuildJobEvent(common.JOB_EVENT_KILL, job)
					// 事件推给scheduler
					Scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: // killer标记过期, 被自动删除
				}
			}
		}
	}()
}

// 初始化管理器
func InitJobMgr() (err error) {
	// 初始化配置

	// 建立连接
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   Config.EtcdEndpoints,                                     // 集群地址
		DialTimeout: time.Duration(Config.EtcdDialTimeout) * time.Millisecond, // 连接超时
	})
	if err != nil {
		return
	}

	// 得到KV和Lease的API子集
	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)
	watcher := clientv3.NewWatcher(client)

	// 赋值单例
	JobMgr = &jobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	// 启动任务监听
	JobMgr.watchJobs()

	// 启动监听killer
	JobMgr.watchKiller()

	return
}

// 创建任务执行锁
func (jm *jobMgr) CreateJobLock(jobName string) (lock *jobLock) {
	lock = InitJobLock(jobName, jm.kv, jm.lease)
	return
}
