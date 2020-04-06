package master

import (
	"context"
	"time"

	"github.com/latesun/crontab/common"
	"go.etcd.io/etcd/clientv3"
)

// /cron/workers/
type workerMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var WorkerMgr *workerMgr

// 获取在线worker列表
func (wm *workerMgr) ListWorkers() (workerArr []string, err error) {
	// 初始化数组
	workerArr = make([]string, 0)

	// 获取目录下所有Kv
	getResp, err := wm.kv.Get(context.TODO(), common.JOB_WORKER_DIR, clientv3.WithPrefix())
	if err != nil {
		return
	}

	// 解析每个节点的IP
	for _, kv := range getResp.Kvs {
		// kv.Key : /cron/workers/192.168.2.1
		workerIP := common.ExtractWorkerIP(string(kv.Key))
		workerArr = append(workerArr, workerIP)
	}
	return
}

func InitWorkerMgr() (err error) {
	// 初始化配置
	config := clientv3.Config{
		Endpoints:   Config.EtcdEndpoints,                                     // 集群地址
		DialTimeout: time.Duration(Config.EtcdDialTimeout) * time.Millisecond, // 连接超时
	}

	// 建立连接
	client, err := clientv3.New(config)
	if err != nil {
		return
	}

	// 得到KV和Lease的API子集
	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)

	WorkerMgr = &workerMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return
}
