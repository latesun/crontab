package master

import (
	"context"
	"encoding/json"
	"time"

	"github.com/latesun/crontab/common"
	"go.etcd.io/etcd/clientv3"
)

// 任务管理器
type jobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var JobMgr *jobMgr

// 初始化管理器
func InitJobMgr() (err error) {
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

	// 赋值单例
	JobMgr = &jobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}

// 保存任务
func (jm *jobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	// 把任务保存到/cron/jobs/任务名 -> json

	// etcd的保存key
	jobKey := common.JOB_SAVE_DIR + job.Name
	// 任务信息json
	jobValue, err := json.Marshal(job)
	if err != nil {
		return
	}

	// 保存到etcd
	putResp, err := jm.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV())
	if err != nil {
		return
	}

	// 如果是更新, 那么返回旧值
	var oldJobObj common.Job
	if putResp.PrevKv != nil {
		// 对旧值做一个反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}

	return
}

// 删除任务
func (jm *jobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	// etcd中保存任务的key
	jobKey := common.JOB_SAVE_DIR + name

	// 从etcd中删除它
	delResp, err := jm.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV())
	if err != nil {
		return
	}

	// 返回被删除的任务信息
	var oldJobObj common.Job
	if len(delResp.PrevKvs) != 0 {
		// 解析一下旧值, 返回它
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}

	return
}

// 列举任务
func (jm *jobMgr) ListJobs() (jobList []*common.Job, err error) {
	// 任务保存的目录
	dirKey := common.JOB_SAVE_DIR

	// 获取目录下所有任务信息
	getResp, err := jm.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix())
	if err != nil {
		return
	}

	// 初始化数组空间
	jobList = make([]*common.Job, 0)
	// len(jobList) == 0

	// 遍历所有任务, 进行反序列化
	var job *common.Job
	for _, kvPair := range getResp.Kvs {
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

// 杀死任务
func (jm *jobMgr) KillJob(name string) (err error) {
	// 更新一下key=/cron/killer/任务名

	// 通知worker杀死对应任务
	killerKey := common.JOB_KILLER_DIR + name

	// 让worker监听到一次put操作, 创建一个租约让其稍后自动过期即可
	grant, err := jm.lease.Grant(context.TODO(), 1)
	if err != nil {
		return
	}

	// 设置killer标记
	if _, err = jm.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(grant.ID)); err != nil {
		return
	}

	return
}
