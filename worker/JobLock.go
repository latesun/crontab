package worker

import (
	"context"

	"github.com/latesun/crontab/common"
	"go.etcd.io/etcd/clientv3"
)

// 分布式锁(TXN事务)
type jobLock struct {
	// etcd客户端
	kv    clientv3.KV
	lease clientv3.Lease

	jobName    string             // 任务名
	cancelFunc context.CancelFunc // 用于终止自动续租
	leaseID    clientv3.LeaseID   // 租约ID
	isLocked   bool               // 是否上锁成功
}

var JobLock *jobLock

// 初始化一把锁
func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) *jobLock {
	JobLock = &jobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}

	return JobLock
}

// 尝试上锁
func (lock *jobLock) TryLock() (err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		cancelCtx      context.Context
		cancelFunc     context.CancelFunc
		leaseId        clientv3.LeaseID
		keepRespChan   <-chan *clientv3.LeaseKeepAliveResponse
		txn            clientv3.Txn
		lockKey        string
		txnResp        *clientv3.TxnResponse
	)

	// 1, 创建租约(5秒)
	if leaseGrantResp, err = lock.lease.Grant(context.TODO(), 5); err != nil {
		return
	}

	// context用于取消自动续租
	cancelCtx, cancelFunc = context.WithCancel(context.TODO())

	// 租约ID
	leaseId = leaseGrantResp.ID

	// 2, 自动续租
	if keepRespChan, err = lock.lease.KeepAlive(cancelCtx, leaseId); err != nil {
		goto FAIL
	}

	// 3, 处理续租应答的协程
	go func() {
		var (
			keepResp *clientv3.LeaseKeepAliveResponse
		)
		for {
			select {
			case keepResp = <-keepRespChan: // 自动续租的应答
				if keepResp == nil {
					goto END
				}
			}
		}
	END:
	}()

	// 4, 创建事务txn
	txn = lock.kv.Txn(context.TODO())

	// 锁路径
	lockKey = common.JOB_LOCK_DIR + lock.jobName

	// 5, 事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))

	// 提交事务
	if txnResp, err = txn.Commit(); err != nil {
		goto FAIL
	}

	// 6, 成功返回, 失败释放租约
	if !txnResp.Succeeded { // 锁被占用
		err = common.ERR_LOCK_ALREADY_REQUIRED
		goto FAIL
	}

	// 抢锁成功
	lock.leaseID = leaseId
	lock.cancelFunc = cancelFunc
	lock.isLocked = true
	return

FAIL:
	cancelFunc()                               // 取消自动续租
	lock.lease.Revoke(context.TODO(), leaseId) //  释放租约
	return
}

// 释放锁
func (lock *jobLock) Unlock() {
	if lock.isLocked {
		lock.cancelFunc()                               // 取消我们程序自动续租的协程
		lock.lease.Revoke(context.TODO(), lock.leaseID) // 释放租约
	}
}
