package worker

import (
	"fmt"
	"time"

	"github.com/latesun/crontab/common"
)

// 任务调度
type scheduler struct {
	jobEventChan      chan *common.JobEvent              //  etcd任务事件队列
	jobPlanTable      map[string]*common.JobSchedulePlan // 任务调度计划表
	jobExecutingTable map[string]*common.JobExecuteInfo  // 任务执行表
	jobResultChan     chan *common.JobExecuteResult      // 任务结果队列
}

var Scheduler *scheduler

// 处理任务事件
func (s *scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	// var (
	var (
		jobSchedulePlan *common.JobSchedulePlan
		// 	jobExecuteInfo  *common.JobExecuteInfo
		// 	jobExecuting    bool
		jobExisted bool
		err        error
	)

	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE: // 保存任务事件
		jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job)
		if err != nil {
			return
		}
		s.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE: // 删除任务事件
		jobSchedulePlan, jobExisted = s.jobPlanTable[jobEvent.Job.Name]
		if jobExisted {
			delete(s.jobPlanTable, jobEvent.Job.Name)
		}
	case common.JOB_EVENT_KILL: // 强杀任务事件
		// 取消掉Command执行, 判断任务是否在执行中
		jobExecuteInfo, jobExecuting := s.jobExecutingTable[jobEvent.Job.Name]
		if jobExecuting {
			jobExecuteInfo.CancelFunc() // 触发command杀死shell子进程, 任务得到退出
		}
	}
}

// 尝试执行任务
func (s *scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	// 调度 和 执行 是2件事情

	// 执行的任务可能运行很久, 1分钟会调度60次，但是只能执行1次, 防止并发！

	// 如果任务正在执行，跳过本次调度
	jobExecuteInfo, jobExecuting := s.jobExecutingTable[jobPlan.Job.Name]
	if jobExecuting {
		// fmt.Println("尚未退出,跳过执行:", jobPlan.Job.Name)
		return
	}

	// 构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	// 保存执行状态
	s.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	// 执行任务
	fmt.Println("执行任务:", jobExecuteInfo.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)
	Executor.ExecuteJob(jobExecuteInfo)
}

// 重新计算任务调度状态
func (s *scheduler) TrySchedule() (scheduleAfter time.Duration) {
	// 如果任务表为空话，随便睡眠多久
	if len(s.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}

	// 当前时间
	now := time.Now()

	var nearTime *time.Time
	// 遍历所有任务
	for _, jobPlan := range s.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			s.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now) // 更新下次执行时间
		}

		// 统计最近一个要过期的任务时间
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}
	// 下次调度间隔（最近要执行的任务调度时间 - 当前时间）
	scheduleAfter = (*nearTime).Sub(now)
	return
}

// 处理任务结果
func (s *scheduler) handleJobResult(result *common.JobExecuteResult) {
	var jobLog *common.JobLog

	// 删除执行状态
	delete(s.jobExecutingTable, result.ExecuteInfo.Job.Name)

	// 生成执行日志
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &common.JobLog{
			JobName:      result.ExecuteInfo.Job.Name,
			Command:      result.ExecuteInfo.Job.Command,
			Output:       string(result.Output),
			PlanTime:     result.ExecuteInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: result.ExecuteInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime:    result.StartTime.UnixNano() / 1000 / 1000,
			EndTime:      result.EndTime.UnixNano() / 1000 / 1000,
		}
		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		} else {
			jobLog.Err = ""
		}
		LogSink.Append(jobLog)
	}

	// fmt.Println("任务执行完成:", result.ExecuteInfo.Job.Name, string(result.Output), result.Err)
}

// 调度协程
func (s *scheduler) scheduleLoop() {
	var (
		jobEvent      *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult     *common.JobExecuteResult
	)

	// 初始化一次(1秒)
	scheduleAfter = s.TrySchedule()

	// 调度的延迟定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	// 定时任务common.Job
	for {
		select {
		case jobEvent = <-s.jobEventChan: //监听任务变化事件
			// 对内存中维护的任务列表做增删改查
			s.handleJobEvent(jobEvent)
		case <-scheduleTimer.C: // 最近的任务到期了
		case jobResult = <-s.jobResultChan: // 监听任务执行结果
			s.handleJobResult(jobResult)
		}
		// 调度一次任务
		scheduleAfter = s.TrySchedule()
		// 重置调度间隔
		scheduleTimer.Reset(scheduleAfter)
	}
}

// 推送任务变化事件
func (s *scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	s.jobEventChan <- jobEvent
}

// 初始化调度器
func InitScheduler() (err error) {
	Scheduler = &scheduler{
		jobEventChan:      make(chan *common.JobEvent, 1000),
		jobPlanTable:      make(map[string]*common.JobSchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan:     make(chan *common.JobExecuteResult, 1000),
	}
	// 启动调度协程
	go Scheduler.scheduleLoop()
	return
}

// 回传任务执行结果
func (s *scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	s.jobResultChan <- jobResult
}
