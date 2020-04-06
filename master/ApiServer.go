package master

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/latesun/crontab/common"
)

// 列举所有crontab任务
// GET /job
func listJobs(c *gin.Context) {
	jobs, err := JobMgr.ListJobs()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}

	c.JSON(http.StatusOK, gin.H{"jobs": jobs})
}

// 保存任务接口
// POST /job {"name": "job1", "command": "echo hello", "cronExpr": "* * * * *"}
func saveJob(c *gin.Context) {
	var job common.Job
	if err := c.ShouldBindJSON(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	oldJob, err := JobMgr.SaveJob(&job)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": oldJob})
}

// 删除任务接口
// DELETE /job name=job1
func deleteJob(c *gin.Context) {
	name := c.Query("name")
	fmt.Println("name:", name)
	oldJob, err := JobMgr.DeleteJob(name)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "job": oldJob})
}

// 强制杀死某个任务
// POST /killer job=job1
func killJob(c *gin.Context) {
	job := c.Query("job")
	if err := JobMgr.KillJob(job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

// 查询任务日志
// GET /log?name=job10&skip=0&limit=10
func listLogs(c *gin.Context) {
	// 获取请求参数
	name := c.Query("name")
	skipParam := c.Query("skip")
	limitParam := c.Query("limit")

	skip, err := strconv.Atoi(skipParam)
	if err != nil {
		skip = 0
	}
	limit, err := strconv.Atoi(limitParam)
	if err != nil {
		limit = 20
	}

	logs, err := LogMgr.ListLog(name, skip, limit)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"logs": logs})
}

// 查看健康 worker 节点
// GET /worker
func listWorkers(c *gin.Context) {
	workers, err := WorkerMgr.ListWorkers()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"workers": workers})
}

func APIGateway() {
	r := gin.Default()

	r.GET("/job", listJobs)
	r.POST("/job", saveJob)
	r.DELETE("/job", deleteJob)
	r.GET("/log", listLogs)
	r.GET("/worker", listWorkers)
	r.POST("/killer", killJob)
	r.StaticFS("/web", http.Dir("webroot"))

	panic(r.Run(":9999"))
}
