package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/latesun/crontab/master"
)

var (
	confFile string // 配置文件路径
)

// 初始化线程数量
func init() {
	// 设置最大线程数
	runtime.GOMAXPROCS(runtime.NumCPU())
	// 解析命令行参数
	// master -config ./master.json -xxx 123 -yyy ddd
	// master -h
	flag.StringVar(&confFile, "config", "./master.json", "指定master.json")
	flag.Parse()
}

func main() {
	// 加载配置
	if err := master.InitConfig(confFile); err != nil {
		fmt.Println(err)
	}

	// 初始化服务发现模块
	if err := master.InitWorkerMgr(); err != nil {
		fmt.Println(err)
	}

	// 日志管理器
	if err := master.InitLogMgr(); err != nil {
		fmt.Println(err)
	}

	//  任务管理器
	if err := master.InitJobMgr(); err != nil {
		fmt.Println(err)
	}

	// 启动Api HTTP服务
	master.APIGateway()

	// 正常退出
	for {
		time.Sleep(1 * time.Second)
	}

}
