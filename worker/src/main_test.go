package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/niuniumart/asyncflow/flowsvr/src/config"
	"github.com/niuniumart/asyncflow/flowsvr/src/db"
	"github.com/niuniumart/asyncflow/taskutils/rpc"
	"github.com/niuniumart/asyncflow/taskutils/rpc/model"
	"github.com/niuniumart/gosdk/redislock"
	"github.com/niuniumart/gosdk/tools"
	"github.com/smartystreets/goconvey/convey"
	"sync"
	"testing"
)

// TestCreateTask 测试创建任务接口
func TestCreateTask(t *testing.T) {
	config.Init()
	db.InitDB()
	convey.Convey("TestCreateTask", t, func() {
		// case 1: input err
		var rpc rpc.TaskRpc
		rpc.Host = "http://43.139.192.217:41555"
		var reqBody = new(model.CreateTaskReq)
		reqBody.TaskData.TaskType = "lark"
		reqBody.TaskData.TaskStage = "sendmsg"
		reqBody.TaskData.UserId = "niuniu"
		reqBody.TaskData.Status = 1
		var ltctx = LarkTaskContext{
			ReqBody: &LarkReq{Msg: "nice to meet u", FromAddr: "fish", ToAddr: "cat"},
		}
		ltctxStr, _ := json.Marshal(ltctx)
		reqBody.TaskData.TaskContext = string(ltctxStr)
		resp, err := rpc.CreateTask(reqBody)
		fmt.Println(tools.GetFmtStr(resp))
		fmt.Println(err)
		convey.So(err, convey.ShouldBeNil)

	})
}

// TestHoldTaskNotLock 测试不加锁的情况下，多个协程获取任务
func TestHoldTaskNotLock(t *testing.T) {
	convey.Convey("TestHoldTask", t, func() {
		var n = 10
		var taskList [][]*model.TaskData
		for i := 0; i < n; i++ {
			taskList = make([][]*model.TaskData, n)
		}
		wg := sync.WaitGroup{}
		hold := func(i int) {
			defer wg.Done()
			var rpc rpc.TaskRpc
			rpc.Host = "http://127.0.0.1:41555"
			var reqBody = new(model.HoldTasksReq)
			reqBody.TaskType = "lark"
			reqBody.Limit = 10

			resp, err := rpc.HoldTasks(reqBody)
			if err != nil {
				fmt.Println(err)
				return
			}
			taskList[i] = resp.TaskList
		}
		wg.Add(n)

		for i := 0; i < n; i++ {
			go hold(i)
		}
		wg.Wait()

		for i := 0; i < n; i++ {
			for _, task := range taskList[i] {
				fmt.Print(tools.GetFmtStr(task), " ")
			}
			fmt.Println()
		}
	})

}

// TestHoldTaskLocked 测试加锁的情况下，多个协程获取任务
func TestHoldTaskLocked(t *testing.T) {
	convey.Convey("TestHoldTask", t, func() {
		// 锁地址
		var lockAddr = "127.0.0.1:6379"
		// 锁密码
		var lockPassword = ""
		var n = 10
		var taskList [][]*model.TaskData
		for i := 0; i < n; i++ {
			taskList = make([][]*model.TaskData, n)
		}
		wg := sync.WaitGroup{}
		hold := func(i int) {
			defer wg.Done()
			var rpc rpc.TaskRpc
			rpc.Host = "http://127.0.0.1:41555"
			lockClient := redislock.NewClient(lockAddr, lockPassword)
			var reqBody = new(model.HoldTasksReq)
			reqBody.TaskType = "lark"
			reqBody.Limit = 10

			// 加锁
			mutex := redislock.NewRedisLock(reqBody.TaskType, lockClient, redislock.WithBlock(), redislock.WithWatchDogMode(), redislock.WithExpireSeconds(3))
			if err := mutex.Lock(context.Background()); err != nil {
				fmt.Println("lock err：", err)
				return
			}
			resp, err := rpc.HoldTasks(reqBody)
			if err := mutex.Unlock(context.Background()); err != nil {
				fmt.Println("unlock err：", err)
				return
			}
			if err != nil {
				fmt.Println(err)
				return
			}
			taskList[i] = resp.TaskList
		}
		wg.Add(n)

		for i := 0; i < n; i++ {
			go hold(i)
		}
		wg.Wait()

		for i := 0; i < n; i++ {
			for _, task := range taskList[i] {
				fmt.Print(tools.GetFmtStr(task), " ")
			}
			fmt.Println()
		}
	})

}
