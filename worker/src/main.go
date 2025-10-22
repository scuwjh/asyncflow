package main

import (
	"encoding/json"
	"fmt"
	"github.com/niuniumart/asyncflow/taskutils/constant"
	"github.com/niuniumart/asyncflow/worker/src/config"
	"github.com/niuniumart/asyncflow/worker/src/tasksdk"
	"github.com/niuniumart/gosdk/martlog"
	"github.com/niuniumart/gosdk/response"
)

// 初始化
func init() {
	config.Init()
	// 初始化分布式锁连接，还有定下flowsvr的地址
	tasksdk.InitSvr(config.Conf.FlowsvrAddr, config.Conf.RedisLockAddr, config.Conf.RedisLockPassword)
}

func main() {
	larkTask := tasksdk.TaskHandler{
		TaskType: "lark",
		NewProc:  func() tasksdk.TaskIntf { return new(LarkTask) },
	}
	tasksdk.RegisterHandler(&larkTask)
	var taskMgr = tasksdk.TaskMgr{
		TaskType: "lark",
	}
	taskMgr.Schedule()
}

type LarkReq struct {
	Msg      string
	FromAddr string
	ToAddr   string
}

// LarkTask 飞书任务结构
type LarkTask struct {
	tasksdk.TaskBase
	ContextData *LarkTaskContext
}

// LarkTaskContext 任务的上下文
type LarkTaskContext struct {
	ReqBody *LarkReq
	UserId  string
}

// ContextLoad 解析上下文
func (p *LarkTask) ContextLoad() error {
	martlog.Infof("run lark task %s", p.TaskId)
	err := json.Unmarshal([]byte(p.TaskContext), &p.ContextData)
	if err != nil {
		martlog.Errorf("json unmarshal for context err %s", err.Error())
		return response.RESP_JSON_UNMARSHAL_ERROR
	}
	if p.ContextData.ReqBody == nil {
		p.ContextData.ReqBody = new(LarkReq)
	}
	return nil
}

// HandleProcess 处理函数
func (p *LarkTask) HandleProcess() error {
	switch p.TaskStage {
	case "sendmsg":
		p.TaskStage = "record"
		p.SetContextLocal(p.ContextData)
		fallthrough
	case "record":
		p.TaskStage = "record"
		// 模拟出现错误
		p.Base().Status = int(constant.TASK_STATUS_SUCC)
		fmt.Printf("处理 task完成: %v\n", p.TaskId)
	default:
		p.Base().Status = int(constant.TASK_STATUS_FAILED)
	}
	return nil
}

// HandleFinish 任务完成函数
func (p *LarkTask) HandleFinish() {
}
