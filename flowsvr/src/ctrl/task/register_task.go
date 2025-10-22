package task

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/niuniumart/asyncflow/flowsvr/src/constant"
	"github.com/niuniumart/asyncflow/flowsvr/src/db"
	"github.com/niuniumart/asyncflow/taskutils/rpc/model"
	"github.com/niuniumart/gosdk/handler"
	"github.com/niuniumart/gosdk/martlog"
	"net/http"
)

// RegisterTaskHandler 注册任务：创建对应的任务信息表t_taskType_1，在位置信息表插入一条(beginPos, endPos)为(1, 1)的记录
type RegisterTasksHandler struct {
	Req  model.RegisterTaskReq
	Resp model.RegisterTaskResp
}

func RegisterTask(c *gin.Context) {
	var hd RegisterTasksHandler
	defer func() {
		hd.Resp.Msg = constant.GetErrMsg(hd.Resp.Code)
		c.JSON(http.StatusOK, hd.Resp)
	}()
	if err := c.ShouldBind(&hd.Req); err != nil {
		martlog.Errorf("RegisterTask shouldBind err %s", err.Error())
		hd.Resp.Code = constant.ERR_SHOULD_BIND
		return
	}
	handler.Run(&hd)
}

func (p *RegisterTasksHandler) HandleInput() error {

	if p.Req.TaskType == "" {
		martlog.Errorf("task type is empty")
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return constant.ERR_HANDLE_INPUT
	}

	return nil
}

// HandleProcess 处理函数
func (p *RegisterTasksHandler) HandleProcess() error {

	// 1. 创建任务信息表（默认都是从1开始）
	if err := db.TaskNsp.CreateTable(db.DB, p.Req.TaskType, "1"); err != nil {
		// 出现错误
		martlog.Errorf("RegisterTask newTable %v is exist, err is %v", fmt.Sprintf("t_%s_%s_%d", p.Req.TaskType, "task", 1), err)
		p.Resp.Code = constant.ERR_REGISTER_TASK_CREATE_NEW_TABLE
		return err
	}

	// 2. 在位置信息表中创建一条记录
	if err := db.TaskPosNsp.Create(db.DB, &db.TaskPos{
		TaskType:         p.Req.TaskType,
		ScheduleBeginPos: 1,
		ScheduleEndPos:   1,
	}); err != nil {
		martlog.Errorf("RegisterTask insert failed: %v", err)
		p.Resp.Code = constant.ERR_REGISTER_TASK_INSERT_RECORD
		return err
	}

	// 如果可以来到这里，说明我们注册成功
	return nil
}
