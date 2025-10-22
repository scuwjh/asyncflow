package task

import (
	"fmt"
	"github.com/niuniumart/asyncflow/flowsvr/src/constant"
	"github.com/niuniumart/asyncflow/flowsvr/src/ctrl/ctrlmodel"
	"github.com/niuniumart/asyncflow/flowsvr/src/db"
	"github.com/niuniumart/asyncflow/taskutils/rpc/model"
	"net/http"

	"github.com/niuniumart/gosdk/martlog"

	"github.com/niuniumart/gosdk/tools"

	"github.com/gin-gonic/gin"
	"github.com/niuniumart/gosdk/handler"
)

// HoldTasksHandler 接口处理handler
type HoldTasksHandler struct {
	Req    model.HoldTasksReq
	Resp   model.HoldTasksResp
	UserId string
}

// HoldTasks 接口
func HoldTasks(c *gin.Context) {

	var hd HoldTasksHandler
	defer func() {
		hd.Resp.Msg = constant.GetErrMsg(hd.Resp.Code)
		martlog.Infof("HoldTasks "+
			"resp code %d, msg %s, taskCount %d", hd.Resp.Code, hd.Resp.Msg, len(hd.Resp.TaskList))
		c.JSON(http.StatusOK, hd.Resp)
	}()
	// 获取用户Id
	hd.UserId = c.Request.Header.Get(constant.HEADER_USERID)
	if err := c.ShouldBind(&hd.Req); err != nil {
		martlog.Errorf("HoldTasks shouldBind err %s", err.Error())
		hd.Resp.Code = constant.ERR_SHOULD_BIND
		return
	}
	martlog.Infof("HoldTasks hd.Req %s", tools.GetFmtStr(hd.Req))
	handler.Run(&hd)
}

// HandleInput 参数检查
func (p *HoldTasksHandler) HandleInput() error {
	if p.Req.TaskType == "" {
		martlog.Errorf("input invalid")
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return constant.ERR_HANDLE_INPUT
	}
	return nil
}

// HandleProcess 处理函数
func (p *HoldTasksHandler) HandleProcess() error {
	limit := p.Req.Limit
	if limit > constant.MAX_TASK_LIST_LIMIT {
		limit = constant.MAX_TASK_LIST_LIMIT
	}
	if limit == 0 {
		limit = constant.DEFAULT_TASK_LIST_LIMIT
	}
	taskPos, err := db.TaskPosNsp.GetTaskPos(db.DB, p.Req.TaskType)
	if err != nil {
		martlog.Errorf("db.TaskPosNsp.GetRandomSchedulePos %s", err.Error())
		p.Resp.Code = constant.ERR_GET_TASK_SET_POS_FROM_DB
		return err
	}
	taskList, err := db.TaskNsp.GetTaskList(db.DB, fmt.Sprintf(
		"%d", taskPos.ScheduleBeginPos), p.Req.TaskType, db.TaskEnum(db.TASK_STATUS_PENDING), limit)
	if err != nil {
		martlog.Errorf("HoldTasks %s", err.Error())
		p.Resp.Code = constant.ERR_GET_TASK_LIST_FROM_DB
		return err
	}
	taskIdList := make([]string, 0)
	for _, dbTask := range taskList {
		// 需要更新状态的任务
		taskIdList = append(taskIdList, dbTask.TaskId)

		var task = &model.TaskData{}
		ctrlmodel.FillTaskResp(dbTask, task)
		p.Resp.TaskList = append(p.Resp.TaskList, task)
	}

	// 更新任务状态
	if len(taskIdList) != 0 {
		err = db.TaskNsp.BatchSetStatus(db.DB, taskIdList, db.TASK_STATUS_PROCESSING)
		if err != nil {
			martlog.Errorf("BatchSetStatus err %s", err.Error())
			return err
		}
	}

	return nil
}
