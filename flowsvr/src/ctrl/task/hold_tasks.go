package task

import (
	"fmt"
	"net/http"

	"github.com/niuniumart/asyncflow/flowsvr/src/constant"
	"github.com/niuniumart/asyncflow/flowsvr/src/ctrl/ctrlmodel"
	"github.com/niuniumart/asyncflow/flowsvr/src/db"
	"github.com/niuniumart/asyncflow/taskutils/rpc/model"

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
	// 注册延迟执行函数
	defer func() {
		hd.Resp.Msg = constant.GetErrMsg(hd.Resp.Code)
		martlog.Infof("HoldTasks "+
			"resp code %d, msg %s, taskCount %d", hd.Resp.Code, hd.Resp.Msg, len(hd.Resp.TaskList))
		c.JSON(http.StatusOK, hd.Resp)
	}()
	// 获取用户Id
	hd.UserId = c.Request.Header.Get(constant.HEADER_USERID)
	// W:c.ShouldBind()将JSON请求体解析到hd.Req结构体中
	if err := c.ShouldBind(&hd.Req); err != nil {
		martlog.Errorf("HoldTasks shouldBind err %s", err.Error())
		hd.Resp.Code = constant.ERR_SHOULD_BIND
		return
	}
	// W:日志记录
	martlog.Infof("HoldTasks hd.Req %s", tools.GetFmtStr(hd.Req))
	// W:执行业务逻辑
	handler.Run(&hd)
}

// HandleInput 参数检查
func (p *HoldTasksHandler) HandleInput() error {
	// W:验证TaskType参数是否为空
	if p.Req.TaskType == "" {
		martlog.Errorf("input invalid")
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return constant.ERR_HANDLE_INPUT
	}
	return nil
}

// HandleProcess 处理函数
func (p *HoldTasksHandler) HandleProcess() error {
	// W:拉取任务的最大限制
	limit := p.Req.Limit
	if limit > constant.MAX_TASK_LIST_LIMIT {
		limit = constant.MAX_TASK_LIST_LIMIT
	}
	if limit == 0 {
		limit = constant.DEFAULT_TASK_LIST_LIMIT
	}
	// W:获取任务位置信息
	taskPos, err := db.TaskPosNsp.GetTaskPos(db.DB, p.Req.TaskType)
	if err != nil {
		martlog.Errorf("db.TaskPosNsp.GetRandomSchedulePos %s", err.Error())
		p.Resp.Code = constant.ERR_GET_TASK_SET_POS_FROM_DB
		return err
	}
	// W:查询指定数量处于PENDING状态的任务，按order_time升序排序
	taskList, err := db.TaskNsp.GetTaskList(db.DB, fmt.Sprintf(
		"%d", taskPos.ScheduleBeginPos), p.Req.TaskType, db.TaskEnum(db.TASK_STATUS_PENDING), limit)
	if err != nil {
		martlog.Errorf("HoldTasks %s", err.Error())
		p.Resp.Code = constant.ERR_GET_TASK_LIST_FROM_DB
		return err
	}
	// W:遍历查询到的任务id
	taskIdList := make([]string, 0)
	for _, dbTask := range taskList {
		// 需要更新状态的任务
		taskIdList = append(taskIdList, dbTask.TaskId)
		// 将数据库模型转换为响应模型
		var task = &model.TaskData{}
		ctrlmodel.FillTaskResp(dbTask, task)
		p.Resp.TaskList = append(p.Resp.TaskList, task)
	}

	// 更新任务状态
	if len(taskIdList) != 0 {
		// W:对所有查询到的任务，将任务状态从pending转换为processing
		// W:防止同一任务被多个worker重复处理，实现了任务的互斥占用机制
		err = db.TaskNsp.BatchSetStatus(db.DB, taskIdList, db.TASK_STATUS_PROCESSING)
		if err != nil {
			martlog.Errorf("BatchSetStatus err %s", err.Error())
			return err
		}
	}

	return nil
}
