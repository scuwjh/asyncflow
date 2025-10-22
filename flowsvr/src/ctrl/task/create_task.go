package task

import (
	"fmt"
	"github.com/niuniumart/asyncflow/flowsvr/src/cache"
	"github.com/niuniumart/asyncflow/flowsvr/src/constant"
	"github.com/niuniumart/asyncflow/flowsvr/src/ctrl/ctrlmodel"
	"github.com/niuniumart/asyncflow/flowsvr/src/db"
	"github.com/niuniumart/asyncflow/taskutils/rpc/model"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/niuniumart/gosdk/handler"
	"github.com/niuniumart/gosdk/martlog"
)

// CreateTaskHandler 创建任务
type CreateTaskHandler struct {
	Req    model.CreateTaskReq
	Resp   model.CreateTaskResp
	UserId string
}

// CreateTask 接口
func CreateTask(c *gin.Context) {
	var hd CreateTaskHandler
	defer func() {
		hd.Resp.Msg = constant.GetErrMsg(hd.Resp.Code)
		c.JSON(http.StatusOK, hd.Resp)
	}()
	// 获取用户Id
	hd.UserId = c.Request.Header.Get(constant.HEADER_USERID)
	// 解析请求包
	if err := c.ShouldBind(&hd.Req); err != nil {
		martlog.Errorf("CreateTask shouldBind err %s", err.Error())
		hd.Resp.Code = constant.ERR_SHOULD_BIND
		return
	}
	// 执行处理函数, 这里会调用对应的HandleInput和HandleProcess，往下看
	handler.Run(&hd)
}

// HandleInput 参数检查
func (p *CreateTaskHandler) HandleInput() error {
	if p.Req.TaskData.TaskType == "" {
		martlog.Errorf("input invalid")
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return constant.ERR_HANDLE_INPUT
	}
	if p.Req.TaskData.Priority != nil {
		if *p.Req.TaskData.Priority > db.MAX_PRIORITY || *p.Req.TaskData.Priority < 0 {
			p.Resp.Code = constant.ERR_INPUT_INVALID
			martlog.Errorf("input invalid")
			return constant.ERR_HANDLE_INPUT
		}
	}
	return nil
}

// HandleProcess 处理函数
func (p *CreateTaskHandler) HandleProcess() error {
	martlog.Infof("into HandleProcess")
	var err error
	// 拿到任务位置信息，这里其实是预先考虑了分表，将数据插入pos表中ScheduleEndPos对应的位置。
	// 目前我们并没有实现分表，所以 ScheduleEndPos 和 ScheduleBeginPos始终都等于1
	var taskPos *db.TaskPos
	taskPos, err = db.TaskPosNsp.GetTaskPos(db.DB, p.Req.TaskData.TaskType)
	if err != nil {
		p.Resp.Code = constant.ERR_GET_TASK_POS
		martlog.Errorf("db.TaskPosNsp.GetTaskPos err: %s", err.Error())
		return err
	}

	taskCfg, err := db.TaskTypeCfgNsp.GetTaskTypeCfg(db.DB, p.Req.TaskData.TaskType)
	if err != nil {
		p.Resp.Code = constant.ERR_GET_TASK_SET_POS_FROM_DB
		martlog.Errorf("visit t_task_type_cfg err %s", err.Error())
		return err
	}
	scheduleEndPosStr := fmt.Sprintf("%d", taskPos.ScheduleEndPos)
	if err != nil {
		martlog.Errorf("db.TaskPosNsp.GetTaskPos %s", err.Error())
		return err
	}
	var task = new(db.Task)

	// 使用配置表的信息(最大重试次数，以及默认最大重试时间)
	p.Req.TaskData.MaxRetryNum = taskCfg.MaxRetryNum
	p.Req.TaskData.MaxRetryInterval = taskCfg.RetryInterval
	// 创建时的时间，就是一开始的调度顺序，调度查询时会根据orderTime由小到大排序
	p.Req.TaskData.OrderTime = time.Now().Unix()
	if p.Req.TaskData.Priority != nil {
		p.Req.TaskData.OrderTime -= int64(*p.Req.TaskData.Priority)
	}
	// 填充了任务信息
	err = ctrlmodel.FillTaskModel(&p.Req.TaskData, task, scheduleEndPosStr)
	if err != nil {
		p.Resp.Code = constant.ERR_CREATE_TASK
		martlog.Errorf("db.TaskPosNsp.GetTaskPos %s", err.Error())
		return err
	}

	// 创建任务记录
	err = db.TaskNsp.Create(db.DB, p.Req.TaskData.TaskType, scheduleEndPosStr, task)
	if err != nil {
		martlog.Errorf("db.TaskNsp.Create DB %s", err.Error())
		p.Resp.Code = constant.ERR_CREATE_TASK
		return err
	}
	// 填充返回包
	p.Resp.TaskId = task.TaskId
	// 增加缓存
	if err := cache.CreateTask(task); err != nil {
		martlog.Errorf("db.TaskNsp.Create Cache %s", err.Error())
		return nil
	}

	return nil
}
