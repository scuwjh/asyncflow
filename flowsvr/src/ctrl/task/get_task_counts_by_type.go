package task

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/niuniumart/asyncflow/flowsvr/src/constant"
	"github.com/niuniumart/asyncflow/flowsvr/src/db"
	"github.com/niuniumart/asyncflow/taskutils/rpc/model"
	"github.com/niuniumart/gosdk/handler"
	"github.com/niuniumart/gosdk/martlog"
	"github.com/niuniumart/gosdk/tools"
	"net/http"
	"strconv"
)

// GetTaskCountsByTypeHandler 接口处理handler
type GetTaskCountsByTypeHandler struct {
	Req    model.GetTaskCountsByTypeReq
	Resp   model.GetTaskCountsByTypeResp
	UserId string
}

// Description 获取这个任务类型所有状态的
// GetTaskCountsByType 接口
func GetTaskCountsByType(c *gin.Context) {
	var hd GetTaskCountsByTypeHandler
	defer func() {
		hd.Resp.Msg = constant.GetErrMsg(hd.Resp.Code)
		martlog.Infof("GetTaskCountsByType resp %s", tools.GetFmtStr(hd.Resp))
		c.JSON(http.StatusOK, hd.Resp)
	}()
	if err := c.ShouldBind(&hd.Req); err != nil {
		martlog.Errorf("GetTaskCountsByType shouldBind err %s", err.Error())
		hd.Resp.Code = constant.ERR_SHOULD_BIND
		return
	}
	martlog.Infof("GetTaskCountsByType hd.Req %s", tools.GetFmtStr(hd.Req))
	handler.Run(&hd)
}

// HandleInput 参数检查
func (p *GetTaskCountsByTypeHandler) HandleInput() error {
	if p.Req.TaskType == "" {
		martlog.Errorf("input invalid")
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return constant.ERR_HANDLE_INPUT
	}
	return nil
}

// HandleProcess 处理函数
func (p *GetTaskCountsByTypeHandler) HandleProcess() error {

	// 获取 beginPos - endPos
	taskPos, err := db.TaskPosNsp.GetTaskPos(db.DB, p.Req.TaskType)
	if err != nil {
		martlog.Errorf("db.TaskPosNsp.GetRandomSchedulePos %s", err.Error())
		p.Resp.Code = constant.ERR_GET_TASK_SET_POS_FROM_DB
		return err
	}

	// 获取任务数量
	for i := 1; i <= taskPos.ScheduleEndPos; i++ {
		pos := strconv.FormatInt(int64(i), 10)
		count, err := db.TaskNsp.GetAllTaskCount(db.DB, p.Req.TaskType, pos)
		if err != nil {
			martlog.Errorf("db.TaskNsp.GetAllTaskCount %s", err.Error())
			fmt.Println("taskType:", p.Req.TaskType, "pos:", pos, "count:", count)
			p.Resp.Code = constant.ERR_GET_TASK_COUNT_FROM_DB
			return err
		}

		p.Resp.Counts += count
		fmt.Println("taskType:", p.Req.TaskType, "pos:", pos, "count:", count)
	}
	return nil
}
