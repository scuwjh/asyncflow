package task

import (
	"github.com/niuniumart/asyncflow/flowsvr/src/cache"
	"github.com/niuniumart/asyncflow/flowsvr/src/constant"
	"github.com/niuniumart/asyncflow/flowsvr/src/ctrl/ctrlmodel"
	"github.com/niuniumart/asyncflow/flowsvr/src/db"
	"github.com/niuniumart/asyncflow/taskutils/rpc/model"
	"net/http"

	"github.com/niuniumart/gosdk/martlog"

	"github.com/gin-gonic/gin"
	"github.com/niuniumart/gosdk/handler"
)

// GetTaskHandler 接口处理handler
type GetTaskHandler struct {
	Req    model.GetTaskReq
	Resp   model.GetTaskResp
	UserId string
}

// GetTask 接口
func GetTask(c *gin.Context) {
	var hd GetTaskHandler
	defer func() {
		hd.Resp.Msg = constant.GetErrMsg(hd.Resp.Code)
		c.JSON(http.StatusOK, hd.Resp)
	}()
	// 获取用户Id
	hd.UserId = c.Request.Header.Get(constant.HEADER_USERID)
	if err := c.ShouldBind(&hd.Req); err != nil {
		martlog.Errorf("GetTaskHandler shouldBind err %s", err.Error())
		hd.Resp.Code = constant.ERR_SHOULD_BIND
		return
	}
	handler.Run(&hd)
}

// HandleInput 参数检查
func (p *GetTaskHandler) HandleInput() error {
	if p.Req.TaskId == "" {
		martlog.Errorf("input invalid")
		p.Resp.Code = constant.ERR_INPUT_INVALID
		return constant.ERR_HANDLE_INPUT
	}
	return nil
}

// HandleProcess 处理函数
func (p *GetTaskHandler) HandleProcess() error {
	// 先从cache 拿
	// start := time.Now()
	cacheTaskData, err := cache.FindTaskByTaskId(p.Req.TaskId)
	if err != nil {
		martlog.Errorf("cache FindTaskByTaskId %s", err.Error())
		p.Resp.Code = constant.ERR_GET_TASK_INFO
		return err
	}
	// fmt.Println("cache 耗时", time.Since(start))

	var task = &model.TaskData{}
	// 找到了，回去吧
	if cacheTaskData != nil {
		ctrlmodel.FillTaskResp(cacheTaskData, task)
		p.Resp.TaskData = task
		// fmt.Println("走了缓存")
		return nil
	}
	// 没找到，来DB 瞧瞧
	// start := time.Now()
	dbTaskData, err := db.TaskNsp.Find(db.DB, p.Req.TaskId)
	if err != nil {
		martlog.Errorf("db.TaskNsp.GetTask %s", err.Error())
		p.Resp.Code = constant.ERR_GET_TASK_INFO
		return err
	}
	// fmt.Println("DB 耗时", time.Since(start))
	// DB里面有，顺便更新到缓存中
	cache.CreateTask(dbTaskData)

	// 填充，返回
	ctrlmodel.FillTaskResp(dbTaskData, task)
	p.Resp.TaskData = task
	// fmt.Println("走了DB")
	return nil
}
