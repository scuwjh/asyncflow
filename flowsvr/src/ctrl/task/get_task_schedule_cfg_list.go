package task

import (
	"github.com/niuniumart/asyncflow/taskutils/rpc/model"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/niuniumart/asyncflow/flowsvr/src/constant"
	"github.com/niuniumart/asyncflow/flowsvr/src/db"
	"github.com/niuniumart/gosdk/handler"
	"github.com/niuniumart/gosdk/martlog"
	"github.com/niuniumart/gosdk/tools"
)

// GetTaskScheduleCfgListHandler 接口处理handler
type GetTaskScheduleCfgListHandler struct {
	Req    model.GetTaskScheduleCfgListReq
	Resp   model.GetTaskScheduleCfgListResp
	UserId string
}

// GetTaskScheduleCfgList 接口
func GetTaskScheduleCfgList(c *gin.Context) {
	var hd GetTaskScheduleCfgListHandler
	defer func() {
		hd.Resp.Msg = constant.GetErrMsg(hd.Resp.Code)
		c.JSON(http.StatusOK, hd.Resp)
	}()
	// 获取用户Id
	martlog.Infof("GetTaskScheduleCfgList hd.Req %s", tools.GetFmtStr(hd.Req))
	handler.Run(&hd)
}

// HandleInput 参数检查
func (p *GetTaskScheduleCfgListHandler) HandleInput() error {
	return nil
}

// HandleProcess 处理函数
func (p *GetTaskScheduleCfgListHandler) HandleProcess() error {
	cfgList, err := db.TaskTypeCfgNsp.GetTaskTypeCfgList(db.DB)
	if err != nil {
		martlog.Errorf("db.TaskNsp.GetTaskScheduleCfgList %s", err.Error())
		p.Resp.Code = constant.ERR_GET_TASK_CFG_FROM_DB
		return err
	}
	for _, dbCfg := range cfgList {
		var cfg model.TaskScheduleCfg
		fillDbScheduleCfgIntoCtrl(&cfg, dbCfg)
		p.Resp.ScheduleCfgList = append(p.Resp.ScheduleCfgList, &cfg)
	}
	return nil
}

func fillDbScheduleCfgIntoCtrl(cfg *model.TaskScheduleCfg, dbCfg *db.TaskScheduleCfg) {
	cfg.TaskType = dbCfg.TaskType
	cfg.ScheduleLimit = dbCfg.ScheduleLimit
	cfg.ScheduleInterval = dbCfg.ScheduleInterval
	cfg.MaxProcessingTime = dbCfg.MaxProcessingTime
	cfg.MaxRetryNum = dbCfg.MaxRetryNum
	cfg.RetryInterval = dbCfg.RetryInterval
	cfg.MaxRetryInterval = dbCfg.MaxRetryInterval
	cfg.CreateTime = dbCfg.CreateTime
	cfg.ModifyTime = dbCfg.ModifyTime
}
