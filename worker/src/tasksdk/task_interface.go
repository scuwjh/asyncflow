// Package Task inft && schedule
package tasksdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/niuniumart/asyncflow/taskutils/constant"
	"github.com/niuniumart/asyncflow/taskutils/rpc"
	"github.com/niuniumart/asyncflow/taskutils/rpc/model"
	"github.com/niuniumart/gosdk/martlog"
	"github.com/niuniumart/gosdk/tools"
	"time"
)

var taskHandlerMap = make(map[string]*TaskHandler, 0)

// RegisterHandler func RegisterHandler
func RegisterHandler(handler *TaskHandler) {
	taskHandlerMap[handler.TaskType] = handler
}

// GetHandler func get handler
func GetHandler(taskType string) (*TaskHandler, int) {
	if _, ok := taskHandlerMap[taskType]; ok {
		return taskHandlerMap[taskType], 0
	}
	return nil, -1
}

// TaskHandler struct TaskHandler
type TaskHandler struct {
	TaskType string
	NewProc  func() TaskIntf
}

// TaskIntf Task interface
type TaskIntf interface {
	ContextLoad() error
	HandleProcess() error
	SetTask() error
	HandleFinish()
	HandleFinishError() error
	Base() *TaskBase
	CreateTask() (string, error)
	HandleFailedMust() error
}

// TaskBase struct TaskBase
type TaskBase struct {
	Id               uint64
	TaskId           string
	UserId           string
	Status           int
	TaskType         string
	TaskStage        string
	TaskContext      string
	CrtRetryNum      int
	MaxRetryNum      int
	MaxRetryInterval int
	ScheduleLog      *ScheduleLog
	ContextIntf      interface{}
	Priority         int64
	OrderTime        int64
	CreateTime       time.Time
	ModifyTime       time.Time
}

// ScheduleLog struct ScheduleLog
type ScheduleLog struct {
	LastData     ScheduleData
	HistoryDatas []ScheduleData
}

// ScheduleData struct ScheduleData
type ScheduleData struct {
	TraceId string
	ErrMsg  string
	Cost    string
}

// Base func get base struct
func (p *TaskBase) Base() *TaskBase {
	return p
}

// SetContextLocal func set context local
func (p *TaskBase) SetContextLocal(data interface{}) {
	p.ContextIntf = data
}

// GetTaskInfoFromStorage func get task info from rpc
func GetTaskInfoFromStorage(storage *model.TaskData) (TaskIntf, error) {
	handler, ok := taskHandlerMap[storage.TaskType]
	if !ok {
		errMsg := fmt.Sprintf("hard error : tasktype %s not exist in taskHandlerMap", storage.TaskType)
		martlog.Errorf(errMsg)
		return nil, errors.New(errMsg)
	}
	t := handler.NewProc()
	t.Base().TaskId = storage.TaskId
	t.Base().TaskType = storage.TaskType
	t.Base().TaskStage = storage.TaskStage
	t.Base().TaskContext = storage.TaskContext
	t.Base().Status = storage.Status
	if storage.Priority != nil {
		var priority = int64(*storage.Priority)
		t.Base().Priority = priority
	}
	t.Base().UserId = storage.UserId
	t.Base().CrtRetryNum = storage.CrtRetryNum
	t.Base().MaxRetryNum = storage.MaxRetryNum
	t.Base().MaxRetryInterval = storage.MaxRetryInterval
	t.Base().CreateTime = storage.CreateTime
	t.Base().ModifyTime = storage.ModifyTime
	t.Base().MaxRetryNum = storage.MaxRetryNum
	t.Base().CrtRetryNum = storage.CrtRetryNum
	martlog.Infof("schedule info %s", storage.ScheduleLog)
	var info ScheduleLog
	_ = json.Unmarshal([]byte(storage.ScheduleLog), &info)
	t.Base().ScheduleLog = &info
	return t, nil
}

// HandleFinishError handle finish error
func (p *TaskBase) HandleFinishError() error {
	return nil
}

// HandleFailedMust if err, then change status from failed to processing
func (p *TaskBase) HandleFailedMust() error {
	return nil
}

// HandleFinishMust handle finish HandleFinishMust
func (p *TaskBase) HandleFinishMust() error {
	return nil
}

// ContextLoad context load
func (p *TaskBase) ContextLoad() error {
	return nil
}

// SetTask set task
func (p *TaskBase) SetTask() error {
	var shortRpc = rpc.TaskRpc{
		Host: taskSvrHost,
	}
	var taskData = model.TaskData{
		TaskType:         p.TaskType,
		TaskId:           p.TaskId,
		TaskStage:        p.TaskStage,
		TaskContext:      p.TaskContext,
		ScheduleLog:      tools.GetFmtStr(p.ScheduleLog),
		UserId:           p.UserId,
		CrtRetryNum:      p.CrtRetryNum,
		MaxRetryNum:      p.MaxRetryNum,
		MaxRetryInterval: p.MaxRetryInterval,
		Status:           p.Status,
		OrderTime:        p.OrderTime,
	}
	if p.ContextIntf != nil {
		context, err := json.Marshal(p.ContextIntf)
		if err != nil {
			martlog.Errorf("json marshal contextInft err %s", err.Error())
			return err
		}
		taskData.TaskContext = string(context)
	}
	var req = &model.SetTaskReq{
		TaskData: taskData,
	}
	_, err := shortRpc.SetTask(req)
	if err != nil {
		martlog.Errorf("set task rpc err %s", err.Error())
		return err
	}
	return nil
}

// CreateTask func create task
func (p *TaskBase) CreateTask() (string, error) {
	var taskData model.TaskData
	taskData.Status = p.Status
	if taskData.Status == 0 {
		taskData.Status = int(constant.TASK_STATUS_PENDING)
	}
	taskData.TaskType = p.TaskType
	taskData.TaskContext = p.TaskContext
	taskData.UserId = p.UserId
	taskData.MaxRetryNum = p.MaxRetryNum
	taskData.MaxRetryInterval = p.MaxRetryInterval
	var shortRpc = rpc.TaskRpc{
		Host: taskSvrHost,
	}
	var createTaskReq = &model.CreateTaskReq{TaskData: taskData}
	resp, err := shortRpc.CreateTask(createTaskReq)
	if err != nil {
		martlog.Errorf("shortRpc.CreateTask err %s", err.Error())
		return "", err
	}
	return resp.TaskId, nil
}

// GetTask func get task
func GetTask(taskId string) (TaskIntf, error) {
	var shortRpc = rpc.TaskRpc{
		Host: taskSvrHost,
	}
	var getTaskReq = &model.GetTaskReq{TaskId: taskId}
	resp, err := shortRpc.GetTask(getTaskReq)
	if err != nil {
		martlog.Errorf("shortRpc.GetTask err %s", err.Error())
		return nil, err
	}
	return GetTaskInfoFromStorage(resp.TaskData)
}
