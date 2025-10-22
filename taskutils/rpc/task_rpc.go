package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/niuniumart/asyncflow/taskutils/rpc/model"
	"github.com/niuniumart/gosdk/martlog"
	"github.com/niuniumart/gosdk/tools"
	"net/http"
)

// TaskRpc struct TaskRpc
type TaskRpc struct {
	Host string
}

// CreateTask func CreateTask
func (p *TaskRpc) CreateTask(reqBody *model.CreateTaskReq) (
	*model.CreateTaskResp, error) {

	var respData = &model.CreateTaskResp{}
	var headerDic = map[string]string{
		"Content-Type": "application/json;charset=utf-8",
	}
	err := sendRequest(TaskClient, http.MethodPost, p.Host, model.CREATE_TASK_SUFFIX,
		nil, headerDic, reqBody, respData, false)
	if err != nil {
		return respData, err
	}
	if respData.Code != 0 {
		errMsg := fmt.Sprintf("Task rpc resp code %d", respData.Code)
		martlog.Errorf(errMsg)
		return respData, errors.New(errMsg)
	}
	return respData, nil
}

func sendRequest(client *http.Client, method, host, urlSuffix string, queryStrDic,
	headerDic map[string]string,
	reqBody interface{}, respData interface{}, disablePrint bool) error {
	var trigger = tools.HttpTrigger{
		Method: method,
		Url:    fmt.Sprintf("%s%s", host, urlSuffix),
	}
	respStr, err := trigger.SendJsonRequest(TaskClient, queryStrDic, headerDic, reqBody)
	if err != nil {
		errMsg := fmt.Sprintf("trigger.SendJsonRequest, [err:%s]", err)
		return errors.New(errMsg)
	}
	// deal with resp
	if respData != nil {
		err = json.Unmarshal(respStr, respData)
		if err != nil {
			errMsg := fmt.Sprintf("unmarshal resp failed, %s", err)
			return errors.New(errMsg)
		}
	}
	return nil

}

// SetTask func SetTask
func (p *TaskRpc) SetTask(reqBody *model.SetTaskReq) (
	*model.SetTaskResp, error) {
	var respData = &model.SetTaskResp{}
	var headerDic = map[string]string{
		"Content-Type": "application/json;charset=utf-8",
	}
	err := sendRequest(TaskClient, http.MethodPost, p.Host, model.SET_TASK_SUFFIX,
		nil, headerDic, reqBody, respData, false)
	if err != nil {
		return respData, err
	}
	if respData.Code != 0 {
		errMsg := fmt.Sprintf("Task rpc resp code %d", respData.Code)
		martlog.Errorf(errMsg)
		return respData, errors.New(errMsg)
	}
	return respData, nil
}

// HoldTasks func for hold tasks
func (p *TaskRpc) HoldTasks(reqBody *model.HoldTasksReq) (*model.HoldTasksResp, error) {
	var queryStrDic = map[string]string{
		"task_type": reqBody.TaskType,
		"limit":     fmt.Sprintf("%d", reqBody.Limit),
	}
	var respData = &model.HoldTasksResp{}
	err := sendRequest(TaskClient, http.MethodPost, p.Host, model.HOLD_TASKS,
		queryStrDic, nil, reqBody, respData, false)
	if err != nil {
		return respData, err
	}
	if respData.Code != 0 {
		errMsg := fmt.Sprintf("Task rpc resp code %d", respData.Code)
		martlog.Errorf(errMsg)
		return respData, errors.New(errMsg)
	}
	return respData, nil
}

// GetTaskList func GetTaskList
func (p *TaskRpc) GetTaskList(reqBody *model.GetTaskListReq) (*model.GetTaskListResp, error) {
	var queryStrDic = map[string]string{
		"taskType": reqBody.TaskType,
		"status":   fmt.Sprintf("%d", reqBody.Status),
		"limit":    fmt.Sprintf("%d", reqBody.Limit),
	}
	var respData = &model.GetTaskListResp{}
	err := sendRequest(TaskClient, http.MethodGet, p.Host, model.GET_TASK_LIST_SUFFIX,
		queryStrDic, nil, reqBody, respData, false)
	if err != nil {
		return respData, err
	}
	if respData.Code != 0 {
		errMsg := fmt.Sprintf("Task rpc resp code %d", respData.Code)
		martlog.Errorf(errMsg)
		return respData, errors.New(errMsg)
	}
	return respData, nil
}

// GetTask func GetTask
func (p *TaskRpc) GetTask(reqBody *model.GetTaskReq) (*model.GetTaskResp, error) {
	var queryStrDic = map[string]string{
		"taskId": reqBody.TaskId,
	}
	var respData = &model.GetTaskResp{}
	err := sendRequest(TaskClient, http.MethodGet, p.Host, model.GET_TASK_SUFFIX,
		queryStrDic, nil, reqBody, respData, false)
	if err != nil {
		return respData, err
	}
	if respData.Code != 0 {
		errMsg := fmt.Sprintf("Task rpc resp code %d", respData.Code)
		martlog.Errorf(errMsg)
		return respData, errors.New(errMsg)
	}
	return respData, nil
}

// GetTaskScheduleCfgList func GetTaskScheduleCfgList
func (p *TaskRpc) GetTaskScheduleCfgList() (*model.GetTaskScheduleCfgListResp, error) {
	var trigger = tools.HttpTrigger{
		Method: http.MethodGet,
		Url:    fmt.Sprintf("%s%s", p.Host, model.GET_TASK_SCHEDULE_CFG_SUFFIX),
	}
	var reqBody = &model.GetTaskScheduleCfgListReq{}
	var respData = &model.GetTaskScheduleCfgListResp{}
	var headerDic = map[string]string{
		"Content-Type": "application/json;charset=utf-8",
	}
	respStr, err := trigger.SendJsonRequest(TaskClient, nil, headerDic, reqBody)
	if err != nil {
		errMsg := fmt.Sprintf("trigger.SendJsonRequest, [err:%s]", err)
		return respData, errors.New(errMsg)
	}
	// deal with resp
	err = json.Unmarshal(respStr, respData)
	if err != nil {
		errMsg := fmt.Sprintf("unmarshal resp failed, %s", err)
		return respData, errors.New(errMsg)
	}
	if respData.Code != 0 {
		errMsg := fmt.Sprintf("Task rpc resp code %d", respData.Code)
		martlog.Errorf(errMsg)
		return respData, errors.New(errMsg)
	}
	return respData, nil
}
