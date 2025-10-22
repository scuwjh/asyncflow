package model

import (
	"time"
)

// RespComm 通用的响应消息
type RespComm struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// CreateTaskReq 请求消息
type CreateTaskReq struct {
	TaskData TaskData `json:"task_data"`
}

// CreateTaskResp 响应消息
type CreateTaskResp struct {
	RespComm
	TaskId string `json:"task_id"`
}

// GetTaskListReq 请求消息
type GetTaskListReq struct {
	TaskType string `json:"task_type" form:"task_type" binding:"required"`
	Status   int    `json:"status" form:"status" binding:"required"`
	Limit    int    `json:"limit" form:"limit" binding:"required"`
}

// GetTaskListResp 响应消息
type GetTaskListResp struct {
	RespComm
	TaskList []*TaskData `json:"task_list"`
}

// HoldTasksReq 请求消息
type HoldTasksReq struct {
	TaskType string `json:"task_type" form:"task_type"`
	Limit    int    `json:"limit" form:"limit"`
}

// HoldTasksResp 响应消息
type HoldTasksResp struct {
	RespComm
	TaskList []*TaskData `json:"task_list"`
}

// GetTaskCountsByTypeReq 请求消息
type GetTaskCountsByTypeReq struct {
	TaskType string `json:"task_type" form:"task_type"`
}

// GetTaskCountsByTypeResp 响应消息
type GetTaskCountsByTypeResp struct {
	RespComm
	Counts int `json:"counts"`
}

// GetTaskReq 请求消息
type GetTaskReq struct {
	TaskId string `json:"task_id" form:"task_id"`
}

// GetTaskResp 响应消息
type GetTaskResp struct {
	RespComm
	TaskData *TaskData `json:"task_data"`
}

// GetTaskCountByStatusReq 请求消息
type GetTaskCountByStatusReq struct {
	TaskType string `json:"task_type" form:"task_type"`
	Status   int    `json:"status" form:"status"`
}

// GetTaskCountByStatusResp 响应消息
type GetTaskCountByStatusResp struct {
	RespComm
	Count int `json:"count"`
}

// GetTaskScheduleCfgListReq 获取任务配置信息 请求体（空）
type GetTaskScheduleCfgListReq struct {
}

// GetTaskScheduleCfgListResp 获取任务配置信息 响应体
type GetTaskScheduleCfgListResp struct {
	RespComm
	ScheduleCfgList []*TaskScheduleCfg `json:"task_schedule_cfg_list"`
}

// RegisterTaskReq 任务注册接口 请求体
type RegisterTaskReq struct {
	TaskType string `json:"task_type" form:"task_type"`
}

// RegisterTaskResp 任务注册接口 响应体
type RegisterTaskResp struct {
	RespComm
}

// TaskScheduleCfg 任务调度信息
type TaskScheduleCfg struct {
	TaskType          string     `json:"task_type"`
	ScheduleLimit     int        `json:"schedule_limit"`
	ScheduleInterval  int        `json:"schedule_interval"`
	MaxProcessingTime int64      `json:"max_processing_time"`
	MaxRetryNum       int        `json:"max_retry_num"`
	RetryInterval     int        `json:"retry_interval"`
	MaxRetryInterval  int        `json:"max_retry_interval"`
	CreateTime        *time.Time `json:"create_time"`
	ModifyTime        *time.Time `json:"modify_time"`
}

// SetTaskReq 请求消息
type SetTaskReq struct {
	TaskData `json:"task_data"`
}

// SetTaskResp 响应消息
type SetTaskResp struct {
	RespComm
}

// TaskData 任务调度数据
type TaskData struct {
	UserId           string    `json:"user_id"`
	TaskId           string    `json:"task_id"`
	TaskType         string    `json:"task_type"`
	TaskStage        string    `json:"task_stage"`
	Status           int       `json:"status"`
	Priority         *int      `json:"priority"`
	CrtRetryNum      int       `json:"crt_retry_num"`
	MaxRetryNum      int       `json:"max_retry_num"`
	MaxRetryInterval int       `json:"max_retry_interval"`
	ScheduleLog      string    `json:"schedule_log"`
	TaskContext      string    `json:"context"`
	OrderTime        int64     `json:"order_time"`
	CreateTime       time.Time `json:"create_time"`
	ModifyTime       time.Time `json:"modify_time"`
}
