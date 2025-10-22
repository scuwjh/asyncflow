package ctrlmodel

import (
	"github.com/niuniumart/asyncflow/flowsvr/src/db"
	"github.com/niuniumart/asyncflow/taskutils/rpc/model"
)

/**
 * @Description: 根据请求转为任务db对象
 * @receiver sTask 请求数据
 * @return *db.Task 任务db对象结果
 * @return error 错误信息
 */

// Fill db Task
func FillTaskModel(sTask *model.TaskData, task *db.Task, scheduleEndPosStr string) error {
	task.UserId = sTask.UserId
	if sTask.TaskId == "" {
		task.TaskId = db.TaskNsp.GenTaskId(sTask.TaskType, scheduleEndPosStr)
		task.Status = int(db.TASK_STATUS_PENDING)
	} else {
		task.TaskId = sTask.TaskId
		task.Status = sTask.Status
	}
	task.TaskType = sTask.TaskType
	task.UserId = sTask.UserId
	task.ScheduleLog = sTask.ScheduleLog
	task.TaskStage = sTask.TaskStage
	task.CrtRetryNum = sTask.CrtRetryNum
	task.MaxRetryNum = sTask.MaxRetryNum
	task.MaxRetryInterval = sTask.MaxRetryInterval
	task.TaskContext = sTask.TaskContext
	return nil
}

/**
 * @Description: 任务db对象转返回
 * @receiver sTask 返回数据
 * @return *db.Task 任务db对象结果
 * @return error 错误信息
 */

// FillTaskResp resp
func FillTaskResp(task *db.Task, sTask *model.TaskData) {
	sTask.UserId = task.UserId
	sTask.TaskId = task.TaskId
	sTask.TaskType = task.TaskType
	sTask.Status = task.Status
	sTask.ScheduleLog = task.ScheduleLog
	var priority = task.Priority
	sTask.Priority = &priority
	sTask.TaskStage = task.TaskStage
	sTask.CrtRetryNum = task.CrtRetryNum
	sTask.MaxRetryNum = task.MaxRetryNum
	sTask.MaxRetryInterval = task.MaxRetryInterval
	sTask.TaskContext = task.TaskContext
	sTask.CreateTime = task.CreateTime
	sTask.ModifyTime = task.ModifyTime
}
