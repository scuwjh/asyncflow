// Package model for list model
package model

const (
	SCHEDULE_TASK_LIMIT = 100
)

const (
	version                      = "/v1"
	GET_TASK_LIST_SUFFIX         = version + "/get_task_list"
	GET_TASK_SUFFIX              = version + "/get_task"
	HOLD_TASKS                   = version + "/hold_tasks"
	SET_TASK_SUFFIX              = version + "/set_task"
	CREATE_TASK_SUFFIX           = version + "/create_task"
	GET_TASK_SCHEDULE_CFG_SUFFIX = version + "/get_task_schedule_cfg_list"
)

const (
	STATS_TASK_COUNT = "/stats_task_count"
)
