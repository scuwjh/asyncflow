package constant

import (
	"errors"
	"fmt"
)

var (
	ERR_HANDLE_INPUT = errors.New("handle input error")
)

var (
	SUCCESS           = 0
	ERR_INPUT_INVALID = 8020
	ERR_SHOULD_BIND   = 8021
	ERR_JSON_MARSHAL  = 8022

	ERR_GET_TASK_INFO                  = 8035
	ERR_GET_TASK_HANDLE_PROCESS        = 8036
	ERR_CREATE_TASK                    = 8037
	ERR_GET_TASK_LIST_FROM_DB          = 8038
	ERR_GET_TASK_SET_POS_FROM_DB       = 8039
	ERR_INCREASE_CRT_RETRY_NUM         = 8040
	ERR_SET_TASK                       = 8041
	ERR_GET_TASK_POS                   = 8042
	ERR_GET_PROCESSING_COUNT           = 8043
	ERR_SET_USER_PRIORITY              = 8044
	ERR_GET_TASK_CFG_FROM_DB           = 8045
	ERR_GET_TASK_COUNT_FROM_DB         = 8046
	ERR_REGISTER_TASK_CREATE_NEW_TABLE = 8047
	ERR_REGISTER_TASK_INSERT_RECORD    = 8048
)

var errMsgDic = map[int]string{
	SUCCESS:                            "ok",
	ERR_INPUT_INVALID:                  "input invalid",
	ERR_SHOULD_BIND:                    "should bind failed",
	ERR_JSON_MARSHAL:                   "json marshal failed",
	ERR_GET_TASK_INFO:                  "get task info failed",
	ERR_GET_TASK_HANDLE_PROCESS:        "get task handle process Failed",
	ERR_CREATE_TASK:                    "create task failed",
	ERR_GET_TASK_LIST_FROM_DB:          "get task list from db failed",
	ERR_GET_TASK_SET_POS_FROM_DB:       "get task set pos from db failed",
	ERR_SET_TASK:                       "set task failed",
	ERR_INCREASE_CRT_RETRY_NUM:         "increase crt retry num failed",
	ERR_GET_TASK_POS:                   "get task pos failed",
	ERR_GET_PROCESSING_COUNT:           "get processing count failed",
	ERR_SET_USER_PRIORITY:              "set user priority failed",
	ERR_GET_TASK_CFG_FROM_DB:           "get task cfg failed",
	ERR_GET_TASK_COUNT_FROM_DB:         "get task count from db failed",
	ERR_REGISTER_TASK_CREATE_NEW_TABLE: "register task create new table failed",
	ERR_REGISTER_TASK_INSERT_RECORD:    "register task insert record failed",
}

// GetErrMsg 获取错误描述
func GetErrMsg(code int) string {
	if msg, ok := errMsgDic[code]; ok {
		return msg
	}
	return fmt.Sprintf("unknown error code %d", code)
}
