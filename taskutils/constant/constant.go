package constant

type TaskEnum int

const (
	TASK_STATUS_PENDING    TaskEnum = 1
	TASK_STATUS_PROCESSING TaskEnum = 2
	TASK_STATUS_SUCC       TaskEnum = 3
	TASK_STATUS_FAILED     TaskEnum = 4
)
