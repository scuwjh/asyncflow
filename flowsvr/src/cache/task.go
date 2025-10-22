package cache

import (
	"encoding/json"
	"fmt"
	"github.com/niuniumart/asyncflow/flowsvr/src/db"
	"github.com/redis/go-redis/v9"
)

func CreateTask(task *db.Task) error {
	value, err := json.Marshal(task)
	if err != nil {
		return err
	}
	key := prefix + fmt.Sprintf("%s", task.TaskId)
	return rdb.Set(key, string(value), expireTime)
}

func FindTaskByTaskId(taskId string) (*db.Task, error) {
	key := prefix + fmt.Sprintf("%s", taskId)
	value, err := rdb.Get(key)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	if value == "" {
		return nil, nil
	}
	task := &db.Task{}
	err = json.Unmarshal([]byte(value), task)
	return task, err // 返回task
}

// OnceDeleteTask 单次删除处理
func OnceDeleteTask(taskId string) {
	key := prefix + fmt.Sprintf("%s", taskId)

	rdb.Delete(key)
}
