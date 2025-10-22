package db

import (
	"fmt"
	"strconv"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/niuniumart/gosdk/martlog"
)

var TaskPosNsp TaskPos

// TaskPos taskPos
type TaskPos struct {
	Id               uint64
	TaskType         string
	ScheduleBeginPos int
	ScheduleEndPos   int
	CreateTime       *time.Time
	ModifyTime       *time.Time
}

// TableName 表名
func (p *TaskPos) TableName() string {
	return "t_schedule_pos"
}

// Create 创建记录
func (p *TaskPos) Create(db *gorm.DB, task *TaskPos) error {
	err := db.Table(p.TableName()).Create(task).Error
	return err
}

// Save 保存记录
func (p *TaskPos) Save(db *gorm.DB, task *TaskPos) error {
	err := db.Table(p.TableName()).Save(task).Error
	return err
}

// GetTaskPos 获取记录
func (p *TaskPos) GetTaskPos(db *gorm.DB, taskType string) (*TaskPos, error) {
	var taskPos = new(TaskPos)
	err := db.Table(p.TableName()).Where("task_type = ?", taskType).First(&taskPos).Error

	if err == gorm.ErrRecordNotFound {
		// 没有记录，自动创建记录
		err = db.Table(p.TableName()).Create(&TaskPos{ScheduleBeginPos: 1, ScheduleEndPos: 1, TaskType: taskType}).Error
	}

	if err != nil {
		return nil, err
	}
	return taskPos, nil
}

// GetNextPos 获取下一个调度指针
func (p *TaskPos) GetNextPos(pos string) string {
	posInt, err := strconv.Atoi(pos)
	if err != nil {
		martlog.Errorf("pos %s maybe not int", pos)
		return ""
	}
	return fmt.Sprintf("%d", posInt+1)
}

// GetTaskPosList 获取记录列表
func (p *TaskPos) GetTaskPosList(db *gorm.DB) ([]*TaskPos, error) {
	var taskList = make([]*TaskPos, 0)
	err := db.Table(p.TableName()).Find(&taskList).Error
	if err != nil {
		return nil, err
	}
	return taskList, nil
}

// BeforeCreate 创建之前的回调函数
func (this *TaskPos) BeforeCreate(scope *gorm.Scope) error {
	now := time.Now()
	scope.SetColumn("create_time", now)
	scope.SetColumn("modify_time", now)
	return nil
}
