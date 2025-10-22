package rtm

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/niuniumart/asyncflow/flowsvr/src/config"
	"github.com/niuniumart/asyncflow/flowsvr/src/db"
	"github.com/niuniumart/gosdk/martlog"
	"github.com/niuniumart/gosdk/requestid"
	"runtime"
	"runtime/debug"
	"time"
)

// TaskRuntime 短任务运行时
type TaskRuntime struct {
}

// Run 开始运行
func (p *TaskRuntime) Run() {
	p.run()
}

func (p *TaskRuntime) run() {
	/******  dealLongTimeProcess *******/
	go func() {
		defer func() {
			if err := recover(); err != nil {
				martlog.Errorf("WatTaskRuntime PanicRecover,Error:%s", err)
				// 打印调用栈信息
				debug.PrintStack()
				buf := make([]byte, 2048)
				n := runtime.Stack(buf, false)
				stackInfo := fmt.Sprintf("%s", buf[:n])
				martlog.Errorf("panic stack info %s\n", stackInfo)
			}
			// p.dealLongTimeProcess()
		}()
		requestIDStr := fmt.Sprintf("%+v", uuid.New())
		requestid.Set(requestIDStr)
		p.dealLongTimeProcess()
	}()
	/****** SubTable *******/
	go func() {
		defer func() {
			if err := recover(); err != nil {
				martlog.Errorf("WatTaskRuntime PanicRecover,Error:%s", err)
				// 打印调用栈信息
				debug.PrintStack()
				buf := make([]byte, 2048)
				n := runtime.Stack(buf, false)
				stackInfo := fmt.Sprintf("%s", buf[:n])
				martlog.Errorf("panic stack info %s\n", stackInfo)
			}
		}()

		p.subTable()
	}()

	/******  UpdateSchedulePos *******/
	go func() {
		defer func() {
			if err := recover(); err != nil {
				martlog.Errorf("WatTaskRuntime PanicRecover,Error:%s", err)
				// 打印调用栈信息
				debug.PrintStack()
				buf := make([]byte, 2048)
				n := runtime.Stack(buf, false)
				stackInfo := fmt.Sprintf("%s", buf[:n])
				martlog.Errorf("panic stack info %s\n", stackInfo)
			}
		}()
		p.UpdateSchedulePos()
	}()
}

// UpdateSchedulePos 定时更新任务位置表的POS
func (p *TaskRuntime) UpdateSchedulePos() {
	for {
		martlog.Infof("subTable process")
		// 定期检查是否需要更新任务位置表的POS
		t := time.NewTimer(time.Duration(config.Conf.Task.MoveInterval) * time.Second)
		<-t.C
		/***** step 1: do get lock   *****/
		// lockKey := "subTable"
		/***** step 2: deal long process do  *****/
		martlog.Infof("schedule do dealTimeoutProcessing")
		p.UpdateSchedulePosProcessing()
		/***** step 3: do unlock *****/
	}
}

// SubTable 定时检查是否需要分表
func (p *TaskRuntime) subTable() {
	for {
		martlog.Infof("subTable process")
		// 定期检查是否需要分表
		t := time.NewTimer(time.Duration(config.Conf.Task.SplitInterval) * time.Second)
		<-t.C
		/***** step 1: do get lock   *****/
		// lockKey := "subTable"
		/***** step 2: deal long process do  *****/
		martlog.Infof("schedule do dealTimeoutProcessing")
		p.subTableProcessing()
		/***** step 3: do unlock *****/
	}
}

func (p *TaskRuntime) dealLongTimeProcess() {
	for {
		martlog.Infof("short task deal long time process")
		t := time.NewTimer(time.Duration(config.Conf.Task.LongProcessInterval) * time.Second)
		<-t.C
		/***** step 1: do get lock   *****/
		// lockKey := SHORT_TASK_LONGTIME_DEAL_LOCK_KEY
		/***** step 2: deal long process do  *****/
		martlog.Infof("schedule do dealTimeoutProcessing")
		p.dealTimeoutProcessing()
		/***** step 3: do unlock *****/
	}
}

// UpdateSchedulePosProcessing 定时更新任务位置表的POS
func (p *TaskRuntime) UpdateSchedulePosProcessing() {
	// 1. 读取t_schedule_pos表中的记录，映射为map[string]*db.TaskPos
	taskPosList, err := db.TaskPosNsp.GetTaskPosList(db.DB)
	if err != nil {
		martlog.Errorf("db.TaskPosNsp.GetTaskPosList %s", err.Error())
		return
	}
	// 2. 遍历List
	for _, taskPos := range taskPosList {
		taskType := taskPos.TaskType
		// 2. 统计t_taskType_beginPos表中status=3或4(success或failed)的记录总数
		beginPos := fmt.Sprintf("%d", taskPos.ScheduleBeginPos)
		finishNum, err := db.TaskNsp.GetFinishTaskCount(db.DB, taskType, beginPos)
		if err != nil {
			martlog.Errorf("db.TaskNsp.GetFinishTaskCount %s", err.Error())
			return
		}
		// 记录总量count
		count, err := db.TaskNsp.GetAllTaskCount(db.DB, taskType, beginPos)
		if err != nil {
			martlog.Errorf("db.TaskNsp.GetAllTaskCount %s", err.Error())
		}

		// 如果相等，说明该表中的任务全部被调度执行过了，begin_pos < end_pos，那就需要移动begin_pos
		if finishNum == count && taskPos.ScheduleBeginPos < taskPos.ScheduleEndPos {
			// 将task_pos.schedule_begin_pos自增1，并更新至数据库
			taskPos.ScheduleBeginPos++
			err = db.TaskPosNsp.Save(db.DB, taskPos)
			if err != nil {
				martlog.Errorf("db.TaskPosNsp.Save %s", err.Error())
			}
		}
	}
}

func (p *TaskRuntime) subTableProcessing() {
	// 1. 获取位置信息表中所有记录
	taskPosList, err := db.TaskPosNsp.GetTaskPosList(db.DB)
	if err != nil {
		martlog.Errorf("db.TaskPosNsp.GetTaskPosList %s", err.Error())
		return
	}

	// 2. 遍历切片，统计t_taskType_endPos表的记录总数count
	for _, taskPos := range taskPosList {
		taskType := taskPos.TaskType
		endPos := fmt.Sprintf("%d", taskPos.ScheduleEndPos)
		// GetAllTaskCount 获取所有任务数量
		count, err := db.TaskNsp.GetAllTaskCount(db.DB, taskType, endPos)
		if err != nil {
			martlog.Errorf("db.TaskNsp.GetAllTaskCount %s", err.Error())
			return
		}

		// 如果count大于等于配置的最大行数，则需要分表
		if count >= config.Conf.Task.TableMaxRows {
			// 2.1 创建一个新的表t_task_type_endPos+1
			nextPos := db.TaskPosNsp.GetNextPos(endPos)
			err = db.TaskNsp.CreateTable(db.DB, taskType, nextPos)
			if err != nil {
				martlog.Errorf("db.TaskNsp.CreateNextTable %s", err.Error())
				return
			}
			// 2.2 并将ScheduleEndPos自增1。
			taskPos.ScheduleEndPos++
			// 2.3 TaskPos更新到数据库中
			err = db.TaskPosNsp.Save(db.DB, taskPos)
			if err != nil {
				martlog.Errorf("db.TaskPosNsp.Save %s", err.Error())
			}
		}
	}
}

func (p *TaskRuntime) dealTimeoutProcessing() {
	taskTypeCfgList, err := db.TaskTypeCfgNsp.GetTaskTypeCfgList(db.DB)
	if err != nil {
		martlog.Errorf("visit t_task_type_cfg err %s", err.Error())
		return
	}
	for _, taskTypeCfg := range taskTypeCfgList {
		p.dealTimeoutProcessingWithType(taskTypeCfg)
	}
}

func (p *TaskRuntime) dealTimeoutProcessingWithType(taskCfg *db.TaskScheduleCfg) {
	taskPos, err := db.TaskPosNsp.GetTaskPos(db.DB, taskCfg.TaskType)
	if err != nil {
		martlog.Errorf("db.TaskPosNsp.GetTaskPos err %s", err.Error())
		return
	}

	maxProcessTime := config.Conf.Task.MaxProcessTime
	if int64(taskCfg.MaxProcessingTime) == 0 {
		maxProcessTime = taskCfg.MaxProcessingTime
	}
	taskList, err := db.TaskNsp.GetLongTimeProcessing(db.DB, taskCfg.TaskType,
		fmt.Sprintf("%d", taskPos.ScheduleBeginPos), maxProcessTime, 1000)
	if err != nil {
		martlog.Errorf("get long time processing err %s", err.Error())
		return
	}
	for _, task := range taskList {
		// 比较重试次数，如果>=最大重试次数，就设置为失败
		if task.CrtRetryNum >= taskCfg.MaxRetryNum {

			// 执行时间超过最大执行时间，并且重试次数超过最大重试次数，设置为失败
			err = db.TaskNsp.SetStatus(db.DB, task.TaskId, db.TASK_STATUS_FAILED)
			if err != nil {
				martlog.Errorf("deal long time task, save task err %s", err.Error())
				continue
			}
			continue
		}

		// 重试次数+1，设置为待执行
		err = db.TaskNsp.SetStatusAndRetryNumIncrement(db.DB, task.TaskId, db.TASK_STATUS_PENDING)
		if err != nil {
			martlog.Errorf("deal long time task, save task err %s", err.Error())
			continue
		}
	}
}
