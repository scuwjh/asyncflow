package tasksdk

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/niuniumart/asyncflow/taskutils/constant"
	"github.com/niuniumart/asyncflow/taskutils/rpc"
	"github.com/niuniumart/asyncflow/taskutils/rpc/model"
	"github.com/niuniumart/gosdk/martlog"
	"github.com/niuniumart/gosdk/redislock"
	"github.com/niuniumart/gosdk/tools"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

const (
	DEFAULT_TIME_INTERVAL = 20 // for second 20s
)

const (
	MAX_ERR_MSG_LEN = 256
)

var (
	taskSvrHost string // new is host: for example http://127.0.0.1:41555
	lockClient  *redislock.Client
)

// InitSvr task svr host
func InitSvr(taskServerHost, lockServerHost, lockPassword string) {
	taskSvrHost = taskServerHost
	lockClient = redislock.NewClient(lockServerHost, lockPassword)
}

// TaskMgr struct short task mgr
type TaskMgr struct {
	IntervalTime  time.Duration
	TaskType      string
	ScheduleLimit int
}

var once sync.Once

var scheduleCfgDic map[string]*model.TaskScheduleCfg

func init() {
	scheduleCfgDic = make(map[string]*model.TaskScheduleCfg, 0)
}

// CycleReloadCfg func cycle reload cfg
func CycleReloadCfg() {
	for {
		now := time.Now()
		intervalTime := time.Second * DEFAULT_TIME_INTERVAL
		next := now.Add(intervalTime)
		martlog.Infof("schedule load cfg")
		sub := next.Sub(now)
		t := time.NewTimer(sub)
		<-t.C
		LoadCfg()
	}
}

// LoadCfg func load cfg
func LoadCfg() error {
	cfgList, err := taskRpc.GetTaskScheduleCfgList()
	if err != nil {
		martlog.Errorf("reload task schedule cfg err %s", err.Error())
		return err
	}
	for _, cfg := range cfgList.ScheduleCfgList {
		scheduleCfgDic[cfg.TaskType] = cfg
	}
	return nil
}

// Schedule func schedule
func (p *TaskMgr) Schedule() {
	taskRpc.Host = taskSvrHost
	once.Do(func() {
		// 初始化加载任务配置信息表
		if err := LoadCfg(); err != nil {
			msg := "load task cfg schedule err" + err.Error()
			martlog.Errorf(msg)
			fmt.Println(msg)
			os.Exit(1)
		}
		go func() {
			CycleReloadCfg()
		}()
	})
	for {
		cfg, ok := scheduleCfgDic[p.TaskType]
		if !ok {
			martlog.Errorf("scheduleCfgDic %s, not have taskType %s", tools.GetFmtStr(scheduleCfgDic), p.TaskType)
			return
		}
		intervalTime := time.Second * time.Duration(cfg.ScheduleInterval)
		if cfg.ScheduleInterval == 0 {
			intervalTime = time.Second * DEFAULT_TIME_INTERVAL
		}
		// 前后波动500ms[0,501)
		step := RandNum(501)
		// 加上波动的时间
		intervalTime += time.Duration(step) * time.Millisecond
		t := time.NewTimer(intervalTime)
		<-t.C
		martlog.Infof("taskType %s intervalTime %v", p.TaskType, intervalTime)
		go func() {
			defer func() {
				if err := recover(); err != nil {
					martlog.Errorf("In PanicRecover,Error:%s", err)
					// 打印调用栈信息
					debug.PrintStack()
					buf := make([]byte, 2048)
					n := runtime.Stack(buf, false)
					stackInfo := fmt.Sprintf("%s", buf[:n])
					martlog.Errorf("panic stack info %s\n", stackInfo)
				}
			}()
			p.schedule()
		}()
	}
}

// 调度逻辑所在
func (p *TaskMgr) schedule() {
	defer func() {
		if err := recover(); err != nil {
			martlog.Errorf("In PanicRecover,Error:%s", err)
			// 打印调用栈信息
			debug.PrintStack()
			buf := make([]byte, 2048)
			n := runtime.Stack(buf, false)
			stackInfo := fmt.Sprintf("%s", buf[:n])
			martlog.Errorf("panic stack info %s\n", stackInfo)
		}
	}()

	// 阻塞模式，如果没有抢到锁，就会阻塞直到抢锁成功（默认阻塞最长时间为5秒）
	mutex := redislock.NewRedisLock(p.TaskType, lockClient, redislock.WithBlock(), redislock.WithWatchDogMode(), redislock.WithExpireSeconds(3))
	if err := mutex.Lock(context.Background()); err != nil {
		martlog.Errorf("RedisLock lock err %s", err.Error())
		return // 没有抢到锁，直接返回
	}

	// 占据一批任务
	taskIntfList, err := p.hold()

	// 释放锁
	if err := mutex.Unlock(context.Background()); err != nil {
		martlog.Errorf("RedisLock unlock err %s", err.Error())
		return
	}

	if err != nil {
		martlog.Errorf("p.hold err %s", err.Error())
		return
	}
	martlog.Infof("End hold.")
	if len(taskIntfList) == 0 {
		martlog.Infof("no task to deal")
		return
	}
	fmt.Println("拉取任务成功，开始执行任务......")
	// 获取这个任务类型的配置
	cfg, ok := scheduleCfgDic[p.TaskType]
	if !ok {
		martlog.Errorf("scheduleCfgDic %s, not have taskType %s", tools.GetFmtStr(scheduleCfgDic), p.TaskType)
		return
	}
	martlog.Infof("will do %d num task", len(taskIntfList))
	// 并发执行每个任务
	for _, taskIntf := range taskIntfList {
		taskInterface := taskIntf
		go func() {
			defer func() {
				if reErr := recover(); reErr != nil {
					martlog.Errorf("In PanicRecover,Error:%s", reErr)
					// 打印调用栈信息
					debug.PrintStack()
					buf := make([]byte, 2048)
					n := runtime.Stack(buf, false)
					stackInfo := fmt.Sprintf("%s", buf[:n])
					martlog.Errorf("panic stack info %s\n", stackInfo)
				}
			}()
			run(taskInterface, cfg)
		}()
	}
}

var taskRpc rpc.TaskRpc

// 占据任务
func (p *TaskMgr) hold() ([]TaskIntf, error) {
	taskIntfList := make([]TaskIntf, 0)
	/**** Step1:拿到scheduleCfgDic中缓存的任务配置 ****/
	cfg, ok := scheduleCfgDic[p.TaskType]
	if !ok {
		martlog.Errorf("scheduleCfgDic %s, not have taskType %s", tools.GetFmtStr(scheduleCfgDic), p.TaskType)
		return nil, errors.New("tasktype not exist")
	}
	// 构造拉取任务列表的请求，其中拉取多少个，由cfg中的ScheduleLimit决定
	var reqBody = &model.HoldTasksReq{
		TaskType: p.TaskType,
		Limit:    cfg.ScheduleLimit,
	}
	/**** Step2:调用http请求，从flowsvr拉任务 ****/
	rpcTaskResp, err := taskRpc.HoldTasks(reqBody)
	if err != nil {
		martlog.Errorf("taskRpc.GetTaskList %s", err.Error())
		return taskIntfList, err
	}
	martlog.Infof("rpcTaskResp %+v", rpcTaskResp)
	if rpcTaskResp.Code != 0 {
		errMsg := fmt.Sprintf("taskRpc.GetTaskList resp code %d", rpcTaskResp.Code)
		martlog.Errorf(errMsg)
		return taskIntfList, errors.New(errMsg)
	}
	storageTaskList := rpcTaskResp.TaskList
	if len(storageTaskList) == 0 {
		return taskIntfList, nil
	}
	// 日志记录拉到了多少任务
	martlog.Infof("schedule will deal %d task", len(storageTaskList))
	taskIdList := make([]string, 0)
	/**** Step 3: 将数据库返回任务结构，转换为TaskIntf这个接口，方面后续操作 ****/
	for _, st := range storageTaskList {
		task, err := GetTaskInfoFromStorage(st)
		if err != nil {
			martlog.Errorf("GetTaskInfoFromStorage err %s", err.Error())
			return taskIntfList, err
		}
		task.Base().Status = int(constant.TASK_STATUS_PROCESSING)
		taskIntfList = append(taskIntfList, task)
		taskIdList = append(taskIdList, task.Base().TaskId)
	}

	return taskIntfList, nil
}

/**
 * @Description: 处理单任务
 * @param taskInterface
 */
func run(taskInterface TaskIntf, cfg *model.TaskScheduleCfg) {
	martlog.Infof("Start run taskId %s... ", taskInterface.Base().TaskId)
	// defer函数会在当前函数结束时调用，用来更新Task状态，以及做一些异常处理
	defer func() {
		// 如果任务失败了
		if taskInterface.Base().Status == int(constant.TASK_STATUS_FAILED) {
			// HandleFailedMust是说这个收尾函数必须成功，不然不让关掉任务
			// 但此时其实任务重试次数已经结束了，所以如果这个操作失败，
			// 就把任务保持在执行中，等待执行时间过长重试，希望下次成功
			// 相当于是给了时间人工介入处理，不是关联逻辑
			err := taskInterface.HandleFailedMust()
			if err != nil {
				taskInterface.Base().Status = int(constant.TASK_STATUS_PROCESSING)
				martlog.Errorf("handle failed must err %s", err.Error())
				return
			}

			// HandleFinishError是失败处理函数，但这个处理无论是否生效，都可以结束任务
			err = taskInterface.HandleFinishError()
			if err != nil {
				martlog.Errorf("handle finish err %s", err.Error())
				return
			}
		}
		// 结束时无论成功和失败，都调用HandleFinish, 用来收尾
		if taskInterface.Base().Status == int(constant.TASK_STATUS_FAILED) ||
			taskInterface.Base().Status == int(constant.TASK_STATUS_SUCC) {
			taskInterface.HandleFinish()
		}
		// 更新任务状态
		err := taskInterface.SetTask()
		if err != nil {
			martlog.Errorf("schedule set task err %s", err.Error())
			// 再尝试一次，非必要流程
			err = taskInterface.SetTask()
			if err != nil {
				martlog.Errorf("schedule set task err twice.Err %s", err.Error())
			}
		}
		martlog.Infof("End run. releaseProcessRight")
	}()
	// 加载任务上下文
	err := taskInterface.ContextLoad()
	if err != nil {
		martlog.Errorf("taskId %s reload err %s", taskInterface.Base().TaskId, err.Error())
		taskInterface.Base().Status = int(constant.TASK_STATUS_PENDING)
		return
	}
	beginTime := time.Now()
	// 执行HandleProcess业务逻辑
	err = taskInterface.HandleProcess()

	// 若用户调用过SetContextLocal, 则会自动更新状态
	// taskInterface.ScheduleSetContext()
	// 记录调度信息
	taskInterface.Base().ScheduleLog.HistoryDatas = append(taskInterface.Base().ScheduleLog.HistoryDatas,
		taskInterface.Base().ScheduleLog.LastData)
	// 只记录最近三次操作信息
	if len(taskInterface.Base().ScheduleLog.HistoryDatas) > 3 {
		taskInterface.Base().ScheduleLog.HistoryDatas = taskInterface.Base().ScheduleLog.HistoryDatas[1:]
	}
	cost := time.Since(beginTime)
	martlog.Infof("taskId %s HandleProcess cost %v", taskInterface.Base().TaskId, cost)
	// 任务没设置为结果，就重置状态以待调度
	if taskInterface.Base().Status == int(constant.TASK_STATUS_PROCESSING) {
		taskInterface.Base().Status = int(constant.TASK_STATUS_PENDING)
	}
	taskInterface.Base().ScheduleLog.LastData.TraceId = fmt.Sprintf("%v", uuid.New())
	taskInterface.Base().ScheduleLog.LastData.Cost = fmt.Sprintf("%dms", cost.Milliseconds())
	taskInterface.Base().ScheduleLog.LastData.ErrMsg = ""
	// 减去优先时间
	taskInterface.Base().OrderTime = time.Now().Unix() - taskInterface.Base().Priority
	if err != nil {
		// 需要排除当前重试时间是不是负数，负数代表均匀重试，不需要渐进式操作
		if taskInterface.Base().MaxRetryInterval > 0 {
			// 计算当前重试次数对应的最大重试时间
			if taskInterface.Base().CrtRetryNum < 30 {
				// 每次重试时间会翻倍
				taskInterface.Base().MaxRetryInterval = taskInterface.Base().MaxRetryInterval << 1
			} else {
				// 当重试次数 大于等于 30，最大重试时间 会变成 MaxRetryInterval
				taskInterface.Base().MaxRetryInterval = cfg.MaxRetryInterval
			}
			// 重试时间不能超过最大重试时间
			if taskInterface.Base().MaxRetryInterval > cfg.MaxRetryInterval {
				taskInterface.Base().MaxRetryInterval = cfg.MaxRetryInterval
			}
		}
		delayTime := taskInterface.Base().MaxRetryInterval
		if delayTime < 0 {
			// 防止delayTime为负数
			delayTime = -delayTime
		}
		// 延时加到orderTime上去（如果需要延迟，那么优先级将会失效）
		if delayTime != 0 {
			// time.Now().Unix() 单位是s
			taskInterface.Base().OrderTime = time.Now().Unix() + int64(delayTime)
		}
		msgLen := tools.Min(len(err.Error()), MAX_ERR_MSG_LEN)
		errMsg := err.Error()[:msgLen]
		taskInterface.Base().ScheduleLog.LastData.ErrMsg = errMsg
		martlog.Errorf("task.HandleProcess err %s", err.Error())
		if taskInterface.Base().MaxRetryNum == 0 || taskInterface.Base().CrtRetryNum >= taskInterface.Base().MaxRetryNum {
			taskInterface.Base().Status = int(constant.TASK_STATUS_FAILED)
			return
		}
		if taskInterface.Base().Status != int(constant.TASK_STATUS_FAILED) {
			taskInterface.Base().CrtRetryNum++
		}
		return
	}
}

// RandNum func for rand num
func RandNum(num int) int {
	// 生成随机数[0,num)
	step := rand.Intn(num)
	// 50%概率为负数
	flag := rand.Intn(2)
	if flag == 0 {
		return -step
	}
	return step
}
