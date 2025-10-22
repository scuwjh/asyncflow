package initialize

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/niuniumart/asyncflow/flowsvr/src/cache"
	"github.com/niuniumart/asyncflow/flowsvr/src/ctrl/task"
	"github.com/niuniumart/asyncflow/flowsvr/src/db"
)

// InitResource 初始化服务资源
func InitResource() {
	if err := db.InitDB(); err != nil {
		panic(fmt.Sprintf("InitDB err %s", err.Error()))
	}

	if err := cache.InitCache(); err != nil {
		panic(fmt.Sprintf("InitCache err %s", err.Error()))
	}
}

// RegisterRouter 注册路由
func RegisterRouter(router *gin.Engine) {
	v1 := router.Group("/v1")
	{
		// 创建任务接口，前面是路径，后面是执行的函数，跳进去
		v1.POST("/create_task", task.CreateTask)
		// 占据任务
		v1.POST("/hold_tasks", task.HoldTasks)
		// 获取任务列表（请求参数是 taskType Limit Status）
		v1.GET("/get_task_list", task.GetTaskList)
		// 获取任务配置信息列表
		v1.GET("/get_task_schedule_cfg_list", task.GetTaskScheduleCfgList)
		// 查询任务（请求参数是 TaskId）
		v1.GET("/get_task", task.GetTask)
		// 更新任务
		v1.POST("/set_task", task.SetTask)
		// 通过taskType获取任务所有记录数量
		v1.GET("/get_task_counts_by_type", task.GetTaskCountsByType)
		// 注册任务
		v1.POST("/register_task", task.RegisterTask)
		v1.GET("ping", func(c *gin.Context) {
			c.JSON(200, gin.H{
				"message": "pong",
			})
		})
	}

}
