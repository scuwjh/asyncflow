# asyncflow

## 项目介绍
* flowsvr：任务流服务，对外提供任务处理/查询接口
* worker：处理某种/多种任务，其中集成了tasksdk提供自动调度，部署于客户端

## 编译&启动

* flowsvr执行前需要自行安装MySQL
* 提前执行asyncflow/flowsvr/sql/create.sql 下面的语句
* 并在asyncflow/flowsvr/src/config/config-test.toml 配置服务地址，还有一些服务治理参数

### Win系统执行
* flowsvr运行
  * 切换到asyncflow/flowsvr/src目录下
  * `go run main.go test`

* worker运行
  * 切换到asyncflow/worker/src目录下
  * `go run main.go`
  
## 创建一条测试任务
* 打开训练营flowsvr设计文档，使用接口文档创建对应任务
