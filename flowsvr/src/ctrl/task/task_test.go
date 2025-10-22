package task

import (
	"fmt"
	"github.com/niuniumart/asyncflow/taskutils/rpc/model"
	"testing"

	"github.com/niuniumart/asyncflow/flowsvr/src/db"

	"github.com/smartystreets/goconvey/convey"

	"github.com/niuniumart/gosdk/tools"
)

func TestCreateTask(t *testing.T) {
	db.InitDB()
	convey.Convey("TestCreateTask", t, func() {
		// case 1: input err
		var hd CreateTaskHandler
		hd.Req.TaskData = model.TaskData{}
		err := hd.HandleInput()
		convey.So(err, convey.ShouldNotBeNil)

		// case 2: success case
		hd = CreateTaskHandler{}
		hd.Req.TaskData = model.TaskData{
			TaskType: "lark",
			UserId:   "cat",
		}
		err = hd.HandleInput()
		convey.So(err, convey.ShouldBeNil)
		err = hd.HandleProcess()
		convey.So(err, convey.ShouldBeEmpty)
	})
}

func TestGetTask(t *testing.T) {
	db.InitDB()
	convey.Convey("TestGetTask", t, func() {
		/******* 1. success case *********/
		hd := GetTaskHandler{}
		hd.Req.TaskId = "7a58acc1-232e-4e2f-b79e-bd03b780d20a_dci-claim_2"
		err := hd.HandleInput()
		convey.So(err, convey.ShouldBeNil)
		err = hd.HandleProcess()
		convey.So(err, convey.ShouldBeNil)
		convey.So(hd.Resp.TaskData.UserId, convey.ShouldEqual, 1449)
		fmt.Printf("resp %+v", tools.GetFmtStr(hd.Resp))

		/******* 2. input error case ***********/
		hd = GetTaskHandler{}
		err = hd.HandleInput()
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestGetTaskList(t *testing.T) {
	db.InitDB()
	convey.Convey("TestGetTaskList", t, func() {
		/******* 1. success case ********/
		var hd GetTaskListHandler
		hd.Req.TaskType = "dci_claim"
		hd.Req.Status = int(db.TASK_STATUS_PENDING)
		err := hd.HandleInput()
		convey.So(err, convey.ShouldBeNil)
		err = hd.HandleProcess()
		convey.So(err, convey.ShouldBeNil)
		fmt.Printf("resp %s", tools.GetFmtStr(hd.Resp))

		/******* 2. invalid input case ********/
		hd = GetTaskListHandler{}
		err = hd.HandleInput()
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestGetTaskScheduleCfgList(t *testing.T) {
	db.InitDB()
	convey.Convey("TestGetTaskScheduleCfgList", t, func() {
		/******* 1. success case ********/
		var hd GetTaskScheduleCfgListHandler
		err := hd.HandleInput()
		convey.So(err, convey.ShouldBeNil)
		err = hd.HandleProcess()
		convey.So(err, convey.ShouldBeNil)
		fmt.Printf("resp %s", tools.GetFmtStr(hd.Resp))

	})
}
