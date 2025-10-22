wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"

local body = [[
{
    "taskData": {
        "user_id": "niuniu",
        "task_id": "",
        "task_type": "lark",
        "task_stage": "sendmsg",
        "status": 1,
        "crt_retry_num": 0,
        "max_retry_num": 0,
        "max_retry_interval": 0,
        "schedule_log": "",
        "context": "{\"ReqBody\":{\"Msg\":\"nice to meet u\",\"FromAddr\":\"fish\",\"ToAddr\":\"cat\"},\"UserId\":\"\"}",
        "order_time": 0,
        "create_time": null,
        "modify_time": null
    }
}
]]

wrk.body = body

