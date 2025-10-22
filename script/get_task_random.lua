wrk.method = "GET"

-- Generate a task ID in the format of UUID + "_lark_1"
function generate_task_id()
    local uuid = "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx" -- Replace with an actual UUID pattern
    local formatted_task_id = string.gsub(uuid, "[xy]", function(c)
        local v = (c == "x") and math.random(0, 0xf) or math.random(8, 0xb)
        return string.format("%x", v)
    end) .. "_lark_1"

    return formatted_task_id
end

-- Generate a task ID and append it to the URL
local random_task_id = generate_task_id()
wrk.path = "/v1/get_task?taskId=" .. random_task_id
