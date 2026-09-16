"""
# 汇总所有检测工具的消息 将消息进行处理放入一个专门的队列中
## 将所有关于spark/flink的消息汇总处理后的结果：
{
    "app_id": "app_id",
    "app_type": "SPARK/FLINK"
    "error_infos": [
        {
            "error_type": "error/warning",
            "describe": "表述",
            "check_type": "check_type",
            "data": any
        },
        ...
    ]
}

> 考虑到Agent并发获取消息，因此采用一个Agent获取一个应用一种消息后，
> 将锁住该应用该类型的其他消息，直到该消息处理完成

redis(哈希):
    name: app:{app_id}:{app_type}
    key: check_type
    value: {"is_occupy": "true/false", "data": queue_infos}

queue_infos -> [
    {
        "error_type": "error/warning",
        "describe": "表述",
        "data": any
    },
    ...
] -> 非数组，只是为了可视化展示
(以下信息为了解决队列数据量过大，导致内存溢出的问题)
or
queue_infos_path -> 队列数据文件路径
or
queue_infos_db -> 队列数据数据库

阈值与报警：
在yaml中按数据量(不分类型)设置 低告警 中告警 高告警

使用：
假设有n个运维Agent(单个Agent并发处理 -> 伪多运维Agent),
初始时，每个Agent只能获取一个应用中的一个错误消息，并将 is_occupy 设为true
当Agent处理完成一个错误消息后，将 is_occupy 设为false，
若此时这个已经完成的Agent想要获取正在处理的应用时，会先检查is_occupy是否为false，
若为false，则获取该应用的错误消息，否则优先跳过，获取其他应用错误消息，
若无其他应用错误消息，则等待处理完成
"""
from queue import Queue
from load_config.config import config

queue_config = config.get("hadoop", {}).get("checker", {}).get("error_queue", {})
mode = queue_config.get("mode", "number")
if mode not in ["number", "time"]:
    raise ValueError(f"error_queue mode 必须为 number 或 time，当前为 {mode}")
value = queue_config.get("value", [])
if not value or len(value) != 3:
    raise ValueError(f"error_queue value 必须为3个阈值，当前为 {value}")

queue = Queue()


def check_message_handle():
    pass
