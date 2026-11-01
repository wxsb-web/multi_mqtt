"""阿里云 SLS 连接参数。

读取顺序：仓库根 `rpc.json` 的 `aliyun_log` 段（子键 endpoint/ak_id/ak_sec/project/logstore，
小写、无前缀）；文件缺失、不是 JSON、该段不是对象、单个键类型不对，都只打 WARNING 并回落空值，
不抛异常。真正缺 key 时才在 build_client() 里报明确的错。
"""

import json
import os

RPC_JSON = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "rpc.json")

_KEYS = ("endpoint", "ak_id", "ak_sec", "project", "logstore")


def _load():
    values = dict.fromkeys(_KEYS, "")
    try:
        with open(RPC_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("WARNING: 未找到 %s，aliyun_log 配置为空" % RPC_JSON)
        return values
    except Exception as ex:
        print("WARNING: 读取 %s 失败（%s），aliyun_log 配置为空" % (RPC_JSON, ex))
        return values

    section = data.get("aliyun_log") if isinstance(data, dict) else None
    if not isinstance(section, dict):
        print("WARNING: rpc.json 的 aliyun_log 不是对象，配置为空")
        return values

    for key in _KEYS:
        value = section.get(key)
        if isinstance(value, str):
            values[key] = value.strip()
        elif value is not None:
            print("WARNING: rpc.json aliyun_log.%s 类型不对（%r），回落空值" % (key, value))
    return values


_values = _load()

ENDPOINT = _values["endpoint"]
AK_ID = _values["ak_id"]
AK_SEC = _values["ak_sec"]
PROJECT = _values["project"]
LOGSTORE = _values["logstore"]


def build_client():
    """创建 LogClient（endpoint 不要带 project，SDK 会自动拼 host）。"""
    missing = [k for k in _KEYS if not _values[k]]
    if missing:
        raise ValueError("rpc.json 的 aliyun_log 缺配置项: %s" % ", ".join(missing))

    from aliyun.log import LogClient

    return LogClient(ENDPOINT, AK_ID, AK_SEC)
