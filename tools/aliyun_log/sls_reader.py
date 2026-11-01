"""读 SLS 日志的两种方式。

* pull_shard / pull_all：按 shard 游标拉取，适合增量；必须用 next_cursor 翻页。
* query_logs：按时间区间走 JSON 查询接口，不看分片，最适合核对"到底有没有写进去"。
"""

import time

try:  # 作为包导入（python -m aliyun_logstore.xxx）
    from .config import LOGSTORE, PROJECT, build_client
except ImportError:  # 直接 python aliyun_logstore/xxx.py 运行
    from config import LOGSTORE, PROJECT, build_client


def list_shards(client, project=PROJECT, logstore=LOGSTORE):
    return client.list_shards(project, logstore).get_shards_info()


def pull_shard(client, shard_id, start_time="begin", count=100, max_pages=1000, query=None,
               project=PROJECT, logstore=LOGSTORE):
    """从 start_time 起沿游标翻页拉取，逐条产出展平后的日志 dict。

    start_time 支持 "begin" / "end" / "now" / 秒级时间戳 / 可读时间字符串。
    单次 pull_logs 最多返回 count 条，不翻页就只能拿到第一批。
    """
    cursor = client.get_cursor(project, logstore, shard_id, start_time).get_cursor()
    for _ in range(max_pages):
        resp = client.pull_logs(project, logstore, shard_id, cursor, count=count, query=query)
        logs = resp.get_flatten_logs_json()
        if not logs:
            return
        for item in logs:
            yield item
        next_cursor = resp.get_next_cursor()
        if not next_cursor or next_cursor == cursor:
            return
        cursor = next_cursor


def pull_all(client, minutes=None, start_time="begin", **kwargs):
    """拉取所有 shard。minutes 只在本机按 __time__ 过滤，不改变游标起点。"""
    cutoff = None if minutes is None else int(time.time()) - int(minutes) * 60
    rows = []
    for shard in list_shards(client):
        for item in pull_shard(client, shard["shardID"], start_time=start_time, **kwargs):
            if cutoff is not None and int(item.get("__time__", 0)) < cutoff:
                continue
            item["__shard__"] = shard["shardID"]
            rows.append(item)
    return rows


def query_logs(client, minutes=1440, query="", line=100, offset=0, reverse=True):
    """按时间区间用 JSON 查询接口取日志，返回 (总数, 日志列表[dict])。"""
    from aliyun.log import GetLogsRequest

    to_time = int(time.time())
    from_time = to_time - int(minutes) * 60
    req = GetLogsRequest(PROJECT, LOGSTORE, fromTime=from_time, toTime=to_time, query=query,
                         line=line, offset=offset, reverse=reverse)
    resp = client.get_logs(req)
    return resp.get_count(), [queried_log_to_dict(x) for x in resp.get_logs()]


def queried_log_to_dict(log):
    """JSON 查询接口返回的是 QueriedLog 对象，统一成和 pull 一样的 dict。"""
    if isinstance(log, dict):
        return log
    row = dict(log.get_contents() or {})
    row["__time__"] = log.get_time()
    row["__source__"] = log.get_source()
    return row


def format_time(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts)))
