"""诊断"日志写进去了但读不到"。

依次检查：logstore 是否存在 / 分片状态 / 分片起点全量条数 / 最近 7 天查询条数，
最后给出结论性提示。

用法：python aliyun_logstore/diagnose.py
"""

import sys

try:
    from .config import LOGSTORE, PROJECT, build_client
    from .sls_reader import format_time, list_shards, pull_shard, query_logs
except ImportError:
    from config import LOGSTORE, PROJECT, build_client
    from sls_reader import format_time, list_shards, pull_shard, query_logs


def main():
    client = build_client()

    print("[1] logstore 信息")
    info = client.get_logstore(PROJECT, LOGSTORE).body
    print("    名称=%s 类型=%s ttl=%s 分片数=%s 创建时间=%s" % (
        info.get("logstoreName"), info.get("mode"), info.get("ttl"),
        info.get("shardCount"), format_time(info.get("createTime", 0))))
    print("    只有 mode=standard 的 logstore 才能用 pull_logs；metric/query 类型读不到。")

    print("[2] 分片状态")
    shards = list_shards(client)
    for s in shards:
        print("    shard=%s status=%s createTime=%s" % (
            s["shardID"], s["status"], format_time(s.get("createTime", 0))))

    print("[3] 从分片起点 begin 拉全量（先确认数据是否存在，不看时间窗）")
    total = 0
    for s in shards:
        logs = list(pull_shard(client, s["shardID"], start_time="begin"))
        total += len(logs)
        print("    shard=%s 条数=%d" % (s["shardID"], len(logs)))
        for row in logs[:5]:
            print("        %s %r" % (format_time(row.get("__time__", 0)), row))
    print("    合计 %d 条" % total)

    print("[4] 最近 7 天 JSON 查询接口核对（走索引，索引未开启时条数可能少于 pull）")
    count, logs = query_logs(client, minutes=7 * 24 * 60, line=10, reverse=True)
    print("    count=%d，最近 3 条:" % count)
    for row in logs[:3]:
        print("        %s %r" % (format_time(row.get("__time__", 0)), row))

    print("[5] 结论")
    if total == 0:
        print("    logstore 里没有任何数据：写入端没成功，或写的是别的 project/logstore。")
    else:
        print("    数据存在（%d 条）。读不到只可能是：(a) proto 字段名写错导致解析为空；" % total)
        print("    (b) 游标用了 start_time 时间点，比它早的日志本来就在游标之前；")
        print("    (c) 只 pull 了一次没翻页，或 count 太小。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
