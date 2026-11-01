"""读取 aliyun logstore 日志（修正版）。

原脚本读数恒为 0 的原因是解析用了 snake_case 字段名：SDK 的 proto 字段是驼峰
`LogGroups` / `Logs` / `Contents`，`log_groups` / `logs` / `contents` 永远匹配不到，
于是静默返回空列表。这里直接用 SDK 的 get_flatten_logs_json()，并翻页取全量。

用法：
    python aliyun_logstore/pull_logs.py                  # 从分片起点读全量
    python aliyun_logstore/pull_logs.py --minutes 30     # 只看最近 30 分钟写入
    python aliyun_logstore/pull_logs.py --begin end      # 只看新写入（像 tail -f）
"""

import argparse
import json
import sys

try:
    from .config import LOGSTORE, PROJECT, build_client
    from .sls_reader import format_time, pull_all, query_logs
except ImportError:
    from config import LOGSTORE, PROJECT, build_client
    from sls_reader import format_time, pull_all, query_logs


def main(argv=None):
    ap = argparse.ArgumentParser(description="读取 aliyun logstore 日志")
    ap.add_argument("--minutes", type=int, default=None, help="只保留最近 N 分钟写入的日志（默认不过滤）")
    ap.add_argument("--begin", default="begin", help="pull 游标起点：begin/end/now 或秒级时间戳（默认 begin）")
    ap.add_argument("--count", type=int, default=100, help="单次 pull 条数（默认 100）")
    ap.add_argument("--query", default=None, help="服务端过滤表达式，如 'msg: hello'")
    ap.add_argument("--source", choices=("pull", "query"), default="pull",
                    help="pull=分片游标（默认），query=按时间区间查询接口")
    ap.add_argument("--json", action="store_true", help="按 JSON 行输出")
    args = ap.parse_args(argv)

    client = build_client()

    if args.source == "query":
        count, logs = query_logs(client, minutes=args.minutes or 1440, query=args.query or "")
    else:
        logs = pull_all(client, minutes=args.minutes, start_time=args.begin,
                        count=args.count, query=args.query)
        count = len(logs)

    print("project=%s logstore=%s source=%s 共读到 %d 条" % (PROJECT, LOGSTORE, args.source, count), file=sys.stderr)
    for row in logs:
        if args.json:
            print(json.dumps(row, ensure_ascii=False))
        else:
            print("%s shard=%s %s" % (format_time(row.get("__time__", 0)), row.get("__shard__", "-"),
                                      json.dumps(row, ensure_ascii=False)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
