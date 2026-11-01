# aliyun_log 工具说明

读阿里云 SLS 日志的小工具集，依赖 `aliyun-log-python-sdk`，配置统一从仓库根 `rpc.json` 的
`aliyun_log` 段读取。

## 配置（rpc.json）

```json
"aliyun_log": {
  "endpoint": "cn-hangzhou.log.aliyuncs.com",
  "ak_id": "...",
  "ak_sec": "...",
  "project": "log-service-...-cn-hangzhou",
  "logstore": "new_logstore"
}
```

子键小写、无前缀。文件缺失、JSON 坏、该段不是对象、单键类型不对，都只打 WARNING 并回落空值。

## config.py

被其它脚本 import，一般不单独运行。

- 导出 `ENDPOINT` / `AK_ID` / `AK_SEC` / `PROJECT` / `LOGSTORE`
- `build_client()` 返回 `LogClient`；缺配置项时抛明确的 `ValueError`
- `rpc.json` 位置从脚本目录向上查找，目录被移动也不会失效

## sls_reader.py

读日志的核心函数，按需 import。

| 函数 | 用法 |
| --- | --- |
| `list_shards(client)` | 返回分片信息列表 |
| `pull_shard(client, shard_id, start_time="begin", count=100)` | 从游标起点翻页，逐条产出展平后的日志 dict |
| `pull_all(client, minutes=None, start_time="begin")` | 所有分片合并；`minutes` 只在本机按 `__time__` 过滤，不改游标起点 |
| `query_logs(client, minutes=1440, query="", line=100)` | 走 JSON 查询接口，返回 `(总数, 日志列表)`，不看分片 |
| `format_time(ts)` | 秒级时间戳格式化成本地时间 |

## pull_logs.py：读日志

```bash
python tools/aliyun_log/pull_logs.py                  # 从分片起点读全量
python tools/aliyun_log/pull_logs.py --minutes 30    # 只看最近 30 分钟
python tools/aliyun_log/pull_logs.py --source query  # 走 JSON 查询接口核对
python tools/aliyun_log/pull_logs.py --begin end     # 只读新写入（类似 tail -f）
python tools/aliyun_log/pull_logs.py --json          # 每行一条 JSON
python tools/aliyun_log/pull_logs.py --query "msg: hello"   # 服务端过滤
```

参数：`--minutes`（本地时间过滤，默认不过滤）、`--begin`（`begin`/`end`/`now`/秒级时间戳）、
`--count`（单次 pull 条数，默认 100）、`--source`（`pull` 默认 / `query`）、`--json`。

## diagnose.py：排查读不到日志

```bash
python tools/aliyun_log/diagnose.py
```

依次打印 logstore 信息（类型、TTL、分片数）、分片状态、从 `begin` 拉全量的条数、最近 7 天
查询接口条数，最后给出结论（没写入，还是解析/游标/翻页问题）。

## 三个常见坑

- proto 字段名是驼峰 `LogGroups` / `Logs` / `Contents`，用 `log_groups` / `logs` / `contents`
  会静默解析出 0 条。
- `pull_logs` 单次最多 `count` 条，不按 `next_cursor` 翻页只能拿到第一批。
- `get_cursor` 用时间戳做起点时，早于该时间写入的日志在游标之前，直接漏掉；核对有无数据用
  `begin` 或 `--source query`。

## 写入侧

`server_mqtt.py` 的 `DEFAULT_GLOBAL_TOPIC_URL_LIST` 已挂 SLS Web Tracking 地址，报告 JSON
以 GET 免签名写入 `new_logstore` 的 `report` 字段（logstore 需开启 `enable_tracking`）：

```
https://<project>.<endpoint>/logstores/<logstore>/track?APIVersion=0.6.0&report=<百分号编码的 JSON>
```
