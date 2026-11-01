这是一个以 Python 为主的 MQTT RPC 项目，辅助逻辑分散在根目录、`client/` 和 `.trae/skills/` 中，没有单独的 `utils/` 模块。

- 核心通信层：`multi_mqtt.py` 管理 MQTT Broker 连接、消息分发、去重、签名和连接统计。
- RPC 执行与服务端：`rpc_executor.py` 提供持久化 Python 执行器；`server_mqtt.py`、`server_http.py`、`server_http_asgi.py`（HTTP + WebSocket 一体化单体 ASGI 应用）、`server_http_wsgi.py`（WSGI 形态）和 `rpc_tcp.py` 分别提供 MQTT、HTTP、ASGI、WSGI 和 TCP 服务。
- 客户端：`client/` 包含 MQTT REPL、远端命令/PTY、HTTP 文件同步和 SOCKS5 客户端。
- 其他：`U.py` 查询设备信息；`realtime_editor.py` 是实时协作编辑工具；`tests/` 放测试；`.trae/skills/` 放独立自动化脚本。

## 目录

1. 辅助工具函数清单
2. pty 客户端首包统计（1188 RPC）
3. 多 broker 下的“重复投递”现象（不是 bug）
4. MQTT RPC 请求/响应流程与安全边界
5. 服务端是否需要配合网络层观测（本次不改）
6. 其他笔记与 TODO

## 1. 辅助工具函数清单

### 通信核心与通用基础

- `ensure_dependencies`：检查并安装运行依赖。
- `get_duplicated_kargs`：从多个别名参数中读取同一选项，处理冲突及是否移除原键。
- `stime`、`utc_ms`、`get_req_id`：时间戳格式化、毫秒时间和请求 ID。
- `process_cipher`：JSON 序列化及可选 AES-GCM 加解密。
- `TTLCache`：有 TTL 和容量上限的内存去重缓存。
- `_eval_safe_int_expression`：安全计算整数表达式，用于密钥解析。
- `get_standard_pem_bytes`、`_describe_public_key`、`get_standard_public_pem_bytes`：私钥/公钥格式标准化和公钥信息描述。
- `BrokerStat`、`ConnectionQualityStats`：Broker 状态和连接质量统计组件。

### RPC 执行与服务端

- `PythonExecutor`：持有共享命名空间、执行 RPC 代码并捕获输出；内部包含普通表达式求值和 `await` 执行逻辑。
- `_redirect_stdout`：临时重定向标准输出。
- `format_result`：把执行结果转成可传输的文本形式。
- `http_import`：从 URL 导入内容。
- `get_bmp_bytes`：生成 BMP 图片字节；`pretty_format` 格式化对象；`stime` 生成时间字符串。
- `ThreadedHTTPServer`、`WebSocket`：HTTP 并发服务器和轻量 WebSocket 支持组件。
- `_is_verify_enabled`：检查 MQTT 服务端是否已正确启用验签。
- `_wsgi_unescape`：还原 WSGI 环境中按 Latin-1 承载的原始路径字节。
- `TCPFrameProtocol.encode`、`TCPFrameProtocol.decode_frames`：TCP 长度前缀帧的编码和解码。

### 客户端通用、远端命令与文件传输

- `client_mqtt._cli_opts`：根据别名生成 CLI 选项。
- `client_mqtt._get_clipboard_text`、`client_mqtt._normalize_history_path`：读取剪贴板、规范化历史文件路径。
- `client_mqtt._build_prompt`、`client_mqtt._make_print`：构造交互式输入和兼容 prompt_toolkit 的打印函数。
- `client_mqtt._handle_magic`：处理 REPL 的 `%topic`、`%key`、`%status` 等命令；`client_mqtt._fallback_code_input`：无 prompt_toolkit 时的输入回退。
- `build_code`、`build_pty_start_code`：构造远端执行及 PTY 启动代码。
- `bytes_to_wire`、`wire_to_bytes`、`_wire_len`、`_iter_wire_chunks`：字节/报文转换、估算传输长度和按预算分块。
- `_is_within`、`_validate_tar_member`、`safe_extract_tar`：校验路径边界并安全解压 tar。
- `_parse_pty_responders`：解析 PTY 响应方信息；`_PtyReorderBuffer` 用于 PTY 数据重排。
- `client_http._req_repr`、`client_http._fmt_size`：生成请求调试表示和格式化文件大小。
- `client_http._rpc_call`、`client_http._server_code`、`client_http._rpc_json`：HTTP RPC 请求、远端代码包装和 JSON 响应解析。
- `client_http._rpc_list_dir`、`client_http._rpc_upload_chunk`、`client_http._rpc_delete_file`、`client_http._rpc_sha256_remote`、`client_http._upload_one_file`：远端目录枚举、分块上传、删除、校验和单文件上传。

### 终端、PTY 与 SOCKS5 工具

- `cmd_client_mqtt._enable_win_vt`、`cmd_client_mqtt._fmt_stats`：启用 Windows VT 输出、格式化连接统计。
- `cmd_client_mqtt._parse_getdir_args`、`cmd_client_mqtt._do_pull_dir`：解析并执行目录拉取。
- `cmd_client_mqtt._parse_tmux_args`、`cmd_client_mqtt._parse_watch`、`cmd_client_mqtt._build_shell_prompt`：解析终端命令参数和构造 shell 提示符。
- `pty_client_mqtt._enable_output_vt`、`pty_client_mqtt._new_input_transcoder`、`pty_client_mqtt._make_raw_console`：终端模式、输入编码和原始控制台创建。
- `pty_client_mqtt._normalize_paste_text`、`pty_client_mqtt._read_clipboard_text_win`、`pty_client_mqtt._bin_stdout`、`pty_client_mqtt._bin_stderr`：粘贴文本、剪贴板和二进制标准流处理。
- `pty_client_mqtt._terminal_cleanup`、`pty_client_mqtt.install_console_guards`：终端恢复和控制台信号保护。
- `pty_client_mqtt._parse_key_spec`、`pty_client_mqtt._build_key_display_map`、`pty_client_mqtt._describe_key`：按键序列解析及显示。
- `pty_client_mqtt._stderr_writer`、`pty_client_mqtt._stderr_write`、`pty_client_mqtt._emit_local`、`pty_client_mqtt.get_log`、`pty_client_mqtt.clear_log`：终端诊断输出和内存日志读写；相关组件有 `_LogRing`、`_RingTextIO`、`_RingLogHandler`、`LogHub`。
- `pty_client_mqtt._lan_ip`、`pty_client_mqtt.log_html`、`pty_client_mqtt._install_ring_logging`、`pty_client_mqtt._install_log_capture`：本机 IP、日志网页及日志捕获初始化。
- `pty_client_mqtt._writer_state`、`pty_client_mqtt.writer_diag`、`pty_client_mqtt.reset_out_writer`：输出 writer 状态诊断和重置。
- `pty_client_mqtt._arm_esc_guard`、`pty_client_mqtt._filter_injected_esc`、`pty_client_mqtt._cancel_console_selection`、`pty_client_mqtt._strip_ansi`：ESC 输入防护、控制台选择处理和 ANSI 清理。
- `pty_client_mqtt.local_rpc`、`pty_client_mqtt._call_ai_bridge`、`pty_client_mqtt.ai_pty_run`、`pty_client_mqtt.ai_pty_send`、`pty_client_mqtt.ai_pty_status`：AI bridge 本地 RPC/PTY 调用。
- `socks5_client_mqtt._recv_exact`、`socks5_client_mqtt._socks_reply`：SOCKS5 TCP 精确读取和协议响应封装；`build_socks5_start_code` 生成远端启动代码。

### 设备信息与编辑器

- `_cmd`、`_file`、`_prop`：执行系统命令、读取文件和 Android 属性。
- `_is_android`、`_safe_getter`：平台检测和安全字符串返回包装器。
- `get_manufacturer`、`get_model`、`get_system_id`、`get_id`：跨 Android、Windows、macOS、Linux 获取设备厂商、型号和系统 ID。
- `Editor._signature`、`Editor._message`、`Editor.broadcast`：文件签名、消息封装和 WebSocket 客户端广播。`Editor` 还负责文件保存、外部变更监视与客户端同步。

### `.trae/skills/` 下的独立脚本工具

这些脚本属于技能/自动化工具，不是 MQTT RPC 核心模块：

- `pull_apk.py`：`human`、`human_speed` 格式化大小/速度；`build_rpc_url` 生成 RPC URL。
- `remote_build.py`：`run_serial` 运行带超时和等待逻辑的命令。
- `relay_pull.py`：`human`、`normalize_raw_url`、`dial_detail`、`head_info`、`download_once`，负责 URL 规范化、连接探测和续传下载。
- `relay_repo.py`：`token_file`、`get_token`、`api`，处理 token 读取和 GitHub API 请求。
- `relay_push.py`：`run` 包装命令运行。

复用性最高的工具集中在 `multi_mqtt.py`、`rpc_executor.py` 和 `client/remote_cmd.py`。终端与日志工具主要聚集在 `client/pty_client_mqtt.py`，HTTP 文件传输工具集中在 `client/client_http.py`。

目前工具函数与各模块的交互流程混在一起，尚未抽成独立工具层；例如时间格式化分别出现在核心 MQTT 模块和 HTTP 服务模块。测试文件中的测试辅助函数未列入上面的运行时代码清单。

## 2. pty 客户端首包统计（1188 RPC）

`client/pty/parts/045_firstpacket.py` 里的 `first_packet_stats` 是**纯被动旁路**：
只在 pty 进程内包住 `net.message_callback` 与 `net.publish_broadcast`（以及每个
paho client 的 `publish`/`on_publish`），不新增任何网络请求，不改 QoS、签名、
去重等语义；安装失败只打 WARN，绝不影响启动与主流程。

```bash
curl "http://127.0.0.1:1188/r=first_packet_stats.get_report()"
curl "http://127.0.0.1:1188/r=first_packet_stats.get_report(reset=True)"
```

报告结构：

- `uptime_s` / `installed`
- `connectivity`：`configured`（配置的 broker 条数）、`clients`（client 对象数）、
  `connected`（在线数）、`disconnected_hosts`（掉线的是谁）。一眼看出
  “配置 14、对象 14、在线 11、掉线 3 个是谁”，不用做减法。
- `requests`：请求首包延迟 `min/avg/max` 与逐 broker `wins/win_pct`。
- `pty_frames`：`unique/duplicates`、`first_frame_broker`、逐 broker
  `rx/wins/dups/win_pct`；`hb_frames`（不同心跳数）、`hb_dups`（同帧重复投递数）、
  `hb_gap_ms`（相邻两次**不同**心跳的本地到达间隔）。
- `uplink_publish`：`calls`、`brokers_queued`、`send_ms`、
  `first_packet_broker`（**首个完成批次的首个 ack** 所在 broker，不会被后续批次覆盖）、
  逐 broker `queued/failed/wins/send_ms`。
- `in_flight`：`batches`、`mids`、`stale_mids`（批次已结束但仍等不到 `on_publish`
  的僵尸 mid，超过 120 s TTL 会被清理）。

口径说明：QoS0 的 `on_publish` 只表示包已发离客户端，不代表 broker 已回执；
“真正到达”的证据由下行首包覆盖。所有时间都基于本地 `monotonic/perf_counter`，
不解析、也不信赖远端时间戳。

## 3. 多 broker 下的“重复投递”现象（不是 bug）

长时间统计常看到两类“异常”，它们都不是本客户端的 bug：

### 3.1 心跳周期分成 2.5 s 与 5 s 两组

真实心跳节拍是 5 s（服务端默认 `--heartbeat 5`）。约 2.5 s 的那一组是
“每条心跳在同一连接上被投递了两份”：相邻间隔变成 `0ms, 5s, 0ms, 5s…`，
平均值恰好 2.5 s，`hb_gap_ms.min` 会是 0.0 ms。实测抓包中同一条心跳
（同一 `hb` 值、同 topic、同 payload）在 `broker.hivemq.com` 上相隔 0.4 ms
到两次，`mqtt-dashboard.com` 相隔 0.1 ms，两个 EMQX 端点相隔约 48 ms；
其余 7 个 broker 各自只收到 1 次。

### 3.2 为什么恰好是这 4 个 broker

```
broker.emqx.io      + broker-cn.emqx.io        → 同一个 EMQX 公共集群的两个端点
broker.hivemq.com   + broker.mqtt-dashboard.com → 同一个 HiveMQ 公共集群的两个端点
```

四个 host 实际只对应两个物理集群。两端点计数几乎逐位相同（两个 EMQX 端点的
`rx/hb` 完全相同），且 `broker-cn.emqx.io` 的 `wins` 长期为 0（同集群里后到的
那份永远抢不到首达），都指向“同一集群、两个域名”。

重复的来源可能有多条，按与本次观测的吻合程度排序：

1. **同一集群被两个 host 各发布一次**：服务端 `publish_broadcast` 按 host 逐条发送，
   两个域名都指向同一集群时，集群实际收到两条 PUBLISH，于是集群内每条订阅连接
   都收到两份副本。与观测最吻合（每连接恰好两份，间隔为两次发布的时差）。
2. **同一 topic 被重复 SUBSCRIBE**：若一个会话里存在两条匹配同一 topic 的订阅，
   部分 broker（EMQX/HiveMQ 等）会“每条匹配订阅投递一份”，Mosquitto 类则合并成一份。
   这也解释了为什么只有部分 broker 出现双份。
3. **两个连接共享 client_id 或 client_id 为空**：MQTT 规范要求踢掉旧连接，
   但集群在会话迁移窗口内可能短暂双投。**本进程实测不适用**：14 个 client 的
   `client_id` 全部不同（形如 `mcd185a7…`），没有会话冲突。

结论：**这不是 bug**，而是“把同一公共集群的两个域名同时接入”带来的网络层现象。
它不影响主流程，只是让“按到达计数”的统计翻倍。需要的话可在配置层去重
（同一集群只保留一个域名）；统计层已做本地去重：PTY 帧按 `(sid, seq)`，
心跳按整帧内容比对（`hb_dups`），所以 `hb_gap_ms` 不会因为双投递变成 2.5 s。

### 3.3 延迟排名与胜率不同序

`wins` 统计的是“这次请求第一份副本先到的是谁”，每个请求只有一个赢家；某个 broker
的延迟块只采样“它恰好赢”的那些请求。因此平均延迟与胜率测的是不同分布，
本来就不该同序：平均慢但抖动小的 broker 可能赢得更多。

## 4. MQTT RPC 请求/响应流程与安全边界

### 4.1 客户端发送请求

客户端发起 `code` 请求时，如果自己配置了私钥（例如 `--key ...`），会在
`MultiMQTTManager.publish_broadcast()` 中为 `req_id` 追加签名：

- 原始 `req_id`：`20260911_173656.404 8d795f`
- 签名后：`20260911_173656.404 8d795f|6a0520...`
- 签名内容：`sign_msg = f"{base_req_id}|{code}|{timestamp}"`

这一步是“客户端发起命令”时的签名，不是服务端返回结果的签名。

### 4.2 服务端接收请求

服务端收到请求时，`MultiMQTTManager._make_on_message()` 会先检查是否存在 `code`
字段；只有服务端配置了 `server_public_key_bytes` 才会执行验签。

关键点：

- `server_public_key_bytes` 是服务端自己的公钥
- `client_private_key_bytes` 是客户端自己的私钥
- 两者是不同侧的密钥材料

因此：客户端启动时是否带私钥，决定它是否签名发请求；服务端启动时是否带公钥，
决定它是否验签收到的请求。`server_mqtt.py --pub ...` 只是“服务端启用验签”的启动参数，
不是客户端默认自动注入的公钥。

### 4.3 服务端返回结果

服务端处理 `code` 后，返回的是普通 JSON（没有 `code` 字段，因此不是新的命令请求）：

```python
{
    "req_id": "...",
    "r": ...,
    "stdout": ...,
    "ok": True,
    "server_time": ...,
    "server_from": ...,
}
```

### 4.4 客户端接收回包

客户端用回包里的 `req_id` 匹配 pending request。若客户端启用了私钥签名，而当前
没有已知服务端公钥，并且没有显式允许 `allow_no_server_pubkey_response=True`，
则应拒绝回包，避免“无公钥/无信任锚点”的服务端伪装成可信响应。默认是拒绝，不是静默放行。

```python
signed_req = bool(client_private_key_bytes)
no_server_pubkey = not bool(self.mqtt_net.server_public_key_bytes)

if signed_req and no_server_pubkey and not allow_no_server_pubkey_response:
    return   # 拦截无公钥服务器返回
```

这不是“客户端搞公钥”，而是在私钥模式下对未知服务器的返回做安全门槛。

### 4.5 错误根源与修法

问题不是“客户端不该碰公钥”，而是“之前把默认公钥注入到了客户端，导致流程被绕乱”：
一个正常的默认 server 被当成“带公钥的可信服务器”，绕过了本来应该出现的拒绝门槛。
正确修法是：不写死默认 server 公钥；让用户显式决定是否允许无公钥回包；
私钥模式 + 无已知服务端公钥 + 未允许时直接丢弃回包。

### 4.6 分工

- `client/client_mqtt.py`：发送签名请求、接收响应、应用“无公钥服务器回包”安全开关。
- `server_mqtt.py`：只负责本机作为 server，通过 `--pub` 配置自己的公钥，处理请求并返回结果。
- `multi_mqtt.py`：真正的 MQTT 网络层，处理签名封装、验签、去重和广播。

一句话：客户端签名发出 `code`，服务端验签执行；服务端返回普通结果；客户端在私钥模式下
只在“无已知公钥 + 未显式允许”时拒收回包。

## 5. 服务端是否需要配合网络层观测（本次不改）

PTY 服务端代码是在首次连接时由客户端动态下发的（`build_pty_start_code` 生成后经
MQTT 请求送到远端执行），所以“客户端代码”和“服务端代码”在同一会话里是配套的；
但代码跑在远端常驻 shell/tmux 会话中，改动需要新会话才生效。

结论：**当前统计不需要改服务端代码**。所有指标都只依赖客户端本地单调钟和收到的
消息内容：

- 上行：`publish` 返回的 mid 与 `on_publish` 回调，全在本地。
- 下行首包：本地 `pending_requests[start_time]` 与回调到达时刻。
- 心跳：只按本地到达间隔 + 整帧内容去重，不解析 `hb` 的值。
- 掉线/在线：`client.is_connected()`。

如果以后想让观测更精确，值得考虑的**可选**服务端改动（本次均不做）：

1. 心跳/数据帧里加 `seq` 或 `pid`（与本端时钟无关的发布序号）：可把“重复投递”
   与“两次不同心跳”彻底区分开，也不必再靠整帧内容比对。
2. 所有帧带上同一个发布 id：把“上行批次的 mid”与“下行首包”串起来，得到端到端的
   因果链（目前只能分别统计两个方向）。
3. 服务端按集群去重 broker 列表（同一公共集群只发一次）：从源头消除双投递，
   这是配置层修改，不是代码修改。
4. 不建议让服务端上报统计（会引入额外网络请求，违背“0 额外网络请求”的前提）；
   也不建议用服务端时间戳做延迟（远端时钟不可信，已实测出过 -1300 ms 的假延迟）。

## 6. 其他笔记与 TODO

- 如果两个 `./server_mqtt.py` 同时运行，client 只会显示最先到达的那个。

降低 CPU 与耗电（Low-Resource & Power Efficiency）：

- 去磁盘 IO：丢弃频繁写 SQLite 的操作，换用内存 `TTLCache` 实现 O(1) 消息查重，
  显著降低 CPU 负载与磁盘读写耗电。
- 事件驱动唤醒：客户端用 `threading.Event()` 实现零 CPU 占用等待，任一 Broker
  返回消息就立刻唤醒主线程。

多路抗丢包与首胜机制（Racing Protocol）：

- 客户端发送时：一条请求并发投递给多个不同 MQTT Broker。
- 服务端收到后：仅处理最先到达的那一条，后续相同 msg_id 被 `TTLCache` 抛弃。
- 服务端响应时：同样广播至所有 Broker；客户端仅接收最先到达的 Response 并解除阻塞。

`PythonExecutor` 的两种结果来源：

- `mqtt_server.py` 收到 code → `PythonExecutor.execute(code)` → `ast.parse()` 解析；
  最后一条语句是表达式（如 `3`）时，前面的语句执行完再对该表达式 `eval`，
  `format_result(3)` 转成 `"3"` 返回。
- 最后一条不是表达式而是赋值（如 `r=3`）时，进入普通 `exec`；执行后读 `locals["r"]`，
  server 格式化成 `"r": "3"` 返回。

TODO：加入非对称加密与解密支持。
