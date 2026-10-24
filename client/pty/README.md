# client/pty —— pty_client_mqtt 的多文件版本

单文件 [`../pty_client_mqtt.py`](../pty_client_mqtt.py) 由本目录**生成**：
`parts/` 分片是唯一编辑入口，改完用生成器重组写回。

## 目录结构

```
pty/
├── parts/                    # 单文件的分片（编号即拼接顺序，唯一编辑入口）
│   ├── 000_head.py           # 模块 docstring / 导入 / sys.path 与环境引导
│   ├── 010_alias.py          # 统一别名表（CLI 选项 + 魔术栏 + rpc 别名共用）
│   ├── 020_console.py        # 本地终端 raw 输入/输出、输入转码、剪贴板粘贴
│   ├── 030_termwrite.py      # 二进制流锁定、_TerminalWriter 异步写线程、终端复位、控制台守卫
│   ├── 040_hotkey.py         # 热键按键名解析（ctrl-] / ctrl-alt-insert → VT 序列）
│   ├── 050_status.py         # broker 在线状态、stderr 异步写出
│   ├── 060_logring.py        # 本地日志环形缓冲 _LogRing / _RingTextIO / _RingLogHandler
│   ├── 070_loghub.py         # 浏览器实时日志台 LogHub（WebSocket 快照+增量）
│   ├── 075_logpage.py        # 日志台 HTML 页面、ring 日志接管安装、_info
│   ├── 080_writerdiag.py     # 写线程诊断与自愈、QuickEdit 选择模式自动解除
│   ├── 090_aibridge.py       # AI 桥：_strip_ansi、base64 严格编码、AIBridge、ai_bridge
│   ├── 095_aicallers.py      # AI 桥调用方便捷函数（local_rpc / ai_pty_run / …）
│   ├── 097_mcp_server.py     # MCP 服务：pty_run/pty_send/pty_status/pty_log/pty_redraw
│   ├── 100_magic.py          # 本地命令栏（魔术命令 _run_magic / 参数规格表）
│   ├── 105_exit.py           # _hard_exit 硬退出、_join_pre_command
│   ├── 110_session.py        # run_session 会话主循环
│   └── 120_cli.py            # build_parser / main / __main__ 入口
├── __init__.py               # 按序把全部分片 exec 进同一命名空间（包模式入口）
├── __main__.py               # python -m client.pty 入口
├── generate_single_file.py   # 分片 → 单文件重组器（含字节 + AST 比对）
└── README.md
```

## 用法

```bash
# 多文件版：包导入（等价 from client.pty_client_mqtt import ...）
from client.pty import ai_pty_run, ai_pty_status, ai_bridge, start_mcp_server

# 多文件版：命令行（等价 python client/pty_client_mqtt.py）
python -m client.pty

# 改完分片后重组写回正式单文件（parts 是唯一编辑入口）
python client/pty/generate_single_file.py -o ../pty_client_mqtt.py -f

# 只比对磁盘上的单文件与分片拼接结果（字节+AST）
python client/pty/generate_single_file.py --check
```

## MCP 工具服务

`pip install "mcp[cli]"` 后，运行中的 pty 进程把常驻 PTY 持续暴露为标准 MCP
工具：**默认复用 `--port` 的 1188 口**（路径 `/mcp`，与 RPC/浏览器日志台同口
并存，不新开端口——给 `server_http.RPCRequestHandler` 打保留路径前置分流，
参考 HuggingFace app.py 的中间件写法；mcp 2.x 走 streamable-http，旧版退回
`/sse`+`/messages`；未装包只记一条日志，PTY 照常工作）：

```bash
claude mcp add --transport http pty http://192.168.1.3:1188/mcp
# 旧版 SDK：claude mcp add --transport sse pty http://192.168.1.3:1188/sse
```

工具：`pty_run`（跑命令回收 rc/输出）、`pty_send`（喂交互键）、`pty_status`
（会话/broker 状态）、`pty_log`（本地诊断日志）、`pty_redraw`（写卡滞自愈
+整屏重绘）。与 1188 HTTP RPC 口同源同锁，三路调用状态一致。`--no-mcp` 关闭；
`--mcp-port N`（N>0）改为另起独立端口（`--port 0` 关闭 RPC 口时仍要 MCP 用）。

## 规则

1. **只编辑 `parts/` 分片**；不要手改 `client/pty_client_mqtt.py`
   （它由生成器产出）。
2. 分片之间**不能调整顺序**、不能留未编号文件；新增分片用等宽 3 位数字
   前缀插入编号序列（按文件名排序即拼接顺序）。
3. 改完分片后 `python client/pty/generate_single_file.py -o ../pty_client_mqtt.py -f`
   重组写回；`--check` 可随时校验分片与单文件是否同步（字节+AST）。
4. 分片不是独立模块，不要单独 import；包模式一律从 `client.pty` 进。

## 设计说明：为什么分片不能是互相 import 的真模块

原文件里 `run_session` 会 `global _OUT_WRITER` 重新绑定写线程，
`_stderr_writer` / `reset_out_writer` / `main` 也会跨段改写
`_ERR_WRITER`、`_LOG_MIRROR_STDERR` 等模块级可变全局。若拆成真模块，
各模块经 `from .x import y` 拿到的是**绑定快照**，重绑定只落在定义模块，
其他段永远读到旧值——语义必然漂移（终端复位/硬退出路径会拿到过期的
`None`）。因此 `__init__.py` 采用**共享命名空间顺序 exec**：加载语义与
单文件完全一致；重组侧则因为分片是逐字节切片，拼接结果与原文件
**逐字节一致**（sha256 相同），功能不变有硬证据。
