# ==================== MCP 服务：把常驻 PTY 挂成标准 MCP 工具服务器 ====================
#
# 背景：本进程已通过 ai_bridge + 本地 HTTP RPC 口把常驻 PTY 暴露给外部进程，
# 但 AI 客户端侧还缺一个标准工具协议入口。MCP（Model Context Protocol）是
# 各 AI 客户端的通用工具协议；装好 ``pip install "mcp[cli]"`` 后，main 在
# --mcp-port 默认 1189 上挂一个 MCP 服务（新版 streamable-http，旧版 SDK
# 退回 sse），AI 客户端即可把远端 shell 当普通 MCP 工具集持续使用：
#
#     claude mcp add --transport http pty http://192.168.1.3:1189/mcp
#     claude mcp add --transport sse  pty http://192.168.1.3:1189/sse
#
# 刻意设计：
# - mcp 包懒导入：没装包只记一条本地日志，PTY 本体功能零影响；
# - 服务跑在 daemon 线程：MCP 口卡死/崩溃绝不拖死终端渲染与主会话；
# - 工具只是 ai_bridge 的薄封装：与 HTTP RPC 口共用同一把 busy 锁与下行
#   分流队列，MCP / RPC / 本地窗口三路调用看到的会话状态完全一致；
# - mcp 1.x/2.x 双兼容：2.x 把 FastMCP 改名 MCPServer 且 host/port 从
#   构造器移到 run() 参数，这里按导入成败分别适配。

_MCP_SERVER = None   # 启动成功后保存 server 实例（诊断/测试用）


def _mcp_pick_transport(server, prefer: str) -> str:
    """选 HTTP transport：显式指定优先；自动时按 SDK 能力选。"""
    if prefer in ("streamable-http", "sse"):
        return prefer
    return "streamable-http" if hasattr(server, "streamable_http_app") else "sse"


def start_mcp_server(port, host="0.0.0.0", prefer: str = "") -> dict:
    """把常驻 PTY 挂成 MCP 工具服务器（daemon 线程，幂等）。

    工具集：pty_run / pty_send / pty_status / pty_log / pty_redraw，全部
    直通进程内 ai_bridge（与 HTTP RPC 口同源同锁）。返回启动结果 dict；
    任何失败只进本地日志，不影响 PTY 会话。
    """
    global _MCP_SERVER
    port = int(port)
    if port <= 0:
        return {"ok": False, "error": "mcp_port<=0，MCP 服务关闭"}
    if _MCP_SERVER is not None:
        return {"ok": True, "already": True, "port": port}
    instructions = (
        "远端服务器的常驻交互 PTY。pty_run 跑非交互命令并回收 rc/输出；"
        "交互提示（密码/y/n）用 pty_send 喂键；pty_status 看会话与 broker "
        "状态；pty_log 看本地诊断日志；画面冻结时 pty_redraw 自愈。"
        "同一 shell 串行执行，重叠调用返回 busy。")
    try:
        try:  # mcp 2.x：FastMCP 已改名 MCPServer
            from mcp.server.mcpserver import MCPServer as _K
            server_kind = "2"
        except ImportError:  # mcp 1.x
            from mcp.server.fastmcp import FastMCP as _K
            server_kind = "1"
        try:
            if server_kind == "2":  # 2.x 的 host/port 在 run() 参数里
                server = _K("pty", instructions=instructions)
            else:                   # 1.x 的 host/port 在构造器 Settings 里
                server = _K("pty", instructions=instructions,
                            host=str(host), port=int(port))
        except TypeError:  # 构造器不收这些参数的老版本：退回 settings 赋值
            server = _K("pty")
            try:
                server.settings.instructions = instructions
                if server_kind == "1":
                    server.settings.host = str(host)
                    server.settings.port = int(port)
            except Exception:
                pass
    except Exception as exc:
        _emit_local('[pty][MCP] mcp 包不可用，跳过（pip install "mcp[cli]" 启用）: %s\n' % exc)
        return {"ok": False, "error": "mcp 包不可用: %s" % exc}
    _MCP_SERVER = server

    @server.tool()
    def pty_run(cmd: str, timeout: float = 60.0, acquire_timeout: float = 2.0) -> dict:
        """在常驻远程 shell 里跑一条命令，等结束标记并回收输出与退出码。

        返回 {"ok", "rc", "out", "timed_out", ...}；命令串行执行，上一条
        未结束时立刻返回 {"ok": False, "busy": True}（可加大 acquire_timeout
        或稍后重试）。超时只停止回收，远端命令继续跑，可用 pty_send 干预
        或再 pty_run 收尾。
        """
        return ai_bridge.run(str(cmd), timeout=float(timeout),
                             acquire_timeout=float(acquire_timeout))

    @server.tool()
    def pty_send(data: str) -> dict:
        """向常驻 PTY 原样喂键，不回收输出：回答密码/y/n、发 Ctrl-C 等。

        例："y\\n" 确认提示、"\\x03" Ctrl-C、"vim file\\r" 打开编辑器。
        """
        return ai_bridge.send(data)

    @server.tool()
    def pty_status() -> dict:
        """查询常驻会话状态：是否 attached/已结束、是否忙、broker 在线数、
        终端写线程卡滞秒数（>2 建议 pty_redraw）等。"""
        return ai_bridge.status()

    @server.tool()
    def pty_log(n: int = 100) -> str:
        """读最近 n 行本地诊断日志（broker 连接/RPC 请求/告警），n 夹在 1~500。"""
        return get_log(max(1, min(int(n), 500)))

    @server.tool()
    def pty_redraw() -> dict:
        """终端写线程卡滞自愈：必要时切换本地写线程，并通知远端整屏重绘。"""
        steps = []
        if _OUT_WRITER is not None and _OUT_WRITER.stall_seconds() > 2.0:
            steps.append(reset_out_writer())
        pty = getattr(ai_bridge, "pty", None)
        if pty is None or getattr(pty, "end_reason", None):
            return {"ok": False, "error": "pty 未连接或已结束", "steps": steps}
        try:
            pty.configure(redraw=True)
        except Exception as exc:
            steps.append({"redraw": False, "error": str(exc)})
            return {"ok": False, "steps": steps}
        steps.append({"redraw": True})
        return {"ok": True, "steps": steps}

    transport = _mcp_pick_transport(server, (prefer or "").strip().lower())

    def _serve():
        try:
            if server_kind == "2":  # 2.x：host/port 是 run() 的 transport 参数
                server.run(transport=transport, host=str(host), port=port)
            else:
                server.run(transport=transport)
        except Exception as exc:
            _emit_local("[pty][MCP] MCP 服务异常退出: %s\n" % exc)

    threading.Thread(target=_serve, name="pty-mcp", daemon=True).start()
    path = "/mcp" if transport == "streamable-http" else "/sse"
    _emit_local("[pty][MCP] MCP 工具服务已开启（%s）：http://%s:%d%s  "
                "工具：pty_run/pty_send/pty_status/pty_log/pty_redraw\n"
                % (transport, host, port, path))
    return {"ok": True, "transport": transport, "port": port, "path": path}


