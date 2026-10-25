# ==================== MCP 服务：把常驻 PTY 挂成标准 MCP 工具服务器 ====================
#
# 背景：本进程已通过 ai_bridge + 本地 HTTP RPC 口把常驻 PTY 暴露给外部进程，
# 但 AI 客户端侧还缺一个标准工具协议入口。MCP（Model Context Protocol）是
# 各 AI 客户端的通用工具协议；装好 ``pip install "mcp[cli]"`` 后，main 默认
# 把 MCP 端点**挂在 --port 已起的 RPC 服务器上**（路径 /mcp，与 RPC / 浏览器
# 日志台同口复用，不新开端口），AI 客户端即可把远端 shell 当普通 MCP 工具
# 集持续使用：
#
#     claude mcp add --transport http pty http://192.168.1.3:1188/mcp
#
# 同口复用的做法（参考 HuggingFace app.py 的保留路径分流中间件）：给
# server_http.RPCRequestHandler 包一层前置分发——命中 /mcp（sse 退回模式下
# 是 /sse、/messages）的请求桥接给 MCP 的 ASGI 应用，其余路径原样走既有
# RPC / WebSocket / 日志台逻辑。桥接要点：
# - mcp 1.x/2.x 双兼容：2.x 把 FastMCP 改名 MCPServer，ASGI 应用从
#   streamable_http_app()/sse_app() 取，SDK 不支持时逐级退回；
# - ASGI 应用跑在本模块专用事件循环线程上；lifespan（session manager
#   等启动上下文）手动驱动，等价 uvicorn 的启动阶段；
# - MCP 工具只是 ai_bridge 的薄封装：与 HTTP RPC 口共用同一把 busy 锁与
#   下行分流队列，MCP / RPC / 本地窗口三路调用看到的会话状态完全一致；
# - mcp 包懒导入：没装包只记一条本地日志，PTY 本体功能零影响；
# - 服务不碰主线程：桥接在 server_http 的请求线程 + 专用循环线程里完成。
#
# --mcp-port>0 时退回独立端口模式（自起 uvicorn，供 --port 0 关闭 RPC 口
# 但仍要 MCP 的场景）；--no-mcp 完全关闭。

import asyncio

_MCP_SERVER = globals().setdefault('_MCP_SERVER', None)  # 启动/挂载成功后保存 MCP server 实例（诊断/测试用）；setdefault 防 reload 重绑丢引用
_MCP_ASGI_APP = globals().setdefault('_MCP_ASGI_APP', None)  # 挂 1188 口模式下的 ASGI 应用。最要命的一个：已打进 RPCRequestHandler 的前置分发 wrapper 运行期按模块全局名查本变量，reload 把名字重绑回 None 后 /mcp 请求静默回落成普通 RPC 执行——setdefault 让旧钩子跨 reload 继续拿到活应用
_MCP_HTTP_PATHS = globals().setdefault('_MCP_HTTP_PATHS', frozenset())  # 需在 RPC 口上拦截分流的路径集合；与 _MCP_ASGI_APP 同生共死，reload 丢集合同样让分流失效
_MCP_LOOP = globals().setdefault('_MCP_LOOP', None)  # ASGI 专用事件循环（后台线程）；reload 丢引用会让 _mcp_ensure_loop 再起第二条循环线程
_MCP_LOOP_LOCK = globals().setdefault('_MCP_LOOP_LOCK', threading.Lock())  # 循环线程启动互斥锁；reload 重造新锁 + _MCP_LOOP 名字回 None = 判空双起循环线程，锁必须与引用一起跨 reload 保活

_MCP_INSTRUCTIONS = (
    "远端服务器的常驻交互 PTY。pty_run 跑非交互命令并回收 rc/输出；"
    "交互提示（密码/y/n）用 pty_send 喂键；pty_status 看会话与 broker "
    "状态；pty_log 看本地诊断日志；画面冻结时 pty_redraw 自愈。"
    "同一 shell 串行执行，重叠调用返回 busy。")


def _mcp_pick_transport(server, prefer: str) -> str:
    """选 HTTP transport：显式指定优先；自动时按 SDK 能力选。"""
    if prefer in ("streamable-http", "sse"):
        return prefer
    return "streamable-http" if hasattr(server, "streamable_http_app") else "sse"


def _new_mcp_server(host=None, port=None):
    """构造 MCP server 实例（mcp 1.x/2.x 双兼容），返回 (server, kind)。

    mcp 2.x 把 FastMCP 改名 MCPServer 且 host/port 移进 run() 参数；1.x
    的 host/port 在构造器 Settings 里。host/port 仅独立端口模式需要。
    不可用时返回 (None, None)。
    """
    try:
        try:  # mcp 2.x
            from mcp.server.mcpserver import MCPServer as _K
            kind = "2"
        except ImportError:  # mcp 1.x
            from mcp.server.fastmcp import FastMCP as _K
            kind = "1"
        try:
            if kind == "1" and port is not None:
                return _K("pty", instructions=_MCP_INSTRUCTIONS,
                          host=str(host), port=int(port)), kind
            return _K("pty", instructions=_MCP_INSTRUCTIONS), kind
        except TypeError:  # 构造器不收 instructions/host/port 的老版本
            server = _K("pty")
            if kind == "1" and port is not None:
                try:
                    server.settings.instructions = _MCP_INSTRUCTIONS
                    server.settings.host = str(host)
                    server.settings.port = int(port)
                except Exception:
                    pass
            return server, kind
    except Exception as exc:
        _emit_local('[pty][MCP] mcp 包不可用，跳过（pip install "mcp[cli]" 启用）: %s\n' % exc)
        return None, None


def _mcp_register_tools(server) -> None:
    """注册五个 MCP 工具（直通进程内 ai_bridge，与 HTTP RPC 口同源同锁）。"""

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


def _mcp_loop_main(loop):
    """ASGI 专用事件循环线程体。"""
    global _MCP_LOOP
    asyncio.set_event_loop(loop)
    _MCP_LOOP = loop
    loop.run_forever()


def _mcp_ensure_loop():
    """懒启动专用事件循环线程，返回 loop（桥接与 lifespan 共用）。"""
    global _MCP_LOOP
    with _MCP_LOOP_LOCK:
        if _MCP_LOOP is None or _MCP_LOOP.is_closed():
            loop = asyncio.new_event_loop()
            threading.Thread(target=_mcp_loop_main, args=(loop,),
                             name="pty-mcp-loop", daemon=True).start()
            while _MCP_LOOP is None:
                time.sleep(0.01)
    return _MCP_LOOP


def _mcp_drive_lifespan(app, loop) -> None:
    """手动驱动 ASGI lifespan（等价 uvicorn 启动阶段）：起 session manager 等。

    只发 startup 不发 shutdown：常驻服务随进程退出（线程为 daemon）。
    """
    q = asyncio.Queue()

    async def receive():
        return await q.get()

    async def send(message):
        if message.get("type") == "lifespan.startup.failed":
            _emit_local("[pty][MCP] MCP lifespan 启动失败: %s\n"
                        % message.get("message", "?"))

    async def _drive():
        await q.put({"type": "lifespan.startup"})
        await app({"type": "lifespan", "asgi": {"version": "3.0",
                                                "spec_version": "2.0"},
                   "state": {}}, receive, send)

    asyncio.run_coroutine_threadsafe(_drive(), loop)


class _MCPHttpBridge:
    """1188 口 /mcp 请求 -> MCP ASGI 应用的同步桥。

    server_http 每请求一线程；ASGI 应用跑在专用事件循环线程上：请求体
    一次性读齐，receive 首次返回后即挂起；send 的响应字节由循环线程直接
    写回（此刻请求线程正阻塞等待，单写者安全）。无 content-length 的流式
    响应按 Connection: close 收尾；SSE 长连接由客户端断开，写失败即整条
    请求结束。
    """

    def __init__(self, handler):
        self.h = handler

    def run(self, app) -> None:
        h = self.h
        path, _, query = h.path.partition("?")
        try:
            length = int(h.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        body = h.rfile.read(length) if length > 0 else b""
        body_state = {"sent": False}
        state = {"started": False, "streaming": False}

        async def receive():
            if not body_state["sent"]:
                body_state["sent"] = True
                return {"type": "http.request", "body": body,
                        "more_body": False}
            await asyncio.Event().wait()  # 请求已完整读入：挂起等断开

        async def send(message):
            if message["type"] == "http.response.start":
                state["started"] = True
                h.log_request(message["status"])
                h.send_response_only(message["status"])
                has_len = False
                for k, v in message.get("headers", []):
                    name = bytes(k).decode("latin-1")
                    value = bytes(v).decode("latin-1")
                    low = name.lower()
                    if low in ("connection", "transfer-encoding"):
                        continue  # 由桥统一决定：流式时补 Connection: close
                    if low == "content-length":
                        has_len = True
                    h.send_header(name, value)
                state["streaming"] = not has_len
                if state["streaming"]:
                    h.send_header("Connection", "close")
                h.end_headers()
            elif message["type"] == "http.response.body":
                chunk = message.get("body", b"") or b""
                if chunk:
                    h.wfile.write(chunk)
                    h.wfile.flush()
                if state["streaming"] and not message.get("more_body"):
                    h.close_connection = True

        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": h.request_version.rsplit("/", 1)[-1] or "1.1",
            "method": h.command,
            "scheme": "http",
            "path": path,
            "query_string": query.encode("latin-1"),
            "root_path": "",
            "server": (h.server.server_address[0], h.server.server_address[1]),
            "client": (h.client_address[0], h.client_address[1]),
            "headers": [(k.lower().encode("latin-1"), v.encode("latin-1"))
                        for k, v in h.headers.items()],
        }
        try:
            fut = asyncio.run_coroutine_threadsafe(app(scope, receive, send),
                                                   _mcp_ensure_loop())
            fut.result()
        except (ConnectionError, BrokenPipeError, OSError):
            h.close_connection = True  # 客户端断开：正常收尾
        except Exception as exc:
            _emit_local("[pty][MCP] /mcp 桥接异常: %r\n" % exc)
            h.close_connection = True
            if not state["started"]:
                try:
                    h.send_error(502, "MCP bridge error")
                except Exception:
                    pass


def _mcp_hook_handler(Handler) -> None:
    """给 RPCRequestHandler 打前置分发钩子（幂等）：保留路径分流给 ASGI 桥。"""
    if not hasattr(Handler, "do_DELETE"):
        # BaseHTTPRequestHandler 对未实现方法回 501；补上让 /mcp 的 DELETE
        # （MCP 会话结束信号）有机会被钩子接管，其余 DELETE 维持 501 原状。
        def _do_delete(self):
            self.send_error(501, "Unsupported method (%r)" % self.command)
        Handler.do_DELETE = _do_delete

    def _wrap(fn):
        def wrapper(self, *args, **kwargs):
            if (_MCP_ASGI_APP is not None
                    and self.path.split("?", 1)[0] in _MCP_HTTP_PATHS):
                _MCPHttpBridge(self).run(_MCP_ASGI_APP)
                return None
            return fn(self, *args, **kwargs)
        wrapper.__name__ = getattr(fn, "__name__", "handler")
        wrapper.__doc__ = getattr(fn, "__doc__", None)
        wrapper._mcp_wrapped = True  # 幂等键挂在被包装函数上而非模块级 _MCP_HOOKED 标志：标志在 reload 后重置 False 会把同一个类再包一层（请求链上叠双层路径检查），而函数对象自带标记跨 reload 永不失真
        return wrapper

    for name in ("do_GET", "do_POST", "do_DELETE"):
        base = getattr(Handler, name, None)
        # 已带 _mcp_wrapped 的（含 reload 前包过的旧类）直接跳过；server_http 被
        # reload 出新类时此处自然给新类补钩子，旧类旧钩子经 setdefault 保住的
        # _MCP_ASGI_APP 继续工作，两边都不脱靶
        if base is not None and not getattr(base, "_mcp_wrapped", False):
            setattr(Handler, name, _wrap(base))


def mount_mcp_on_rpc(prefer: str = "", host: str = "0.0.0.0") -> dict:
    """把 MCP 端点挂到 --port 已起的 HTTP RPC 服务器上（同口复用，不新开端口）。

    1188 一个口上 RPC / 浏览器日志台 / MCP 三种服务并存；返回启动结果
    dict；任何失败只进本地日志，不影响 PTY 会话。
    """
    global _MCP_SERVER, _MCP_ASGI_APP, _MCP_HTTP_PATHS  # 原 _MCP_HOOKED 模块标志已删除：幂等改由被包装函数上的 _mcp_wrapped 标记承担，标志在 reload 后重置 False 会失真
    if _MCP_ASGI_APP is not None:
        return {"ok": True, "already": True, "paths": sorted(_MCP_HTTP_PATHS)}
    try:
        import server_http
    except Exception as exc:
        return {"ok": False, "error": "server_http 不可用: %s" % exc}
    server, kind = _new_mcp_server()
    if server is None:
        return {"ok": False, "error": "mcp 包不可用"}
    _mcp_register_tools(server)

    app, paths = _mcp_build_asgi(server, prefer, host)
    if app is None or not paths:
        return {"ok": False, "error": "当前 mcp SDK 未提供可挂载的 HTTP 应用"}

    _mcp_hook_handler(server_http.RPCRequestHandler)  # 无条件调用：钩子按 fn._mcp_wrapped 自幂等，不再依赖会被 reload 重置的模块标志
    loop = _mcp_ensure_loop()
    _mcp_drive_lifespan(app, loop)
    _MCP_SERVER = server
    _MCP_HTTP_PATHS = frozenset(paths)
    _MCP_ASGI_APP = app
    _emit_local("[pty][MCP] MCP 工具服务已挂到 RPC 口：路径 %s（与 RPC/日志台同口复用）"
                "  工具：pty_run/pty_send/pty_status/pty_log/pty_redraw\n"
                % "、".join(sorted(paths)))
    return {"ok": True, "paths": sorted(paths)}


def _mcp_build_asgi(server, prefer: str, host: str):
    """按 SDK 能力取 ASGI 应用与它占用的 HTTP 路径（优先 streamable-http）。"""
    if prefer != "sse" and hasattr(server, "streamable_http_app"):
        try:
            try:
                app = server.streamable_http_app(host=str(host))
            except TypeError:
                app = server.streamable_http_app()
            base = str(getattr(getattr(server, "settings", None),
                               "streamable_http_path", None) or "/mcp").rstrip("/")
            return app, {base, base + "/"}
        except Exception as exc:
            _emit_local("[pty][MCP] streamable-http 应用创建失败，改试 sse: %r\n" % exc)
    if hasattr(server, "sse_app"):
        try:
            try:
                app = server.sse_app(host=str(host))
            except TypeError:
                app = server.sse_app()
            st = getattr(server, "settings", None)
            sse = str(getattr(st, "sse_path", None) or "/sse").rstrip("/")
            msg = str(getattr(st, "message_path", None) or "/messages").rstrip("/")
            return app, {sse, sse + "/", msg, msg + "/"}
        except Exception as exc:
            _emit_local("[pty][MCP] sse 应用创建失败: %r\n" % exc)
    return None, set()


def start_mcp_server(port, host="0.0.0.0", prefer: str = "") -> dict:
    """独立端口模式：在 daemon 线程里自起 MCP HTTP 服务（--mcp-port>0 时用）。

    挂 1188 口的常规路径见 mount_mcp_on_rpc。返回启动结果 dict；任何失败
    只进本地日志，不影响 PTY 会话。
    """
    global _MCP_SERVER
    port = int(port)
    if port <= 0:
        return {"ok": False, "error": "mcp_port<=0，独立 MCP 服务未开启"}
    if _MCP_SERVER is not None:
        return {"ok": True, "already": True, "port": port}
    server, kind = _new_mcp_server(host=host, port=port)
    if server is None:
        return {"ok": False, "error": "mcp 包不可用"}
    _mcp_register_tools(server)
    _MCP_SERVER = server

    transport = _mcp_pick_transport(server, (prefer or "").strip().lower())

    def _serve():
        try:
            if kind == "2":  # 2.x：host/port 是 run() 的 transport 参数
                server.run(transport=transport, host=str(host), port=port)
            else:
                server.run(transport=transport)
        except Exception as exc:
            _emit_local("[pty][MCP] MCP 服务异常退出: %s\n" % exc)

    threading.Thread(target=_serve, name="pty-mcp", daemon=True).start()
    path = "/mcp" if transport == "streamable-http" else "/sse"
    _emit_local("[pty][MCP] MCP 工具服务已开启（%s 独立端口）：http://%s:%d%s  "
                "工具：pty_run/pty_send/pty_status/pty_log/pty_redraw\n"
                % (transport, host, port, path))
    return {"ok": True, "transport": transport, "port": port, "path": path}


