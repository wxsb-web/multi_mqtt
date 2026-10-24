# ============================ CLI ============================

def build_parser() -> argparse.ArgumentParser:
    # 选项名一律由文件前面的 alias_xxx 别名表 + client_mqtt._cli_opts 生成，
    # 与本地魔术栏命令、rpc(**ka) 别名三处永远一致；禁止再硬编码 "--xxx"。
    # dest 保持历史字段名（args.interval/args.port 等），调用方无需改动。
    p = argparse.ArgumentParser(
        prog="pty_client_mqtt",
        description="PTY over MQTT：SSH 式远程交互终端（常驻 shell，逐键过 broker）")
    # 连接参数（topic/key/allow/timeout）：直接复用 client_mqtt 别名表
    add_connection_args(p, default_timeout=30.0)
    # PTY 会话参数
    p.add_argument(*_cm._cli_opts(*alias_shell), dest="shell", default="",
                   help="远端 shell，默认服务端用户登录 shell（$SHELL/passwd）")
    p.add_argument(*_cm._cli_opts(*alias_term), dest="term", default="",
                   help="TERM，默认本地 $TERM 或 xterm-256color")
    p.add_argument(*_cm._cli_opts(*alias_cwd), dest="cwd", default=None,
                   help="启动目录，默认远端 HOME")
    p.add_argument(*_cm._cli_opts(*alias_interval), dest="interval",
                   type=float, default=0.0,
                   help="服务端主动推送最小间隔秒：0=实时（默认），>0 攒批")
    p.add_argument(*_cm._cli_opts(*alias_no_login), dest="no_login",
                   action="store_true",
                   help="不使用 login shell（默认 argv0 带 - 前缀）")
    p.add_argument(*_cm._cli_opts(*alias_ttl), dest="ttl",
                   type=float, default=DEFAULT_PTY_TTL,
                   help="孤儿会话最长存活秒（默认 12h，上限 24h）")
    p.add_argument(*_cm._cli_opts(*alias_heartbeat), dest="heartbeat",
                   type=float, default=5.0,
                   help="服务端心跳间隔秒：0=关闭（默认 5s）")
    p.add_argument(*_cm._cli_opts(*alias_dead_timeout), dest="dead_timeout",
                   type=float, default=15.0,
                   help="多久收不到任何远端帧（输出/心跳）即判定服务器已死"
                        "并直接退出（默认 15s，实际不小于 3 倍心跳；"
                        "0=不检测，心跳关闭时自动失效）")
    p.add_argument(*_cm._cli_opts(*alias_size), dest="size", default=None,
                   help="强制窗口 ROWSxCOLS，如 24x100；默认取本地终端大小")
    p.add_argument(*_cm._cli_opts(*alias_detach_key), dest="detach_key",
                   default="ctrl-]",
                   help="本地脱离键：只断开 client，远端 tmux/shell 继续运行"
                        "（默认 ctrl-]，沿用 telnet 惯例，不与 tmux 前缀冲突；"
                        "可写 ctrl-a/f9/insert 等名字，none 禁用）")
    p.add_argument(*_cm._cli_opts(*alias_menu_key), dest="menu_key",
                   default="ctrl-alt-insert",
                   help="本地命令栏热键：detach、status、热调 interval/"
                        "heartbeat/ttl/dead，不会把按键发到远端（默认 "
                        "ctrl-alt-insert；名字写法同脱离键，none 禁用）")
    p.add_argument(*_cm._cli_opts(*alias_rpc_port), dest="port",
                   type=int, default=1188,
                   help="本地 AI 控制口 HTTP RPC 端口（默认 1188）；"
                        "外部进程经它调用 ai_bridge 复用本 PTY，不再重连 broker；0=关闭")
    p.add_argument(*_cm._cli_opts(*alias_rpc_host), dest="host",
                   default="0.0.0.0",
                   help="本地 AI 控制口绑定地址（默认 0.0.0.0）；仅本机调用建议 127.0.0.1")
    p.add_argument(*_cm._cli_opts(*alias_mcp_port), dest="mcp_port",
                   type=int, default=1189,
                   help="MCP 工具服务端口（默认 1189）：pip install \"mcp[cli]\" 后 "
                        "AI 客户端经 MCP 协议直接调用常驻 PTY（pty_run 等工具）；0=关闭")
    p.add_argument(*_cm._cli_opts(*alias_mcp_transport), dest="mcp_transport",
                   default="",
                   help="MCP 传输类型：留空自动（优先 streamable-http，旧版 mcp 包 "
                        "退回 sse），可显式 streamable-http / sse")
    # SSH 式可选位置参数：连接成功后自动敲进常驻 shell 的前置命令，如
    # ``pty_client_mqtt.py -t q -k *** "tmux at"``；REMAINDER 保证命令自身
    # 的 -x 选项（tmux attach -d）不会被本客户端解析。命令结束后会话继续，
    # 人仍留在远端 shell / 全屏程序里。
    p.add_argument(alias_command[0], nargs=argparse.REMAINDER,
                   help="可选：连接后自动执行的前置命令（SSH 式），如 \"tmux at\"")

    return p


def main(argv=None) -> int:
    global _LOG_MIRROR_STDERR,transport
    args = build_parser().parse_args(argv)

    if args.size:
        try:
            r_txt, c_txt = args.size.lower().split("x", 1)
            rows, cols = int(r_txt), int(c_txt)
        except ValueError:
            sys.stderr.write("[ERROR] --size 格式应为 ROWSxCOLS，如 24x100\n")
            return 2
    else:
        sz = shutil.get_terminal_size((80, 24))
        rows, cols = sz.lines, sz.columns

    if not sys.stdin.isatty():
        sys.stderr.write("[ERROR] PTY 需要一个交互式本地终端（stdin 不是 TTY）\n")
        return 2
    
    _enable_output_vt()
    signed = bool(str(args.key or "").strip())
    t_conn = time.monotonic()
    _info("正在并发连接 %d 个公共 MQTT broker（多路径冗余，首个连上即继续，通常不足 1 秒）..."
          % len(BROKER_LIST))
    _info("request_topic=%s  reply_topic=%s  请求签名=%s  允许未验签回包=%s"
          % (args.request_topic, args.reply_topic,
             "是" if signed else "否", "是" if args.allow else "否"))
    transport = MqttTransport(
        request_topic=args.request_topic, reply_topic=args.reply_topic,
        private_key=args.key, allow_no_pub=args.allow)
    online, total, hosts = _broker_status(transport)
    _info("MQTT 就绪：在线 broker %d/%d，建连耗时 %.1f 秒"
      % (online, total, time.monotonic() - t_conn)
      + ("；在线节点：" + ", ".join(hosts) if hosts else ""))
    # if hosts:
        # _info("在线节点：" + ", ".join(hosts))
    # el
    if not hosts and total:
        _emit_local("[WARN] 当前没有任何 broker 在线，握手大概率超时\n")
        
    if args.port:
        import server_http
        # 注意：持久命名空间在此刻快照一次，ai_bridge/get_log 都是模块级
        # 对象引用，会话建立后 attach/feed 对 HTTP 调用方立即生效。
        # 起口成功即切换为"本地日志只进环形缓冲"：包括下面的启动 _info、
        # 每笔 [RPC] 请求行和 root 上的 MultiMQTT/paho logging，终端零写入；
        # 起口失败则保留 stderr 镜像，致命错误用户照样看得见。
        _LOG_MIRROR_STDERR = False
        try:
            ghs = server_http.start_rpc_server(
                port=args.port, ip=args.host, globals=globals(), locals=locals(),
                log_sink=_emit_local,
                websocket_handler=_LOG_HUB.serve, websocket_path="/wslog",
                redirect_root="/log_html(p)")
        except Exception:
            _LOG_MIRROR_STDERR = True
            raise
        # 起口成功后再接管 stdout/stderr/logging：浏览器日志台与 RPC 取日志
        # 两个查看通道都已就绪，PTY 终端从此只渲染远端 shell。
        _install_log_capture()
        lan = _lan_ip()
        _info(f"本地 AI 控制口已开启：http://{lan}:{args.port}/"
              f"根路径自动跳转 /log_html(p)")

    # MCP 工具服务独立于 HTTP RPC 口：不依赖 --port（工具直通进程内
    # ai_bridge，不经 HTTP 转发）；没装 mcp 包只记日志，PTY 照常工作。
    if getattr(args, "mcp_port", 0):
        mcp_res = start_mcp_server(
            port=int(args.mcp_port), host=args.host,
            prefer=getattr(args, "mcp_transport", "") or "")
        if mcp_res.get("ok") and not mcp_res.get("already"):
            _info(f"MCP 工具服务已开启（{mcp_res['transport']}）："
                  f"http://{_lan_ip()}:{mcp_res['port']}{mcp_res['path']}  "
                  f"AI 客户端添加：claude mcp add --transport http pty "
                  f"http://{_lan_ip()}:{mcp_res['port']}{mcp_res['path']}")

    try:
        return run_session(transport, args, rows, cols)
    except RemoteError as exc:
        _stderr_write(f"[ERROR] {type(exc).__name__}: {exc}\n")
        # main 即将 return 退出进程，给 stderr 写线程 1s 把错误落盘
        if _ERR_WRITER is not None:
            _ERR_WRITER.close(timeout=1.0)
        return 2
    finally:
        transport.close()


if __name__ == "__main__":
    sys.exit(main())
