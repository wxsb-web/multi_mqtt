#!/usr/bin/env python3
from multi_mqtt import BROKER_LIST, ConnectionQualityStats, MultiMQTTManager, stime, utc_ms, _describe_public_key
import time,logging,sys
from rpc_executor import PythonExecutor, format_result

logger = logging.getLogger("Server")
REQUEST_TOPIC = "sys/device/request"
DEFAULT_REPLY_TOPIC = "sys/device/response"

# --- [global_topic_list 监听 topic 定期报告] 默认配置 ---
# 命名说明：stats.get_report 是网络连接质量报告；本组是"监听 topic 列表"报告，
# 定期发布到 global_topic_list，供新订阅者经 Retain 消息发现服务端监听 topic
DEFAULT_GLOBAL_TOPIC_LIST = "global_topic_list"          # 报告发布到的 topic
DEFAULT_GLOBAL_TOPIC_LIST_INTERVAL = 3600.0              # 发布间隔：空闲期每小时一次
DEFAULT_GLOBAL_TOPIC_LIST_IDLE_THRESHOLD = 86400.0       # 距上次下发指令超过 24 小时视为空闲（可配置）
DEFAULT_GLOBAL_TOPIC_LIST_CHECK_INTERVAL = ConnectionQualityStats.PING_INTERVAL  # 默认复用连接质量 ping 间隔（600s），与 ping 同节拍统一时间驱动避免交错唤醒；可配置为更小值如 15 便于测试
# 发布参数（经 **ka 一路透传到 paho-mqtt）：QoS1+Retain，
# broker 会连同 QoS 一起保存保留消息，新订阅者上线即收到最新 topic 列表
DEFAULT_GLOBAL_TOPIC_LIST_PUBLISH_KWARGS = {"qos": 1, "retain": True}


def _is_verify_enabled(mqtt_net):
    """
    判断当前 MQTT 网络层是否真的处于"验签"状态。

    MultiMQTTManager 里的关键属性：
      - server_public_key_bytes: 已解析成 PEM bytes（None 表示未配置）
      - server_vk:              ecdsa.VerifyingKey 对象（None 表示未启用）
      - _server_vk_invalid:     配置了公钥但解析失败时为 True

    返回 (enabled: bool, reason: str)
    """
    if getattr(mqtt_net, "_server_vk_invalid", False):
        return False, "公钥解析失败，验签未启用！"
    if getattr(mqtt_net, "server_vk", None) is not None:
        return True, "启用"
    if getattr(mqtt_net, "server_public_key_bytes", None):
        # 有 PEM bytes 但没有 vk，说明构造时被异常吞掉过，需要显式暴露
        return False, "公钥已配置但 VerifyingKey 未初始化！"
    return False, "关闭（接收所有消息）"


class MQTTServer:
    def __init__(
        self,
        globals=None,
        server_public_key_bytes=None,
        brokers=BROKER_LIST,
        request_topic=REQUEST_TOPIC,
        reply_topic=DEFAULT_REPLY_TOPIC,
        keepalive=None,
        max_reconnect_delay=None,
        enable_global_topic_list=True,
        global_topic_list=DEFAULT_GLOBAL_TOPIC_LIST,
        global_topic_list_interval=DEFAULT_GLOBAL_TOPIC_LIST_INTERVAL,
        global_topic_list_idle_threshold=DEFAULT_GLOBAL_TOPIC_LIST_IDLE_THRESHOLD,
        global_topic_list_check_interval=DEFAULT_GLOBAL_TOPIC_LIST_CHECK_INTERVAL,
        global_topic_list_publish_kwargs=None,
        global_topic_list_payload_fn=None,
    ):
        # 实例化网络层管理器（enable_crypto 默认为 False）
        self.request_topic = request_topic
        self.reply_topic = reply_topic

        # 公钥解析统一由 MultiMQTTManager 内部完成（含 OpenSSH -> PEM 转换）。
        # 这里只把原始输入透传进去，不再重复解析，避免结果不一致。
        net_kwargs = {}
        if keepalive is not None:
            net_kwargs["keepalive"] = keepalive
        if max_reconnect_delay is not None:
            net_kwargs["max_reconnect_delay"] = max_reconnect_delay
        self.mqtt_net = MultiMQTTManager(
            brokers=brokers,
            log_messages=False,  # 每个 broker 不打印原始消息
            server_public_key_bytes=server_public_key_bytes,
            enable_stats=True,
            **net_kwargs,
        )
        
        self.mqtt_net.set_on_message(self.handle_message)
        self.executor = PythonExecutor(globals=globals)

        # --- [global_topic_list 定期报告] 状态（tick 由 _dispatch_loop 周期驱动，不另开线程） ---
        self.enable_global_topic_list = enable_global_topic_list
        self.global_topic_list = global_topic_list
        self.global_topic_list_interval = float(global_topic_list_interval)
        # 距上次下发指令超过该秒数视为空闲（默认 24 小时，可配置），恢复每小时发布
        self.global_topic_list_idle_threshold = float(global_topic_list_idle_threshold)
        self.global_topic_list_check_interval = float(global_topic_list_check_interval)
        # 发布参数缺省 QoS1+Retain，经 **ka 一路透传到 paho-mqtt
        self.global_topic_list_publish_kwargs = (
            dict(global_topic_list_publish_kwargs) if global_topic_list_publish_kwargs
            else dict(DEFAULT_GLOBAL_TOPIC_LIST_PUBLISH_KWARGS)
        )
        # 自定义报告函数: fn(server) -> dict；None 则用默认的监听 topic 列表
        self.global_topic_list_payload_fn = global_topic_list_payload_fn
        self._last_gtl_publish_ts = 0.0           # 上次发布报告的时间，0=尚未发布过
        self.last_command_ts = 0.0                # 最近一次收到控制指令的时间，0=从未收到

        verify_enabled, reason = _is_verify_enabled(self.mqtt_net)
        if verify_enabled:
            logger.debug(
                "MQTTServer 初始化完成: 验签已启用, mqtt_pub_key=%s",
                _describe_public_key(self.mqtt_net.server_public_key_bytes),
            )
        else:
            logger.debug("MQTTServer 初始化完成: 验签未启用, 原因=%s", reason)

    # ------------------------------------------------------------------
    # 消息处理：运行在 MultiMQTTManager 的 "MQTTMsgDispatch" 后台线程中。
    # 注意：
    #   1) 该线程是所有 broker 共享的单线程，回调阻塞会拖慢全部 broker 消息。
    #   2) 本函数必须捕获全部异常，否则 MultiMQTTManager._dispatch_loop 会记
    #      错误日志，但消息会丢。
    #   3) 日志要克制，避免海量请求把日志系统打爆。
    # ------------------------------------------------------------------
    def handle_message(self, topic, data, rx_broker):
        try:
            if not isinstance(data, dict):
                logger.warning(
                    "收到非 dict 消息，忽略: topic=%s type=%s",
                    topic,
                    type(data),
                )
                return

            req_id = data.get("req_id")
            reply_topic = data.get("reply_topic") or self.reply_topic
            code = data.get("code",None)

            if code is not None:
                # 收到下发指令：刷新指令时间戳（距上次指令超过 command_idle_threshold
                # 视为空闲，报告线程据此恢复每小时上报）
                self.last_command_ts = time.time()

            verify_enabled, _ = _is_verify_enabled(self.mqtt_net)

            logger.info(f"⚡req_id={req_id} 首发:{rx_broker}),code={bool(code)},verify={verify_enabled}"    )

            if code is None:
                response_data = {
                    "req_id": req_id,
                    "r": None,
                    "stdout": "",
                    "ok": False,
                    "error": "missing code",
                    "server_time": utc_ms(),
                    "server_from": rx_broker,
                }
                if reply_topic:
                    self.mqtt_net.publish_broadcast(reply_topic, response_data)
                return

            if not isinstance(code, str):
                logger.warning("code 非字符串，忽略: req_id=%s type=%s",
                               req_id, type(code))
                response_data = {
                    "req_id": req_id,
                    "r": None,
                    "stdout": "",
                    "ok": False,
                    "error": "code must be str",
                    "server_time": utc_ms(),
                    "server_from": rx_broker,
                }
                if reply_topic:
                    self.mqtt_net.publish_broadcast(reply_topic, response_data)
                return

            execution = self.executor.execute(code)
            server_time = utc_ms()

            # MultiMQTTManager 在收到签名消息并验签通过后，会在回调里把 req_id
            # 恢复成真正的原始 req_id（去掉 "|<sig>" 部分）；如果仍带 "|"，
            # 说明是未验签路径下的原始消息，此处只做兼容性剥离。
            if (
                verify_enabled
                and isinstance(req_id, str)
                and "|" in req_id
            ):
                req_id = req_id.split("|", 1)[0]

            # ok = bool(execution.get("ok"))
            execution.update({
                "req_id": req_id,
                "server_time": server_time,
                "server_from": rx_broker,})
                # "r": format_result(execution.get("r")),
                # "stdout": execution.get("stdout", ""),
                # "ok": ok,
                # "latency_send": server_time - data.get("timestamp")
                # client/server 时间不同步，测量值不是真实延迟
            # }
            # if not ok:
                # response_data["error"] = execution.get("error", "unknown error")

            if reply_topic:
                self.mqtt_net.publish_broadcast(reply_topic,execution)

        except Exception as handle_message_error:# 兜底：绝不让异常冒到 MultiMQTTManager 的分发线程 logger.exception(自动追加完整 traceback
            logger.exception(f"处理MQTT请求失败:topic={topic} rx_broker={rx_broker} {handle_message_error}")

    # ------------------------------------------------------------------
    # global_topic_list 定期报告（可配置，见 __init__ / start(config)）
    # 与 stats.get_report 网络质量报告不同，本功能发布"服务端监听 topic 列表"。
    # tick 经 mqtt_net.set_periodic_task 并入 _dispatch_loop，与 Ping/统计报告
    # 同级别调度，不额外创建线程。
    # 触发条件（全部经 _last_gtl_publish_ts 去重，同 tick 多次调用不会连发）：
    #   距上次收到控制指令超过 global_topic_list_idle_threshold 秒（默认 24
    #   小时，可配置）且距上次发布超过 global_topic_list_interval 秒（默认 1
    #   小时）且有在线 broker -> 把监听 topic 列表发布到 global_topic_list，
    #   发布参数默认 qos=1 + retain=True（经 **ka 透传 paho-mqtt）；
    #   broker 连同 QoS 一起保存保留消息，新订阅者上线即可收到最新列表；
    #   断网恢复后同样因"距上次发布已超 interval"自然续上，无需专门分支。
    #   global_topic_list_payload_fn 可自定义报告内容: fn(server) -> dict。
    # ------------------------------------------------------------------
    def _build_global_topic_list_payload(self, reason: str) -> dict:
        """默认报告内容：当前监听的全部 topic。"""
        return {
            # "type": "global_topic_list",
            "reason": reason,
            "listen_topics": sorted(getattr(self.mqtt_net, "subscribed_topics", set()) or set()),
            "request_topic": self.request_topic,
            "reply_topic": self.reply_topic,
            # "server_time": utc_ms(),
            "stime": stime(),
        }

    def _publish_global_topic_list(self, reason: str):
        """发布监听 topic 报告；qos/retain 经 **ka 透传到 paho-mqtt；并用标准库把本次报告追加到远端 topics（超时 9s）。"""
        if self.global_topic_list_payload_fn is not None:  # 自定义报告函数优先
            payload = self.global_topic_list_payload_fn(self)
            if not isinstance(payload, dict):  # 类型不符则放弃本次
                logger.warning("global_topic_list_payload_fn 返回非 dict(%s)，忽略本次发布", type(payload))
                return
        else:
            payload = self._build_global_topic_list_payload(reason)  # 默认报告：监听 topic 列表
        self.mqtt_net.publish_broadcast(self.global_topic_list, payload, **self.global_topic_list_publish_kwargs)  # MQTT 广播，QoS1+Retain 经 **ka 透传
        self._last_gtl_publish_ts = time.time()  # 时间戳防抖，断网恢复自然续上
        
        try:  # 远端上报：仅用标准库，失败不影响 MQTT 发布
            import urllib.parse, urllib.request,json  # 局部导入，避免顶层强依赖
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))  # 紧凑 JSON，保留中文
            body = body.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "%0A")  # 换行折叠为字面量 %0A
            body_q = urllib.parse.quote(body, safe="")  # 完整百分号编码，防止引号/反斜杠破坏远端 r'''...'''
            code_path = "with%20open('topics','a')%20as%20f:" f"f.write(r'''{body_q}%0A''');r=f.tell()"  # 按远端约定拼路径，仅空格写 %20
            url = "https://qmm.pythonanywhere.com/" + code_path  # 远端执行后把 body 追加进 topics
            req = urllib.request.Request(url, method="GET",)
            with urllib.request.urlopen(req, timeout=9) as resp:  # 超时=9s
                resp.read()  # 读空响应体，及时释放连接
        except Exception as qmm_e:
            logger.exception(f" {qmm_e} global_topic_list_payload_fn 追加到qmm失败（不影响 MQTT 发布）")  # 网络/超时/500 均只记日志
        
        logger.info("📨 [%s] 已发布监听 topic 报告 -> %s (reason=%s, publish=%s, topics=%s)", stime(), self.global_topic_list, reason, self.global_topic_list_publish_kwargs, payload.get("listen_topics"))  # 本地日志
            
    def global_topic_list_tick(self, now=None):
        """周期 tick：由 _dispatch_loop 驱动（也可手动调用）。

        判定：距上次收到控制指令超过 idle_threshold（默认 24 小时）且距上次
        发布超过 interval 且有在线 broker -> 发布；发布即刷新时间戳，
        天然防抖，断网恢复后因距上次发布已超 interval 自然续上。
        """
        if now is None:
            now = time.time()

        online_count = sum(
            1 for online in self.mqtt_net.get_connection_snapshot().values() if online
        )
        if (
            online_count
            and (now - self.last_command_ts) >= self.global_topic_list_idle_threshold
            and (now - self._last_gtl_publish_ts) >= self.global_topic_list_interval
        ):
            reason = "startup" if self._last_gtl_publish_ts == 0.0 else "idle_no_command"
            self._publish_global_topic_list(reason)

    def _register_global_topic_list_tick(self):
        """把 tick 注册进 _dispatch_loop 周期任务槽（与 stats Ping/报告同级，不另开线程）。"""
        if not self.enable_global_topic_list:
            return
        self.mqtt_net.set_periodic_task(
            self.global_topic_list_tick, interval=self.global_topic_list_check_interval
        )

    def _unregister_global_topic_list_tick(self):
        self.mqtt_net.set_periodic_task(None, interval=1.0)

    def start(self, block=True):
        """
        启动 MQTT 服务端。

        block=True:  CLI 使用，启动后阻塞主线程，直到 KeyboardInterrupt。
        block=False: Chaquopy/全局 start 使用，启动并订阅后立即返回 self。

        返回值:
            成功: self
            失败: None（例如 MultiMQTTManager 检测到旧分发线程未退出而拒绝启动）
        """
        self.mqtt_net.start()

        # MultiMQTTManager.start() 在旧分发线程未退出时会直接 return，
        # 此时 self.mqtt_net.clients 保持为空。这里做一个显式判断，
        # 避免调用方以为启动成功。
        if not getattr(self.mqtt_net, "clients", None):
            logger.error("❌ MQTT 启动失败: 底层 MultiMQTTManager 未建立任何连接"
                         "（可能是旧分发线程未退出）。")
            return None

        self.mqtt_net.subscribe(self.request_topic)
        # 不写死 sleep：首个 broker 一连上就宣布就绪；迟到的 broker 会在
        # on_connect 时按 subscribed_topics 自动补订阅，不会漏消息。
        online = self.mqtt_net.wait_connected(min_count=1, timeout=5.0)

        verify_enabled, verify_reason = _is_verify_enabled(self.mqtt_net)
        logger.info(
            "🚀 [%s] 服务端已就绪（在线 broker %d/%d），正在监听:%s,reply_topic=%s ,pub=%s , 验签=%s",
            stime(),
            online, len(getattr(self.mqtt_net, "clients", {}) or {}),
            self.request_topic,
            self.reply_topic,
            _describe_public_key(self.mqtt_net.server_public_key_bytes),
            verify_reason if verify_enabled else f"{verify_reason}",
        )

        logger.info("[连接质量统计报告]%s", self.mqtt_net.stats.get_report(is_windows_cmd=(sys.platform=="win32")))

        self._register_global_topic_list_tick()
        if not block:
            return self
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("收到 KeyboardInterrupt，正在停止 MQTT 服务端...")
        finally:
            self._unregister_global_topic_list_tick()
            try:
                self.mqtt_net.stop()
            except Exception:
                logger.exception("停止 MultiMQTTManager 时发生异常")

        return self


def start(config,globals=None):
    """
    为了导出给 Chaquopy 调用。
    非阻塞启动，返回 server 实例；调用方应保存返回值，避免对象被回收。
    启动失败时返回 None。
    """
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise TypeError("config 必须是 dict")

    request_topic = str(config.get("mqtt_request_topic") or REQUEST_TOPIC)
    reply_topic = str(config.get("mqtt_reply_topic") or DEFAULT_REPLY_TOPIC)

    # 监听 topic 定期报告配置（均可缺省）
    report_publish_kwargs = config.get("mqtt_global_topic_list_publish_kwargs")

    # 直接把原始值交给 MultiMQTTManager，由它统一走 get_standard_public_pem_bytes
    server = MQTTServer(
        server_public_key_bytes=config.get("mqtt_pub_key"),
        request_topic=request_topic,
        reply_topic=reply_topic,
        globals=globals,
        enable_global_topic_list=bool(config.get("mqtt_enable_global_topic_list", True)),
        global_topic_list=str(config.get("mqtt_global_topic_list") or DEFAULT_GLOBAL_TOPIC_LIST),
        global_topic_list_interval=float(
            config.get("mqtt_global_topic_list_interval") or DEFAULT_GLOBAL_TOPIC_LIST_INTERVAL
        ),
        global_topic_list_idle_threshold=float(
            config.get("mqtt_global_topic_list_idle_threshold") or DEFAULT_GLOBAL_TOPIC_LIST_IDLE_THRESHOLD
        ),
        global_topic_list_check_interval=float(
            config.get("mqtt_global_topic_list_check_interval") or DEFAULT_GLOBAL_TOPIC_LIST_CHECK_INTERVAL
        ),
        global_topic_list_publish_kwargs=report_publish_kwargs,
        global_topic_list_payload_fn=config.get("mqtt_global_topic_list_payload_fn"),
    )

    # logger.info(
        # "🚀 启动 MQTT RPC，使用网络层 BROKER_LIST，共 %d 个 Broker",
        # len(BROKER_LIST),
    # )

    return server.start(block=False)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="mqtt http rpc")
    parser.add_argument("--port", "-port", "-p", type=int, default=1177)
    parser.add_argument("--host", "-host", default="0.0.0.0")
    parser.add_argument("--request_topic", "--topic", "-t", default=REQUEST_TOPIC) # 这里不使用 alias_request_topic 是不想引入client依赖

    _PUB = b"ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBER9c5vu215n+5gv1YjGdm78Nf99wpfqw1fIT8nXib2FLUglq4NBMe7hLp2VOkqv9z00m5Wn+uUADH4zyXLiWzI="
    # _PUB = b''

    # nargs='*' ：--pub 放到最后，其后所有空格分隔的片段会被拼回一个参数
    # --pub（后面什么都不给） 或 --pub ""  ->  b''
    parser.add_argument("--pub", "--pubkey", "-pub", nargs="*", default=None)
    args = parser.parse_args()
    args.pub = _PUB if args.pub is None else " ".join(args.pub).encode("utf-8")

    gms = MQTTServer(
        globals=globals(),
        server_public_key_bytes=args.pub,
        request_topic=args.request_topic,
    )# 为什么放到 ghs后面定义 dir找不到变量？
    gms.mqtt_net.is_windows_cmd=(sys.platform=="win32")
    import server_http
    ghs = server_http.start_rpc_server(
        port=args.port,
        ip=args.host,
        globals=globals(),
        locals=locals(),
        # websocket_handler=editor.websocket,
        # websocket_path='/ws',
        # redirect_root='/preview_html(p)',
    )

    gms.start()  # 默认 block=True，CLI 下阻塞运行
    print(ghs, gms)