# ==================== 首包统计（纯被动，零额外网络请求） ====================
#
# 目标：长时间观察「底层 multi_mqtt 的首包」在多个 broker 上的分布，不发
# 任何额外报文、不改 QoS、不动收发语义。全部样本来自本来就在跑的流量：
#
#   ① 下行首包：每次请求/每帧 PTY 输出，第一份副本经哪个 broker 先到客户端。
#      请求还额外记录「发布 → 首包到达」的延迟（复用请求上下文里的 start_time，
#      与 client_mqtt 自己算的 latency_ms 同口径）。
#   ② 上行首包：每次 publish_broadcast 的首包，哪个 broker 的 on_publish
#      （包已发离客户端）先完成，以及各 broker 的发送耗时。
#
# 查看：
#   curl "http://127.0.0.1:1188/r=first_packet_stats.get_report()"
#   curl "http://127.0.0.1:1188/r=first_packet_stats.get_report(reset=True)"
#       # 读完清零，开始新的统计窗口
#   curl "http://127.0.0.1:1188/r=first_packet_stats.get_report(snapshots=True)"
#       # 时间切片体积=窗口数×broker数，默认不返回，需要时才带出
#
# 说明：QoS0 下 on_publish 只代表「包已交给网络层」，不代表 broker 已回执；
# 上行统计因此是发送侧口径。真正能证明到达 broker 的只有业务回包，那部分
# 由 ① 的下行首包覆盖。所有计数只在本进程内存里，0 额外网络请求。
#
# 挂在 manager 的 publish_broadcast / paho client.publish / on_publish /
# message_callback 上；每处都吞自己的异常，任何统计故障只丢样本，
# 绝不打断主流程（AGENTS.md 铁律）。


class FirstPacketStats:
    """被动首包统计器。线程安全；所有入口异常自吞，绝不外溢。"""

    _BATCH_MAX = 4096             # 未收到全部 on_publish 的上行批次上限
    _MID_MAX = 32768              # mid -> 批次 的在途映射上限
    _MID_TTL = 120.0              # mid 最长在途秒数，超时视为等不到回执
    _EARLY_MAX = 4096             # 早到 ack 暂存上限
    _EARLY_TTL = 60.0             # 早到 ack 等登记的最长秒数，超时算未归属
    _DROPPED_MAX = 4096           # 已 TTL 回收的 mid 标记上限
    _DROPPED_TTL = 480.0          # 该标记保留秒数，用于识别迟到 ack
    _FOREIGN_MAX = 4096           # 非广播 publish 的 mid 标记上限
    _FOREIGN_TTL = 60.0           # 该标记保留秒数，用于丢弃其 on_publish
    _EARLY_SAMPLE_MAX = 8         # 过期早到 ack 的现场样本上限
    _UNMATCHED_SAMPLE_MAX = 5     # 未匹配回包的滚动样本上限（保留最近几条）
    _SEEN_MAX = 16384             # 下行 seq 去重表上限
    _SID_MAX = 512                # 会话首帧 broker 表上限
    _RES_MAX = 512                # 每 host 每指标的分位数样本上限（滚动）
    _RES_GLOBAL_MAX = 2048        # 全局指标的分位数样本上限（滚动）
    _SNAP_INTERVAL = 3600.0       # 时间切片窗口宽度（秒）
    _SNAP_MAX = 48                # 时间切片保留窗口数
    HB_EXPECTED_MS = 5000.0       # 心跳周期基准（服务端默认 5s，见 remote_cmd）
    HB_EXPECTED_S = 5.0

    # _g 的唯一初值模板：__init__ 与 _reset_locked 都从它复制。
    # min/max 类字段初值必须是 None（0 会把后续 min 卡死在 0），
    # sum 类是 0.0/0，计数是 0——按类型猜归零值曾导致
    # up_pub_min 在 reset 后锁死 0 的 bug，因此这里显式逐字段声明。
    _G_INIT = {
        "req_obs": 0, "req_foreign": 0,
        "req_n": 0,
        "req_lat_n": 0, "req_lat_min": None,
        "req_lat_sum": 0.0, "req_lat_max": None,
        "dl_unique": 0, "dl_dups": 0,
        "up_calls": 0,
        "up_predicted": 0, "up_attempted": 0, "up_seen": 0,
        "up_blind": 0, "up_late": 0, "up_offline": 0,
        "up_pub_n": 0, "up_pub_min": None,
        "up_pub_sum": 0, "up_pub_max": None,
        "up_send_n": 0, "up_send_min": None,
        "up_send_sum": 0.0, "up_send_max": None,
        "mid_orphan_write": 0,
        "mid_ttl_dropped": 0,
        # ack_without_mid 恒等于 ack_before_reg + ack_after_drop：
        #   早到且始终没等到登记（含暂存过期） -> ack_before_reg
        #   _mid 已被 TTL 回收之后才到           -> ack_after_drop
        # 早到但在登记时成功匹配的记 ack_early_matched（不进 without_mid）。
        "ack_early_matched": 0,
        "ack_before_reg": 0,
        "ack_after_drop": 0,
        "ack_without_mid": 0,
        "ack_foreign": 0,
        "srv_observed": 0, "srv_agree": 0, "srv_disagree": 0,
    }

    def __init__(self):
        self._lock = threading.Lock()
        self._t0 = time.monotonic()
        self._born = self._t0          # 对象创建时刻，reset 不重置
        self._pid = os.getpid()
        self._created_epoch = time.time()
        self._installed = False
        self._logger = logging.getLogger("first_packet_stats")
        self._net = None
        self._node = None
        self._tls = threading.local()
        self._batches = {}        # bid -> {"t0","pending","win","published"}
        self._mid = {}            # (host, mid) -> (bid, publish_start)
        # on_publish 可能早于 _note_publish_call 登记到达（paho 在 publish()
        # 返回前就写出并回调）。早到 ack 先暂存在这里，登记时再消费，避免
        # mid 写出后永远等不到第二次 ack、120s 后被 TTL 回收。
        self._early_acks = {}     # (host, mid) -> ack 到达的 monotonic 秒
        # _mid 被 TTL 回收的标记：用于把「回收后才到的迟到 ack」与早到区分开。
        self._dropped_mids = {}   # (host, mid) -> 标记时间
        # 非 broadcast publish（stats QoS1 ping 等）的 mid：它们的 on_publish
        # 不属于业务批次，直接丢弃并计数，不能当成早到 ack 暂存。
        self._foreign_mids = {}   # (host, mid) -> 记录时间
        # 过期早到 ack 的现场：按 host 分布 + 最近几条样本（含 mid/age/source），
        # 用于回答“ack_before_reg 到底是谁的 ack”。
        self._early_expired_hosts = {}
        self._early_expired_samples = []
        self._batch_seq = 0
        self._dl_seen = {}        # (sid, seq) -> 首达 broker
        self._sids = {}           # sid -> 首帧 broker（仅展示用）
        self._first_req_broker = None
        self._first_req_broker_at = None   # 设置该字段时的 uptime 秒（本地钟）
        self._first_dl_broker = None
        self._first_ul_broker = None
        self._first_ul_broker_at = None
        self._first_ul_bid = None          # 哪个广播批次把它确定下来的
        # 未匹配本机 pending_requests 的带 req_id 帧：topic 计数 + 少量样本。
        # 只为回答「这些帧到底是谁的」，样本只留键名，不保存任何业务内容。
        self._unmatched_topics = {}
        self._unmatched_samples = []
        # 上行覆盖诊断：区分「扇出集合没包含它」和「发了但统计没抓到」。
        self._blind = {}          # host -> 该发却没进 wrapper 的批次数
        self._absent = {}         # host -> 不在 net.clients 快照里的批次数
        self._offline = {}        # host -> 在 clients 里但 is_connected() 为假的批次数
        self._late = {}           # host -> 快照后才连上、却已进 wrapper 的批次数
        self._offline_samples = []   # 最近 10 条 offline 现场（滚动，看时间轴）
        self._offline_latest = {}    # host -> 该 host 最近一次离线现场（有界）
        self._last_rx = {}        # host -> 最近一次下行到达的 monotonic 秒
        self._last_online = {}    # host -> 最近一次检查时 is_connected() 为真的 at_s
        self._last_bcast_at = None   # 最近一次 publish_broadcast 的 at_s
        # 上行端到端确认：回包里的 server_from 是服务端「首个接到该请求的
        # broker」，客户端本地拿不到；配合同一 req_id 的发送侧首个 on_publish
        # broker 才能看出“包先排出 socket”与“服务端先收到”是否同一条路。
        self._req_send_win = {}   # req_id -> 发送侧首个 on_publish 的 broker
        self._srv_seen = {}       # req_id -> 已计过 server_ack 的匹配回包（去重）
        self._srv_first = None    # 首个带 server_from 的匹配回包所报 broker
        self._srv_disagree_hosts = {}   # server_from -> 与发送侧首包不一致的次数
        # host -> 当前活订阅的 topic filter 集合（区分双投形态 A/B 用）。
        # paho 2.1 已不再维护 c._subscriptions，这里用 manager 的规范集合做
        # 初值，再包装每个 client 的 subscribe/unsubscribe 增量维护。
        self._sub_topics = {}
        self._sub_seeded = {}     # host -> 播种时补入的规范订阅数（诊断用）
        self._brokers = {}        # host -> 计数器 dict
        self._g = dict(self._G_INIT)
        self._samples = {}            # host -> 指标 -> 最近样本（分位数用）
        self._gsamples = {}           # 全局指标 -> 最近样本
        self._conn_state = {}         # host -> 连接事件累计（在线率用）
        self._peak_mids = 0           # 在途 mid 历史峰值
        self._peak_early = 0          # 早到 ack 历史峰值
        self._snapshots = []          # 时间切片窗口（滚动，见 _roll_snapshot_locked）
        self._snap_at = self._t0      # 当前窗口起点
        self._snap_anchor = {"brokers": {}, "g": dict(self._G_INIT),
                             "conn": {}}

    # ---------------------------- 安装 ----------------------------

    def install(self, transport) -> bool:
        """在 MQTTClientNode 上挂被动钩子；已装/结构不符返回 False。"""
        with self._lock:
            if self._installed:
                return False
            node = getattr(transport, "node", None)
            net = getattr(node, "mqtt_net", None)
            if net is None or not callable(getattr(net, "publish_broadcast", None)):
                return False
            self._net = net
            self._node = node
            prev = getattr(net, "message_callback", None)

            def _cb(topic, data, broker):
                try:
                    self._note_downlink(topic, data, broker)
                except Exception:
                    pass
                if prev is not None:
                    prev(topic, data, broker)

            try:
                net.set_on_message(_cb)
            except Exception:
                pass
            orig_pb = net.publish_broadcast

            def _pb(topic, payload_dict, *a, **kw):
                return self._publish_broadcast(orig_pb, topic, payload_dict, a, kw)

            try:
                net.publish_broadcast = _pb
            except Exception:
                return False
            self._installed = True
        self._wrap_clients()
        return True

    def _wrap_clients(self):
        """给每个 paho client 挂 publish/on_publish 观察器；新旧连接都覆盖。

        :return: 本次看到的 [(host, client)] 快照，拿不到时返回 []。
        这份快照同时用来判定「本批本该发到哪些 host」，从而暴露
        「实际发了但统计没抓到」的盲区。
        """
        net = self._net
        if net is None:
            return []
        try:
            with net.lock:
                items = list(net.clients.items())
        except Exception:
            return []
        for host, c in items:
            self._wrap_client(host, c)
        return items

    def _wrap_client(self, host, c):
        try:
            # 订阅计数种子放在 _fp_wrapped 守卫之前：reset 清空 _sub_topics
            # 后，即使 client 已包装过，下一次 _wrap_clients() 也要把初值补回。
            # paho 2.1 没有 c._subscriptions，只能用 manager 的规范集合做初值。
            try:
                with self._lock:
                    if host not in self._sub_topics:
                        self._sub_topics[host] = set(
                            getattr(self._net, "subscribed_topics", ()) or ())
                        self._sub_seeded[host] = len(self._sub_topics[host])
            except Exception:
                pass
            if getattr(c, "_fp_wrapped", False):
                return
            orig_pub = c.publish

            def _pub(topic, payload=None, qos=0, retain=False, properties=None,
                     _orig=orig_pub, _host=host, _self=self):
                t0 = time.perf_counter()
                info = _orig(topic, payload, qos=qos, retain=retain,
                             properties=properties)
                try:
                    bid = getattr(_self._tls, "batch", None)
                    if bid is not None:
                        _self._note_publish_call(bid, _host, info, t0)
                    else:
                        # 不属于任何 broadcast 的 publish（如 stats 的 QoS1
                        # ping、业务直发）：它的 on_publish 会在没有登记槽时
                        # 到达，必须标记出来直接丢弃，否则会被误存成“早到
                        # ack”，过期后污染 ack_before_reg。
                        _self._note_foreign_publish(_host, info)
                except Exception:
                    pass
                return info

            orig_ack = getattr(c, "on_publish", None)

            def _ack(client, userdata, mid, *a, **kw):
                try:
                    self._note_publish_ack(host, mid)
                except Exception:
                    pass
                if orig_ack is not None:
                    return orig_ack(client, userdata, mid, *a, **kw)

            c.publish = _pub
            c.on_publish = _ack
            # 订阅计数增量维护：同一 filter 的重连重订阅按 MQTT 语义是替换，
            # 只保留一份；只有新增另一个能匹配同一帧的 filter 才会变大（形态 A）。
            orig_sub = getattr(c, "subscribe", None)
            if callable(orig_sub):
                def _sub(topic, *a, _orig=orig_sub, _host=host,
                         _self=self, **kw):
                    info = _orig(topic, *a, **kw)
                    try:
                        rc = info[0] if isinstance(info, (tuple, list)) else info
                        if int(rc) == 0:
                            with _self._lock:
                                _self._sub_topics.setdefault(
                                    _host, set()).add(topic)
                    except Exception:
                        pass
                    return info

                try:
                    c.subscribe = _sub
                except Exception:
                    pass
            orig_unsub = getattr(c, "unsubscribe", None)
            if callable(orig_unsub):
                def _unsub(topic, *a, _orig=orig_unsub, _host=host,
                           _self=self, **kw):
                    info = _orig(topic, *a, **kw)
                    try:
                        with _self._lock:
                            _self._sub_topics.setdefault(
                                _host, set()).discard(topic)
                    except Exception:
                        pass
                    return info

                try:
                    c.unsubscribe = _unsub
                except Exception:
                    pass
            # on_connect / on_disconnect 链式包装：累计断连/重连次数与在线
            # 时长，支撑 brokers[host].connection.online_ratio（非瞬时快照）。
            orig_conn = getattr(c, "on_connect", None)
            if callable(orig_conn) or orig_conn is None:
                def _oc(client, userdata, *a, _orig=orig_conn, _host=host,
                        _self=self, **kw):
                    try:
                        with _self._lock:
                            _self._conn_event_locked(_host, True)
                    except Exception:
                        pass
                    if _orig is not None:
                        return _orig(client, userdata, *a, **kw)

                try:
                    c.on_connect = _oc
                except Exception:
                    pass
            orig_disc = getattr(c, "on_disconnect", None)
            if callable(orig_disc) or orig_disc is None:
                def _od(client, userdata, *a, _orig=orig_disc, _host=host,
                        _self=self, **kw):
                    try:
                        with _self._lock:
                            _self._conn_event_locked(_host, False)
                    except Exception:
                        pass
                    if _orig is not None:
                        return _orig(client, userdata, *a, **kw)

                try:
                    c.on_disconnect = _od
                except Exception:
                    pass
            # 种子：安装时按 is_connected() 记初始状态（之后只认连接事件），
            # 并确保 _brokers 里有该 host 的空行，让统一视图覆盖安静的节点。
            try:
                with self._lock:
                    self._broker(host)
                    self._conn_seed_locked(host, bool(c.is_connected()))
            except Exception:
                pass
            c._fp_wrapped = True
        except Exception:
            pass

    # ---------------------------- 上行 ----------------------------

    def _publish_broadcast(self, orig, topic, payload_dict, args, kw):
        """包住一次广播：建批次 → 记每个 broker 的 publish/on_publish。"""
        self._maybe_roll()
        items = []
        try:
            items = self._wrap_clients()   # 重连会换新 client，顺带补齐
        except Exception:
            items = []
        # 本批「预测该发到」的 host：与 publish_broadcast 内部的扇出快照同源
        # （都是 net.clients），再按它自己的过滤条件 is_connected() 筛一遍。
        # 注意这只是预测：真正的扇出快照在 orig 内部、且发生在序列化/签名之后，
        # 两者之间存在时间差（reconnect / _recover_client 换对象都可能发生），
        # 所以报告里用 attempted = predicted ∪ seen，保证 seen 永远是子集。
        attemptable = set()
        present = set()
        offline_info = []
        online_now = {}
        try:
            with self._lock:
                last_rx = dict(self._last_rx)
        except Exception:
            last_rx = {}
        now_mono = time.monotonic()
        at_s = round(now_mono - self._t0, 1)
        for host, c in items:
            present.add(host)
            try:
                connected = c.is_connected()
            except Exception:
                connected = False
            if connected:
                attemptable.add(host)
                online_now[host] = at_s
                continue
            # 现场快照：用来区分「真掉线」和「paho 状态机假离线 / 僵尸 client」。
            # rx_age_s=该 host 最近一次下行到达距今几秒，若它明明在收心跳却
            # is_connected()=False，就是判断口径或对象错位，而不是真的离线。
            last = last_rx.get(host)
            try:
                state = str(getattr(c, "_state", ""))
            except Exception:
                state = ""
            try:
                socket_alive = getattr(c, "_socket", None) is not None
            except Exception:
                socket_alive = False
            offline_info.append({
                "host": host,
                "at_s": at_s,
                "state": state,
                "socket": socket_alive,
                "rx_age_s": (None if last is None
                             else round(now_mono - last, 1)),
            })
        cfg = self._configured_hosts()
        absent = cfg - present if cfg else set()
        # 请求的 req_id 记在批次上：回包带 server_from 时，要用同一 req_id 的
        # 发送侧首个 on_publish broker 做对照。去掉签名部分，与 pending_requests
        # 的键保持一致。
        req_id = None
        if isinstance(payload_dict, dict):
            rid = payload_dict.get("req_id")
            if isinstance(rid, str) and rid:
                req_id = rid.split("|", 1)[0]
        bid = None
        try:
            with self._lock:
                self._batch_seq += 1
                bid = self._batch_seq
                self._batches[bid] = {"t0": time.perf_counter(),
                                      "pending": 0, "win": None,
                                      "published": None, "done": False,
                                      "attemptable": attemptable,
                                      "seen": set(), "absent": absent,
                                      "offline": offline_info,
                                      "online": online_now,
                                      "req_id": req_id}
                self._g["up_calls"] += 1
                self._last_bcast_at = at_s
                self._prune()
        except Exception:
            bid = None
        prev = getattr(self._tls, "batch", None)
        if bid is not None:
            self._tls.batch = bid
        published = None
        try:
            published = orig(topic, payload_dict, *args, **kw)
            return published
        finally:
            try:
                self._tls.batch = prev
                if bid is not None:
                    self._finish_batch(bid, published)
            except Exception:
                pass

    def _finish_batch(self, bid, published):
        with self._lock:
            b = self._batches.get(bid)
            if b is None:
                return
            b["published"] = published
            # 标记广播已结束：只有到这里之后，pending 归零才允许回收批次。
            # 否则先到的 ack 会在循环还没 publish 到其余 broker 时就把批次
            # 删掉，后续 publish 写出的 mid 就变成指向不存在批次的孤儿
            # （现场 stale_mids=3 的来源）。
            b["done"] = True
            g = self._g
            predicted = b.get("attemptable", ())
            seen = b.get("seen", ())
            # attempted 用预测集 ∪ 实见集：预测发生在 orig 之前（序列化/签名
            # 之前），orig 内部真正的扇出快照可能已经包含新连上的 host。若只
            # 用预测集，就会在现场出现 seen > attempted；并上 seen 后
            # attempted 恒等于 seen + blind，口径自洽。
            attempted = set(predicted) | set(seen)
            g["up_predicted"] += len(predicted)
            g["up_attempted"] += len(attempted)
            g["up_seen"] += len(seen)
            for h in set(seen) - set(predicted):
                g["up_late"] += 1
                self._late[h] = self._late.get(h, 0) + 1
            for h in attempted - set(seen):
                g["up_blind"] += 1
                self._blind[h] = self._blind.get(h, 0) + 1
            for h in b.get("absent", ()):
                self._absent[h] = self._absent.get(h, 0) + 1
            for info in b.get("offline", ()):
                g["up_offline"] += 1
                h = info.get("host")
                if h is None:
                    continue
                self._offline[h] = self._offline.get(h, 0) + 1
                # 同一 host 只保留最近一次现场：这样每个 host 的稳态离线
                # 都不会被滚动窗口挤掉（滚动窗口只适合看时间轴）。
                self._offline_latest[h] = dict(info)
                self._offline_samples.append(dict(info))
            if len(self._offline_samples) > 10:
                del self._offline_samples[:len(self._offline_samples) - 10]
            # 每个 host 最近一次「检查时 is_connected() 为真」的时刻：与
            # offline_latest 配对，才能判断“离线后是否恢复”，而不是把
            # “最后一次检查到离线”误读成“进入离线后一直没恢复”。
            for h, online_at in b.get("online", {}).items():
                self._last_online[h] = online_at
            if isinstance(published, int):
                g["up_pub_n"] += 1
                g["up_pub_sum"] += published
                if g["up_pub_min"] is None or published < g["up_pub_min"]:
                    g["up_pub_min"] = published
                if g["up_pub_max"] is None or published > g["up_pub_max"]:
                    g["up_pub_max"] = published
            if b["pending"] <= 0:
                self._batches.pop(bid, None)

    def _note_publish_call(self, bid, host, info, t0):
        rc = getattr(info, "rc", None)
        try:
            ok = int(rc) == 0
        except Exception:
            ok = False
        mid = getattr(info, "mid", None)
        with self._lock:
            st = self._broker(host)
            b = self._batches.get(bid)
            if b is not None:
                # seen 的口径是「这次调用进了包装器」，与 publish 成功与否无关：
                # 失败的 publish 也会记 up_failed，但不能算成 blind。
                b["seen"].add(host)
            if not ok:
                st["up_failed"] += 1
                if mid is not None:
                    self._early_acks.pop((host, mid), None)
                return
            st["up_queued"] += 1
            if mid is None:
                return
            key = (host, mid)
            # 同一个 (host, mid) 可能因客户端重建而复用：先扣回旧记录的
            # pending，否则旧批次会永远等一个不会来的 ack。
            self._drop_mid(key)
            # 早到 ack 必须在这里消费，且必须在 b is None 的 orphan 分支之前：
            # 否则 ack 已暂存、登记路径却提前 return，暂存项只能等 _EARLY_TTL
            # 过期，变成泄漏式的 ack_before_reg。
            early = self._early_acks.pop(key, None)
            if early is not None:
                self._g["ack_early_matched"] += 1
                lat = (early - t0) * 1000.0
                if lat >= 0:
                    self._add_send(host, lat)
                self._settle_win(b, host, bid)
                return
            if b is None:
                # 批次已被结束/淘汰：这条 mid 永远等不到归属批次，写进去
                # 只会制造 stale_mids。计入 orphan 便于定位（正常应为 0）。
                self._g["mid_orphan_write"] += 1
                return
            b["pending"] += 1
            if len(self._mid) >= self._MID_MAX:
                for k in list(self._mid)[:self._MID_MAX // 2]:
                    self._drop_mid(k)
            self._mid[key] = (bid, t0)
            if len(self._mid) > self._peak_mids:
                self._peak_mids = len(self._mid)

    def _add_send(self, host, lat):
        """记一条发送耗时样本（持锁调用）。"""
        st = self._broker(host)
        self._lat_add(st, "up_send", lat)
        self._lat_add(self._g, "up_send", lat)
        self._res_host(host, "up_send", lat)
        self._res_global("up_send", lat)

    def _settle_win(self, b, host, bid):
        """批次首个完成的 ack：记首包竞速与 req_id -> 发送侧首包映射。

        早到 ack 与正常 ack 共用，保证两种路径口径一致；b is None（批次已
        结束/淘汰）时只丢首包样本，不影响已记的发送耗时。
        """
        if b is None or b.get("win") is not None:
            return
        b["win"] = host
        self._broker(host)["up_wins"] += 1
        rid = b.get("req_id")
        if rid and rid not in self._req_send_win:
            if len(self._req_send_win) >= self._SEEN_MAX:
                self._req_send_win.pop(next(iter(self._req_send_win)), None)
            self._req_send_win[rid] = host
        if self._first_ul_broker is None:
            self._first_ul_broker = host
            self._first_ul_broker_at = time.monotonic() - self._t0
            self._first_ul_bid = bid

    def _stage_early(self, key, now):
        """暂存早到 ack；超限按 FIFO 淘汰，被淘汰的算未归属（持锁调用）。"""
        if key in self._early_acks:
            return
        if len(self._early_acks) >= self._EARLY_MAX:
            old_key = next(iter(self._early_acks))
            old_at = self._early_acks.pop(old_key, now)
            self._expire_early(old_key, old_at, now, "evicted")
        self._early_acks[key] = now
        if len(self._early_acks) > self._peak_early:
            self._peak_early = len(self._early_acks)

    def _expire_early(self, key, at, now, reason):
        """早到 ack 等不到登记：计入 ack_before_reg，并留 host 分布/样本。

        来源只能标 unknown：它既没等到业务登记，也没被 _foreign_mids 认领
        （认领的会走 ack_foreign）。持锁调用。
        """
        try:
            host, mid = key
        except Exception:
            host, mid = None, None
        self._g["ack_before_reg"] += 1
        self._g["ack_without_mid"] += 1
        self._early_expired_hosts[host] = (
            self._early_expired_hosts.get(host, 0) + 1)
        self._early_expired_samples.append({
            "host": host,
            "mid": mid,
            "age_ms": round(max(0.0, now - at) * 1000.0, 1),
            "source": "unknown",
            "reason": reason,
        })
        if len(self._early_expired_samples) > self._EARLY_SAMPLE_MAX:
            del self._early_expired_samples[
                :len(self._early_expired_samples) - self._EARLY_SAMPLE_MAX]

    def _mark_dropped(self, key):
        """标记一个被 TTL 回收的 mid，用于识别之后的迟到 ack（持锁调用）。"""
        if len(self._dropped_mids) >= self._DROPPED_MAX:
            self._dropped_mids.pop(next(iter(self._dropped_mids)), None)
        self._dropped_mids[key] = time.perf_counter()

    def _note_foreign_publish(self, host, info):
        """记录一次非 broadcast publish 的 mid（stats QoS1 ping / 业务直发）。

        这类 publish 没有批次登记，其 on_publish 若走 _note_publish_ack 会被
        当成“早到 ack”暂存，_EARLY_TTL 后转成 ack_before_reg，把业务口径
        污染。这里先按 (host, mid) 打标，ack 到达时直接丢弃并计入
        ack_foreign（不持锁调用，内部加锁）。
        """
        try:
            mid = getattr(info, "mid", None)
        except Exception:
            mid = None
        if mid is None:
            return
        key = (host, mid)
        with self._lock:
            # on_publish 也可能早于这次打标（paho 在 publish() 返回前就回调）：
            # 先把已暂存的同名 ack 认领掉，否则它会一直等到过期，变成一条
            # 与业务无关的 ack_before_reg。
            if self._early_acks.pop(key, None) is not None:
                self._g["ack_foreign"] += 1
                return
            if len(self._foreign_mids) >= self._FOREIGN_MAX:
                self._foreign_mids.pop(next(iter(self._foreign_mids)), None)
            self._foreign_mids[key] = time.perf_counter()

    def _drop_mid(self, key):
        """丢弃一条在途 mid，并同步扣减所属批次的 pending（持锁调用）。"""
        rec = self._mid.pop(key, None)
        if rec is None:
            return
        bid = rec[0]
        b = self._batches.get(bid)
        if b is not None:
            b["pending"] -= 1
            if b["done"] and b["pending"] <= 0:
                self._batches.pop(bid, None)

    def _note_publish_ack(self, host, mid):
        """paho on_publish：包已发离该 broker 连接，算发送耗时/首达竞速。

        rec 为空分三种情况：
          ① 早到：登记前就回调 -> 暂存进 _early_acks，登记时消费；
          ② 迟到：_mid 已被 TTL 回收 -> 记 ack_after_drop；
          ③ 既非早到也没登记 -> 同 ① 暂存，_EARLY_TTL 过期后记 ack_before_reg。
        注意「ack_without_mid 与 mid_ttl_dropped 同量级」只能说明时间错位
        存在（①或②至少一类大量出现），不能直接证明方向是早到。
        """
        now = time.perf_counter()
        with self._lock:
            key = (host, mid)
            # 非 broadcast publish（stats QoS1 ping 等）的 ack：不属于业务
            # 首包口径，直接丢弃并计数；否则会以“早到 ack”身份暂存，过期
            # 后变成 ack_before_reg（与业务发布无关的假异常）。
            if key in self._foreign_mids:
                self._foreign_mids.pop(key, None)
                self._g["ack_foreign"] += 1
                return
            rec = self._mid.pop(key, None)
            if rec is None:
                if key in self._dropped_mids:
                    self._dropped_mids.pop(key, None)
                    self._g["ack_after_drop"] += 1
                    self._g["ack_without_mid"] += 1
                    return
                self._stage_early(key, now)
                return
            bid, t0 = rec
            lat = (now - t0) * 1000.0
            if lat >= 0:
                self._add_send(host, lat)
            b = self._batches.get(bid)
            if b is not None:
                b["pending"] -= 1
                self._settle_win(b, host, bid)
                if b["done"] and b["pending"] <= 0:
                    self._batches.pop(bid, None)

    # ---------------------------- 下行 ----------------------------

    def _note_downlink(self, topic, data, broker):
        self._maybe_roll()   # 下行流量持续时也要能按时封时间切片
        if not isinstance(data, dict) or not broker:
            return
        req_id = data.get("req_id")
        if req_id:
            base = req_id.split("|", 1)[0] if isinstance(req_id, str) else req_id
            ctx = None
            try:
                with self._node.lock:
                    ctx = self._node.pending_requests.get(base)
            except Exception:
                ctx = None
            with self._lock:
                g = self._g
                self._last_rx[broker] = time.monotonic()
                g["req_obs"] += 1
                if ctx is None:
                    # 同一个 reply topic 上还会收到其他进程/设备的回包：
                    # 只计入 observed/unmatched，不参与本机首包分布与延迟，
                    # 否则胜率和延迟样本会被别人的请求污染。
                    g["req_foreign"] += 1
                    self._broker(broker)["req_foreign"] += 1
                    self._note_unmatched(topic, data, broker)
                    return
                lat = (time.perf_counter()
                       - ctx.get("start_time", time.perf_counter())) * 1000.0
                g["req_n"] += 1
                st = self._broker(broker)
                st["req_wins"] += 1
                if self._first_req_broker is None:
                    self._first_req_broker = broker
                    self._first_req_broker_at = time.monotonic() - self._t0
                if lat >= 0:
                    self._lat_add(st, "req_lat", lat)
                    self._lat_add(g, "req_lat", lat)
                    self._res_host(broker, "req_lat", lat)
                    self._res_global("req_lat", lat)
                # 端到端上行确认：回包里的 server_from 是服务端首个接到该请求
                # 的 broker（客户端本地拿不到）。同一 req_id 只计一次，避免
                # 多服务端/迟到副本把 observed 放大；缺字段（旧版服务端）或
                # 非字符串一律跳过，只降级不报错。
                srv = data.get("server_from")
                if (isinstance(base, str) and base
                        and isinstance(srv, str) and srv
                        and base not in self._srv_seen):
                    if len(self._srv_seen) >= self._SEEN_MAX:
                        self._srv_seen.pop(next(iter(self._srv_seen)), None)
                    self._srv_seen[base] = True
                    g["srv_observed"] += 1
                    self._broker(srv)["srv_wins"] += 1
                    if self._srv_first is None:
                        self._srv_first = srv
                    send_win = self._req_send_win.get(base)
                    if send_win is not None:
                        if send_win == srv:
                            g["srv_agree"] += 1
                        else:
                            g["srv_disagree"] += 1
                            self._srv_disagree_hosts[srv] = (
                                self._srv_disagree_hosts.get(srv, 0) + 1)
            return

        sid = data.get("pty") or data.get("s5") or data.get("stream")
        sid = str(sid) if sid is not None else None
        seq = data.get("seq")
        key = (sid, seq) if (sid is not None and seq is not None) else None
        with self._lock:
            self._last_rx[broker] = time.monotonic()
            st = self._broker(broker)
            st["dl_rx"] += 1
            if self._first_dl_broker is None:
                self._first_dl_broker = broker
            if sid is not None and sid not in self._sids:
                if len(self._sids) >= self._SID_MAX:
                    self._sids.pop(next(iter(self._sids)), None)
                self._sids[sid] = broker
            if key is not None:
                if key in self._dl_seen:
                    st["dl_dups"] += 1
                    self._g["dl_dups"] += 1
                else:
                    self._dl_seen[key] = broker
                    st["dl_wins"] += 1
                    self._g["dl_unique"] += 1
                    if len(self._dl_seen) > self._SEEN_MAX:
                        for k in list(self._dl_seen)[:self._SEEN_MAX // 2]:
                            self._dl_seen.pop(k, None)

        if data.get("hb") is not None:
            # 心跳帧：间隔只用本地单调钟；同一帧被同一 broker 重复投递时，
            # 用整帧内容做本地比对去重（不解析、不用远端的 hb 时间戳）。
            key = repr(data)
            now = time.monotonic()
            with self._lock:
                st = self._broker(broker)
                if key == st["hb_last_key"]:
                    st["hb_dups"] += 1
                    return
                st["hb_last_key"] = key
                st["hb_cnt"] += 1
                last = st["hb_last"]
                st["hb_last"] = now
                if last is not None:
                    gap = (now - last) * 1000.0
                    self._lat_add(st, "hb", gap)
                    self._res_host(broker, "hb", gap)

    def _note_unmatched(self, topic, data, broker):
        """记录一条「不是本机请求的回包」的来源（持锁调用）。

        只保留按 topic 的计数（最多 8 个）和最多 5 条滚动样本；样本只含
        at_s、topic、broker、req_id 与键名，绝不保存 code 等业务内容。
        at_s=记录时的 uptime 秒，用来和外部请求的时间对齐；样本保留最近
        几条（FIFO 淘汰），不是只留启动最初那几条。
        """
        try:
            t = str(topic)
            self._unmatched_topics[t] = self._unmatched_topics.get(t, 0) + 1
            if len(self._unmatched_topics) > 8:
                drop = min(self._unmatched_topics,
                           key=lambda k: self._unmatched_topics[k])
                self._unmatched_topics.pop(drop, None)
            rid = data.get("req_id")
            self._unmatched_samples.append({
                "at_s": round(time.monotonic() - self._t0, 1),
                "topic": t,
                "broker": broker,
                "req_id": rid if isinstance(rid, str) else repr(rid),
                "keys": sorted(str(k) for k in data.keys()),
            })
            if len(self._unmatched_samples) > self._UNMATCHED_SAMPLE_MAX:
                del self._unmatched_samples[
                    :len(self._unmatched_samples) - self._UNMATCHED_SAMPLE_MAX]
        except Exception:
            pass

    def set_unmatched_sample_max(self, n):
        """运行期调整未匹配回包样本上限（RPC 用，显式输入非法即报错）。

        上限只约束样本列表长度，不影响计数；调小会立刻裁掉最老的样本。
        RPC 用法：/r=first_packet_stats.set_unmatched_sample_max(20)
        """
        if isinstance(n, bool) or not isinstance(n, int) or not 0 <= n <= 256:
            raise ValueError("unmatched_sample_max 必须是 0..256 的整数")
        with self._lock:
            self._UNMATCHED_SAMPLE_MAX = n
            if n == 0:
                self._unmatched_samples.clear()
            elif len(self._unmatched_samples) > n:
                del self._unmatched_samples[:-n]
        return n

    # ---------------------------- 报告 ----------------------------

    def _configured_hosts(self):
        """从 net.brokers 解析出配置里的 host 集合（拿不到就返回空集）。"""
        try:
            net = self._net
            out = set()
            for entry in getattr(net, "brokers", None) or []:
                unpacked = net._unpack_broker(entry)
                if unpacked:
                    out.add(unpacked[0])
            return out
        except Exception:
            return set()

    def get_report(self, reset: bool = False, snapshots: bool = False) -> dict:
        """生成一份可 JSON 序列化的统计报表（reset=True 时读完即清零）。

        三层结构：
          health    总览结论（整体评分 / 按阈值归类的名单）
          brokers   每 broker 统一视图（连接 + 下行 + 上行 + 评分/角色/趋势）
          snapshots 时间切片（默认每小时一窗，看「最近是否在恶化」）；
                    snapshots=False（默认）时整块不返回——每窗 × 每 broker
                    的记录体积很大，按需用 snapshots=True 才带出
        其余 section 保留原始计数与诊断现场，字段口径见 note。
        """
        now = time.monotonic()
        with self._lock:
            self._prune()
            # 到点先封时间切片再出报表，窗口与报表口径自洽。
            self._roll_snapshot_locked(now)
            for c in self._conn_state.values():
                self._conn_flush_locked(c)   # 在线时长先结清再取快照
            g = dict(self._g)
            brok = {k: dict(v) for k, v in self._brokers.items()}
            conn = {k: dict(v) for k, v in self._conn_state.items()}
            sids = dict(self._sids)
            # done 但仍有存活 mid 的批次也算在途（与 _mid 对应），只有全部
            # on_publish 回来才被摘除；与 mids/pending_sum 的守恒关系一致。
            pending = dict(self._batches)
            pend_sum = sum(int(p["pending"]) for p in pending.values())
            mids = len(self._mid)
            early = len(self._early_acks)
            stale = self._stale_mid_count()
            orphan_writes = g["mid_orphan_write"]
            mid_ttl_dropped = g["mid_ttl_dropped"]
            ack_early_matched = g["ack_early_matched"]
            ack_before_reg = g["ack_before_reg"]
            ack_after_drop = g["ack_after_drop"]
            ack_without_mid = g["ack_without_mid"]
            ack_foreign = g["ack_foreign"]
            early_ack_source = self._early_ack_source()
            sub_counts = {h: len(t) for h, t in self._sub_topics.items()}
            sub_seeded = dict(self._sub_seeded)
            observed = g["req_obs"]
            unmatched = g["req_foreign"]
            unmatched_topics = dict(self._unmatched_topics)
            unmatched_samples = [dict(v) for v in self._unmatched_samples]
            unmatched_sample_max = int(self._UNMATCHED_SAMPLE_MAX)
            req_n = g["req_n"]
            req_first = (self._first_req_broker, self._first_req_broker_at)
            dl_unique = g["dl_unique"]
            dl_dups = g["dl_dups"]
            sessions = len(sids)
            frame_first, frame_tie = self._first_frame_broker(sids)
            up_first = self._first_ul_broker
            up_calls = g["up_calls"]
            up_first_batch = self._first_ul_bid
            up_first_at = self._first_ul_broker_at
            predicted = g["up_predicted"]
            attempted = g["up_attempted"]
            seen = g["up_seen"]
            blind = g["up_blind"]
            late = g["up_late"]
            offline = g["up_offline"]
            srv_observed = g["srv_observed"]
            srv_first = self._srv_first
            srv_agree = g["srv_agree"]
            srv_disagree = g["srv_disagree"]
            blind_map = dict(self._blind)
            late_map = dict(self._late)
            offline_map = dict(self._offline)
            absent_map = dict(self._absent)
            online_latest = dict(self._last_online)
            offline_latest = {h: dict(v)
                              for h, v in self._offline_latest.items()}
            offline_samples = [dict(v) for v in self._offline_samples]
            disagree_map = dict(self._srv_disagree_hosts)
            res = {h: {k: list(v) for k, v in d.items()}
                   for h, d in self._samples.items()}
            gres = {k: list(v) for k, v in self._gsamples.items()}
            snap_rows = list(self._snapshots) if snapshots else None
            uptime = max(0.0, now - self._t0)
            # _last_bcast_at 是相对 _t0 的秒数，now 是绝对 monotonic，
            # 必须先减去 _t0 才是「距上次广播的秒数」。
            last_bcast_at = (round(now - self._t0 - self._last_bcast_at, 1)
                             if self._last_bcast_at is not None else None)
            if reset:
                self._reset_locked()
        if self._pid != os.getpid():
            self._logger.warning(
                "PID=%s 与启动时 PID=%s 不一致：后台任务/写线程等进程状态已随"
                "fork 丢失，以下统计是本进程视角。", os.getpid(), self._pid)
        # 刻意在锁外取：connectivity 是查询瞬间的 is_connected() 快照，与
        # 上面锁内的统计快照不同时刻（note 的 connectivity 条目有说明）。
        connectivity = self._connectivity()
        totals = {
            "req": sum(b.get("req_wins", 0) for b in brok.values()),
            "dl": sum(b.get("dl_wins", 0) for b in brok.values()),
            "ul": sum(b.get("up_wins", 0) for b in brok.values()),
            "srv": sum(b.get("srv_wins", 0) for b in brok.values()),
        }
        records = {}
        for host in sorted(brok):
            st = brok[host]
            st["sub_count"] = sub_counts.get(host)   # 统一视图带订阅计数
            records[host] = self._broker_record(
                st, conn.get(host, {"state": None}),
                res.get(host, {}), totals, uptime)
        for host, r in records.items():
            r["trend"] = self._trend(host)
        health = self._health(records)
        out = {
            "uptime_s": round(uptime, 1),
            "installed": self._installed,
            "health": health,
            "brokers": records,
            "requests": {
                "n": req_n,
                "observed": observed,
                "unmatched": unmatched,
                "unmatched_rate": (round(unmatched / observed, 4)
                                   if observed else 0.0),
                "unmatched_topics": unmatched_topics,
                "unmatched_samples": unmatched_samples,
                "unmatched_sample_max": unmatched_sample_max,
                "latency_ms": self._lat_block(
                    g["req_lat_n"], g["req_lat_min"],
                    g["req_lat_sum"], g["req_lat_max"],
                    gres.get("req_lat")),
                "first_packet_broker": req_first[0],
                "first_packet_broker_at_s": req_first[1],
            },
            "pty_frames": {
                "unique": dl_unique,
                "duplicates": dl_dups,
                "duplicate_rate": (round(dl_dups / (dl_unique + dl_dups), 4)
                                   if (dl_unique + dl_dups) else 0.0),
                "sessions": sessions,
                "first_frame_broker": frame_first,
                "first_frame_broker_tie": frame_tie,
            },
            "uplink_publish": {
                "calls": up_calls,
                "brokers_queued": self._lat_block(
                    g["up_pub_n"], g["up_pub_min"],
                    g["up_pub_sum"], g["up_pub_max"]),
                "send_ms": self._lat_block(
                    g["up_send_n"], g["up_send_min"],
                    g["up_send_sum"], g["up_send_max"],
                    gres.get("up_send")),
                "first_packet_broker": up_first,
                "first_packet_broker_batch": up_first_batch,
                "first_packet_broker_at_s": up_first_at,
                "coverage": {
                    "predicted": predicted,
                    "attempted": attempted,
                    "seen": seen,
                    "blind": blind,
                    "blind_hosts": blind_map,
                    "late": late,
                    "late_hosts": late_map,
                    "offline": offline,
                    "offline_hosts": offline_map,
                    "absent_hosts": absent_map,
                    "recovered_since_offline": sorted(
                        h for h in online_latest
                        if h in offline_latest
                        and online_latest[h] > offline_latest[h]["at_s"]),
                    "recovery_same_tick": sorted(
                        h for h in online_latest
                        if h in offline_latest
                        and online_latest[h] == offline_latest[h]["at_s"]),
                    "online_latest": online_latest,
                    "offline_latest": offline_latest,
                    "offline_samples": offline_samples,
                    "broadcast_idle_s": last_bcast_at,
                },
                "server_ack": {
                    "observed": srv_observed,
                    "first_packet_broker": srv_first,
                    "vs_send_first": {
                        "agree": srv_agree,
                        "disagree": srv_disagree,
                        "disagree_hosts": disagree_map,
                    },
                },
            },
            "in_flight": {
                "batches": len(pending),
                "mids": mids,
                "pending_sum": pend_sum,
                "stale_mids": stale,
                "orphan_writes": orphan_writes,
                "mid_ttl_dropped": mid_ttl_dropped,
                "mid_ttl_dropped_by_broker": {
                    h: b["mid_ttl_dropped"] for h, b in sorted(brok.items())
                    if b.get("mid_ttl_dropped")},
                "peak_mids": self._peak_mids,
                "peak_ack_early_pending": self._peak_early,
                "ack_early_pending": early,
                "ack_early_matched": ack_early_matched,
                "ack_before_reg": ack_before_reg,
                "ack_after_drop": ack_after_drop,
                "ack_without_mid": ack_without_mid,
                "ack_foreign": ack_foreign,
                "early_ack_source": early_ack_source,
                "early_acks_key": "(host, mid)",
                "early_ttl_s": self._EARLY_TTL,
                "ack_before_reg_hosts": dict(
                    sorted(self._early_expired_hosts.items())),
                "ack_before_reg_samples": [
                    dict(v) for v in self._early_expired_samples],
            },
            "connectivity": connectivity,
            "process": {
                "pid": self._pid,
                "current_pid": os.getpid(),
                "forked": self._pid != os.getpid(),
                "born_at_s": round(self._born - self._t0, 1),
                "object_age_s": round(time.monotonic() - self._born, 1),
            },
            "subscriptions": {
                "topics": sorted(self._net.subscribed_topics),
                "seeded": sub_seeded,
                "counts": sub_counts,
            },
            "note": (
                "字段口径：报表分三层——health=总览结论（整体评分/"
                "按阈值归类名单）；brokers=每 broker 统一视图（连接+下行+上行+评分），"
                "按 host 查找是 O(1)；snapshots=时间切片（默认每小时一窗，"
                "看最近是否在恶化）。"
                "\n三处「首包/竞速」口径不同，不要混读："
                "\n1. 下行首包=多 broker 广播回程里最先回到本机的 broker"
                "（requests.first_packet_broker / brokers[].downlink."
                "first_packet_win_pct）；"
                "\n2. 上行首包=一次广播里最先写出 socket 的 broker"
                "（uplink_publish.first_packet_broker / brokers[].uplink."
                "first_packet_win_pct）；QoS0 没有应用层 ack，它主要反映每台"
                "发送队列的深浅；"
                "\n3. 服务端确认=回包 server_from=服务端实际首个收到该请求的"
                "broker（uplink_publish.server_ack / brokers[].uplink."
                "server_ack_win_pct）。这是上行的金标准：QoS0 下 1 与 3 的"
                "agree 率可能只有 25%，评判 broker 优劣以 server_ack 为准。"
                "\n延迟块（latency_ms/send_ms/hb_gap_ms）：n/min/avg/max 是"
                "全窗口精确值；p50/p90/p95/p99/jitter/std 来自最近 "
                "_RES_MAX（每 broker）/_RES_GLOBAL_MAX（全局）条滚动样本，"
                "是近似值；jitter==std，都是样本标准差。时间切片窗口里 "
                "min/max/分位为 None（极值不可差分），窗口内 n/avg 精确。"
                "\nhb_gap_ms=同一 broker 相邻两次不同心跳的本地到达间隔"
                "（不用远端 hb 时间戳）；expected=5000ms 是服务端默认 5s；"
                "frames_expected=窗口时长/5、frames_received=去重后的不同心跳"
                "帧数（hb_frames，不含 hb_dups）、frame_loss_rate="
                "(expected-received)/expected——正值是丢帧/晚上线，负值只可能"
                "来自窗口边界误差；hb_dup_rate=hb_dups/(hb_frames+hb_dups)，"
                "单独反映同一心跳被同一 broker 重复投递的比例。"
                "窗口不足 5s 时为 None。"
                "hb_frames≈uptime/5 与会话数无关（心跳全局唯一源）；明显少于 "
                "uptime/5 才是丢帧。"
                "\nconnection.online_ratio=在线时长/窗口时长，按 on_connect/"
                "on_disconnect 事件累计（重连重建 client 不丢账）；"
                "reconnect_count=connect_events-1（首次建连不算重连）；"
                "current=最后已知状态。score=100×(0.4×online_ratio+"
                "0.3×lat_score+0.3×delivery)，lat_score=1-avg_req_lat/2000 "
                "截断 0..1（无样本 0.5），delivery=1-max(0,frame_loss_rate)"
                "-0.5×duplicate_rate 截断 0..1（无下行帧 0.5）。role："
                "dead=online_ratio<0.5；primary=score≥80；backup="
                "60≤score<80；degraded=score<60。trend=时间切片里 "
                "requests.latency_ms.avg 的环比：>+20% degrading、<-20% "
                "improving、近三窗正负交替 oscillating、其余或样本不足 "
                "stable。"
                "\nunmatched=同一 reply topic 上其他设备/进程的回包，"
                "unmatched_rate=unmatched/observed；样本只留键名与来源，"
                "绝不保存业务内容；上限 set_unmatched_sample_max 可运行期调。"
                "\nin_flight：mids=在途 QoS0 发布槽（未等到 on_publish 的 "
                "(host, mid)）；stale_mids=指向不存在批次的 mid（正常应为 0）；"
                "orphan_writes=批次结束后的迟到写入（正常应为 0）；"
                "mid_ttl_dropped=120s 等不到 on_publish 的回收次数，应等于 "
                "queued−send_ms.n 的缺口，mid_ttl_dropped_by_broker 按 host "
                "拆分；peak_*=历史峰值（瞬时值 0 不代表从未积压）；"
                "ack_early_pending=已收到但还没登记的早到 ack（常态 <1，随登记"
                "随即消费）；ack_without_mid 恒等于 ack_before_reg+"
                "ack_after_drop；ack_foreign=stats ping 的 ack，不进前两者。"
                "\nsnapshots：窗口=相邻两次滚动时刻的累计差分，默认 1 小时一窗"
                "（_SNAP_INTERVAL），窗口边界是滚动时刻不是整点，静默期会并窗；"
                "该块体积=窗口数×broker数，默认不返回，get_report(snapshots=True)"
                " 才带出（trend 仍用内部切片计算，不受影响）。"
                "\ncoverage：predicted=按连接状态预测本批要发的 host 数；"
                "attempted=publish 实际进包装器的 host 数（恒等于 seen+blind）；"
                "blind=有下行却无上行样本（client 被重建/未包装，publish 绕过了"
                "wrapper）；late=预测离线但扇出时已在线；offline=发布时刻 "
                "is_connected()=假（offline_samples/offline_latest 是现场）；"
                "absent_hosts=包安装了但不在 clients；broadcast_idle_s=距上次"
                "广播的秒数。"
                "\nget_report(reset=True)=读完清零本窗口（计数/在线率/峰值/"
                "时间切片全部重开），_born 与 PID 不变；uptime 是本报表覆盖的"
                "窗口时长。connectivity.connected 是查询瞬间值，与上面的统计"
                "快照不同时刻，属正常。"
            ),
        }
        if snap_rows is not None:
            out["snapshots"] = snap_rows
        return out

    def _connectivity(self):
        net = getattr(self._node, "mqtt_net", None)
        if net is None:
            return {"configured": 0, "clients": 0,
                    "connected": 0, "disconnected_hosts": []}
        try:
            hosts = (getattr(net, "_configured_hosts", None)
                     or getattr(net, "brokers", None)
                     or list(getattr(net, "clients", {}) or {}))
        except Exception:
            hosts = []
        try:
            clients = getattr(net, "clients", {}) or {}
        except Exception:
            clients = {}
        connected = [h for h, c in clients.items()
                     if bool(getattr(c, "is_connected", None)
                             and c.is_connected())]
        disc = [h for h in clients.keys() if h not in set(connected)]
        return {
            "configured": len(hosts),
            "clients": len(clients),
            "connected": len(connected),
            "disconnected_hosts": disc,
        }

    @staticmethod
    def _first_frame_broker(sids):
        """首帧领先 broker：样本最多的首帧 broker；并列取最早进入计数的
        broker（dict 插入序，确定性），并列清单见 first_frame_broker_tie。"""
        cnt = {}
        for b in sids.values():
            cnt[b] = cnt.get(b, 0) + 1
        if not cnt:
            return None, []
        mx = max(cnt.values())
        tied = [b for b, n in cnt.items() if n == mx]
        return tied[0], sorted(tied) if len(tied) > 1 else []

    def _lat_block(self, n, mn, total, mx, samples=None):
        """延迟块：n/min/avg/max 全窗口精确；p50..p99 与 jitter/std 来自
        最近样本库（滚动，见 _RES_MAX/_RES_GLOBAL_MAX），是近似值；
        jitter==std，都是样本标准差。samples=None（时间切片窗口）时
        分位数为 None。"""
        out = {"n": n,
               "min": round(mn, 1) if mn is not None else None,
               "avg": round(total / n, 1) if n else None,
               "max": round(mx, 1) if mx is not None else None}
        vals = list(samples) if samples else []
        for q, tag in ((0.50, "p50"), (0.90, "p90"),
                       (0.95, "p95"), (0.99, "p99")):
            out[tag] = self._pct(vals, q)
        sd = self._std(vals)
        out["std"] = sd
        out["jitter"] = sd
        return out

    @staticmethod
    def _win_pct(v, total):
        return round(v * 100.0 / total, 1) if total else 0.0

    def _broker_record(self, st, conn, res, totals, span, window=False):
        """每 broker 统一视图：连接 + 下行 + 上行 + 评分/角色（窗口与累计
        同构，故时间切片窗口也走这里）。"""
        res = res or {}

        def num(k):
            v = st.get(k)
            return v if isinstance(v, (int, float)) and not isinstance(
                v, bool) else 0

        rx = num("dl_rx")
        wins = num("dl_wins")
        dups = num("dl_dups")
        hb_frames = num("hb_cnt")
        hb_dups = num("hb_dups")
        unc = rx - wins - dups - hb_frames - hb_dups
        req_wins = num("req_wins")
        hb_expected = span / self.HB_EXPECTED_S
        # frames_received 只算去重后的不同心跳帧；hb_dups 单独成率，
        # 否则重复投递会被当成"多收到的帧"，把丢帧率压成负的大数。
        frames_received = hb_frames
        frame_loss_rate = (
            round((hb_expected - frames_received) / hb_expected, 4)
            if hb_expected >= 1.0 else None)
        hb_deliveries = hb_frames + hb_dups
        hb_dup_rate = round(hb_dups / hb_deliveries, 4) if hb_deliveries else None
        dl = {
            "rx": rx,
            "wins": wins,
            "first_packet_win_pct": self._win_pct(wins, totals["dl"]),
            "dups": dups,
            "duplicate_rate": round(dups / rx, 4) if rx else 0.0,
            "unclassified": unc,
            "subscription_count": st.get("sub_count"),
            "req_wins": req_wins,
            "req_win_pct": self._win_pct(req_wins, totals["req"]),
            "unmatched": num("req_foreign"),
            "latency_ms": self._lat_block(
                num("req_lat_n"), st.get("req_lat_min"),
                num("req_lat_sum"), st.get("req_lat_max"),
                res.get("req_lat")),
            "hb_frames": hb_frames,
            "hb_dups": hb_dups,
            "hb_dup_rate": hb_dup_rate,
            "hb_gap_ms": dict(self._lat_block(
                num("hb_n"), st.get("hb_min"), num("hb_sum"),
                st.get("hb_max"), res.get("hb")),
                expected=self.HB_EXPECTED_MS),
            "frames_expected": round(hb_expected, 1),
            "frames_received": frames_received,
            "frame_loss_rate": frame_loss_rate,
        }
        queued = num("up_queued")
        send_n = num("up_send_n")
        ttl = num("mid_ttl_dropped")
        ul = {
            "queued": queued,
            "failed": num("up_failed"),
            "wins": num("up_wins"),
            "first_packet_win_pct": self._win_pct(
                num("up_wins"), totals["ul"]),
            "send_ms": self._lat_block(
                send_n, st.get("up_send_min"), num("up_send_sum"),
                st.get("up_send_max"), res.get("up_send")),
            "server_ack_wins": num("srv_wins"),
            "server_ack_win_pct": self._win_pct(num("srv_wins"),
                                                totals["srv"]),
            "mid_ttl_dropped": ttl,
            "mid_ttl_drop_rate": round(ttl / queued, 4) if queued else 0.0,
            "queued_vs_acked_gap": queued - send_n,
        }
        # 评分：0.4×在线率 + 0.3×往返延迟 + 0.3×送达质量，各项截断 0..1
        conn_blk = self._conn_block(conn, span, window=window)
        avail = conn_blk["online_ratio"]
        avail_s = avail if avail is not None else 0.5
        lat_n = num("req_lat_n")
        if lat_n:
            lat_s = max(0.0, min(1.0, 1.0
                                 - (num("req_lat_sum") / lat_n) / 2000.0))
        else:
            lat_s = 0.5
        if rx:
            loss = max(0.0, frame_loss_rate or 0.0)
            deliv_s = max(0.0, min(1.0, 1.0 - loss - 0.5 * (dups / rx)))
        else:
            deliv_s = 0.5
        score = round(100.0 * (0.4 * avail_s + 0.3 * lat_s
                               + 0.3 * deliv_s), 1)
        if avail is not None and avail < 0.5:
            role = "dead"
        elif score >= 80.0:
            role = "primary"
        elif score >= 60.0:
            role = "backup"
        else:
            role = "degraded"
        return {"connection": conn_blk, "downlink": dl, "uplink": ul,
                "score": score, "role": role, "trend": None}

    def _trend(self, host):
        """趋势=时间切片里 requests.latency_ms.avg 的环比（只认延迟：
        流量涨不等于变差）。样本不足两个窗口 -> stable。"""
        vals = []
        for w in self._snapshots[-3:]:
            row = w.get("brokers", {}).get(host)
            if not row:
                continue
            lat = row.get("downlink", {}).get("latency_ms", {})
            if lat.get("n") and lat.get("avg") is not None:
                vals.append(lat["avg"])
        if len(vals) < 2 or not vals[-2]:
            return "stable"
        d1 = (vals[-1] - vals[-2]) / vals[-2]
        if d1 > 0.2:
            return "degrading"
        if d1 < -0.2:
            return "improving"
        if len(vals) >= 3 and vals[-2] and vals[-3]:
            d2 = (vals[-2] - vals[-3]) / vals[-3]
            if d1 * d2 < 0 and abs(d1) > 0.1 and abs(d2) > 0.1:
                return "oscillating"
        return "stable"

    @staticmethod
    def _health(records):
        """总览结论：整体评分 + 按阈值归类的名单（只陈述数据，不给建议）。"""
        scores = [r["score"] for r in records.values()]
        overall = round(sum(scores) / len(scores), 1) if scores else None
        dead = sorted(h for h, r in records.items() if r["role"] == "dead")
        degraded = sorted(h for h, r in records.items()
                          if r["role"] == "degraded")
        return {
            "overall_score": overall,
            "degraded_brokers": dead + degraded,
        }

    # ---------------------------- 内部工具 ----------------------------

    def _broker(self, host):
        st = self._brokers.get(host)
        if st is None:
            st = {
                "dl_rx": 0, "dl_wins": 0, "dl_dups": 0,
                "hb_cnt": 0, "hb_last": None,
                "hb_dups": 0, "hb_last_key": None,
                "hb_n": 0, "hb_min": None, "hb_sum": 0.0, "hb_max": None,
                "req_wins": 0, "req_foreign": 0,
                "req_lat_n": 0, "req_lat_min": None,
                "req_lat_sum": 0.0, "req_lat_max": None,
                "up_queued": 0, "up_failed": 0,
                "up_send_n": 0, "up_send_min": None,
                "up_send_sum": 0.0, "up_send_max": None,
                "up_wins": 0,
                "srv_wins": 0,
                "mid_ttl_dropped": 0,
            }
            self._brokers[host] = st
        return st

    @staticmethod
    def _lat_add(st, prefix, value):
        st[prefix + "_n"] += 1
        st[prefix + "_sum"] += value
        mn = st[prefix + "_min"]
        if mn is None or value < mn:
            st[prefix + "_min"] = value
        mx = st[prefix + "_max"]
        if mx is None or value > mx:
            st[prefix + "_max"] = value

    def _stale_mid_count(self):
        """在途表里指向已消失批次的 mid 数（正常应为 0，报警口径见 note）。"""
        return sum(1 for bid, _t0 in self._mid.values()
                   if bid not in self._batches)

    def _early_ack_source(self):
        """早到 ack 的来源分流：matched=登记时匹配上的业务 ack；
        ping_dropped=被 _foreign_mids 认领的非广播（stats QoS1 ping /
        业务直发）；unknown_expired=既没等到登记也没被认领，只能记为
        未归属（ack_before_reg 的成分）。"""
        g = self._g
        return {"business_matched": g["ack_early_matched"],
                "ping_dropped": g["ack_foreign"],
                "unknown_expired": g["ack_before_reg"]}

    # ---------------- 样本库（分位数/抖动，滚动） ----------------

    def _push_res(self, store, key, value, cap):
        res = store.get(key)
        if res is None:
            res = []
            store[key] = res
        res.append(value)
        if len(res) > cap:
            del res[:len(res) - cap]

    def _res_host(self, host, key, value):
        self._push_res(self._samples.setdefault(host, {}), key, value,
                       self._RES_MAX)

    def _res_global(self, key, value):
        self._push_res(self._gsamples, key, value, self._RES_GLOBAL_MAX)

    @staticmethod
    def _pct(samples, q):
        """最近样本的近似分位数（近似秩）。空样本返回 None。"""
        if not samples:
            return None
        vals = sorted(samples)
        idx = int(q * len(vals))
        if idx >= len(vals):
            idx = len(vals) - 1
        return round(vals[idx], 1)

    @staticmethod
    def _std(samples):
        """样本标准差（抖动）。空样本返回 None。"""
        n = len(samples)
        if not n:
            return None
        mean = sum(samples) / n
        var = sum((x - mean) ** 2 for x in samples) / n
        return round(var ** 0.5, 1)

    # ---------------- 连接事件（online_ratio 用） ----------------

    def _conn_rec(self, host):
        c = self._conn_state.get(host)
        if c is None:
            c = {"state": None, "since": time.monotonic(),
                 "online_s": 0.0, "offline_s": 0.0,
                 "connects": 0, "disconnects": 0,
                 "last_connect_at_s": None, "last_disconnect_at_s": None}
            self._conn_state[host] = c
        return c

    def _conn_seed_locked(self, host, connected):
        """包装时按 is_connected() 记初始状态，不计事件。"""
        c = self._conn_rec(host)
        if c["state"] is None:
            c["state"] = bool(connected)
            c["since"] = time.monotonic()

    def _conn_flush_locked(self, c):
        """把当前状态的未入账时长并入累计量，保持累计量连续可差分。"""
        now = time.monotonic()
        if c.get("state") is not None:
            dt = now - c["since"]
            if dt > 0:
                if c["state"]:
                    c["online_s"] += dt
                else:
                    c["offline_s"] += dt
            c["since"] = now

    def _conn_event_locked(self, host, connected):
        """连接事件：先结清上一段状态的时长，再翻状态、计数。"""
        c = self._conn_rec(host)
        self._conn_flush_locked(c)
        if c["state"] != connected:
            c["state"] = connected
            c["since"] = time.monotonic()
        if connected:
            c["connects"] += 1
            c["last_connect_at_s"] = round(time.monotonic() - self._t0, 1)
        else:
            c["disconnects"] += 1
            c["last_disconnect_at_s"] = round(time.monotonic() - self._t0, 1)

    def _conn_block(self, c, span, window=False):
        """连接块：online_ratio = 在线时长 / 窗口时长（事件口径，非瞬时）。"""
        online = float(c.get("online_s", 0.0))
        offline = float(c.get("offline_s", 0.0))
        state = c.get("state")
        unknown = (state is None and not c.get("connects")
                   and not c.get("disconnects")
                   and not online and not offline)
        block = {
            "online_ratio": (
                None if unknown else
                (round(online / span, 4) if span > 0 else None)),
            "online_s": round(online, 1),
            "offline_s": round(offline, 1),
            "connect_events": c.get("connects", 0),
            "disconnect_events": c.get("disconnects", 0),
        }
        if not window:
            block["reconnect_count"] = max(0, c.get("connects", 0) - 1)
            block["last_connect_at_s"] = c.get("last_connect_at_s")
            block["last_disconnect_at_s"] = c.get("last_disconnect_at_s")
            block["current"] = (
                "online" if state else "offline"
            ) if state is not None else "unknown"
        return block

    # ---------------- 时间切片（滚动窗口） ----------------

    def _maybe_roll(self):
        """事件入口便宜检查：到点才进锁封窗口。"""
        try:
            if time.monotonic() - self._snap_at < self._SNAP_INTERVAL:
                return
            with self._lock:
                self._roll_snapshot_locked(time.monotonic())
        except Exception:
            pass

    def _roll_snapshot_locked(self, now):
        """到点把累计计数与锚点求差，封一个时间切片窗口（持锁调用）。

        窗口内 n/avg 精确（计数与求和可差分）；min/max/分位数不可差分，
        窗口里为 None。窗口边界是滚动时刻，不是严格整点。
        """
        if now - self._snap_at < self._SNAP_INTERVAL:
            return
        anchor = self._snap_anchor
        for c in self._conn_state.values():
            self._conn_flush_locked(c)   # 累计量先结清，窗口差分才精确
        cur_b = {h: dict(st) for h, st in self._brokers.items()}
        cur_c = {h: dict(c) for h, c in self._conn_state.items()}
        span = max(0.0, now - self._snap_at)
        rows = {}
        for host in sorted(set(anchor["brokers"]) | set(cur_b)):
            rows[host] = self._window_row(
                cur_b.get(host, {}), anchor["brokers"].get(host, {}),
                cur_c.get(host, {}), anchor["conn"].get(host, {}), span)
        self._snapshots.append({
            "window_start_s": round(self._snap_at - self._t0, 1),
            "window_end_s": round(now - self._t0, 1),
            "brokers": rows})
        if len(self._snapshots) > self._SNAP_MAX:
            del self._snapshots[:-self._SNAP_MAX]
        self._snap_anchor = {"brokers": cur_b,
                             "g": dict(self._g), "conn": cur_c}
        self._snap_at = now

    # 差分时不搬这些键：min/max/last 是极值或状态量，不是流量。
    _WIN_SKIP = ("req_lat_min", "req_lat_max", "up_send_min", "up_send_max",
                 "hb_min", "hb_max", "hb_last", "hb_last_key", "sub_count")

    def _window_row(self, st, st0, conn, conn0, span):
        d = {}
        for k, v in st.items():
            if k in self._WIN_SKIP or isinstance(v, bool):
                continue
            if not isinstance(v, (int, float)):
                continue
            base = st0.get(k)
            if not isinstance(base, (int, float)) or isinstance(base, bool):
                base = 0
            d[k] = v - base
        for k in self._WIN_SKIP:
            if k in st and k != "sub_count":
                d[k] = None
        c = {}
        for k, v in conn.items():
            if k in ("state", "since", "last_connect_at_s",
                     "last_disconnect_at_s"):
                continue
            if not isinstance(v, (int, float)) or isinstance(v, bool):
                continue
            base = conn0.get(k)
            if not isinstance(base, (int, float)) or isinstance(base, bool):
                base = 0
            c[k] = v - base
        return self._broker_record(
            d, c, None, {"req": 0, "dl": 0, "ul": 0, "srv": 0}, span,
            window=True)

    def _prune(self):
        """只在持锁时调用：给批次/mid 表兜底，防止 on_publish 永不回来。"""
        if len(self._batches) > self._BATCH_MAX:
            evicted = list(self._batches)[:self._BATCH_MAX // 2]
            for k in evicted:
                self._batches.pop(k, None)
            # 批次被淘汰时，它的在途 mid 也必须一起丢弃，否则这些 mid
            # 会一直指向不存在的批次，变成报告里的 stale_mids。
            if evicted:
                dead = set(evicted)
                for k, rec in list(self._mid.items()):
                    if rec[0] in dead:
                        self._mid.pop(k, None)
        # 超时的 mid 一定等不到 on_publish 了（正常回执最长不过数十秒），
        # 直接丢弃，避免长期运行持续增长。
        now = time.perf_counter()
        for k, (_bid, t0) in list(self._mid.items()):
            if now - t0 > self._MID_TTL:
                self._drop_mid(k)
                # 记下这个 mid 已被回收：之后如果 on_publish 才到，就能区分
                # 成“迟到 ack”，不会误当成早到而永远暂存。
                self._mark_dropped(k)
                # 显式计数：mid 等不到 on_publish、被 TTL 静默回收的次数。
                # 报告里 send_ms.n 与 queued 的差额应等于它，否则说明还有
                # 别的丢样本路径（批次淘汰/orphan）。
                self._g["mid_ttl_dropped"] += 1
                # 按 host 拆分（in_flight.mid_ttl_dropped_by_broker），
                # 让 TTL 丢失能定位到具体 broker。
                self._broker(k[0])["mid_ttl_dropped"] += 1
        # 早到 ack 等登记超过 _EARLY_TTL 仍未等到 -> 未归属，计入
        # ack_before_reg；正常的 publish 调用返回就在微秒级，过期即泄漏。
        for k, at in list(self._early_acks.items()):
            if now - at > self._EARLY_TTL:
                self._early_acks.pop(k, None)
                self._expire_early(k, at, now, "ttl")
        for k, at in list(self._dropped_mids.items()):
            if now - at > self._DROPPED_TTL:
                self._dropped_mids.pop(k, None)
        for k, at in list(self._foreign_mids.items()):
            if now - at > self._FOREIGN_TTL:
                self._foreign_mids.pop(k, None)

    def _reset_locked(self):
        self._t0 = time.monotonic()
        self._batches.clear()
        self._mid.clear()
        self._early_acks.clear()
        self._dropped_mids.clear()
        self._foreign_mids.clear()
        self._early_expired_hosts.clear()
        self._early_expired_samples.clear()
        self._dl_seen.clear()
        self._sids.clear()
        self._first_req_broker = None
        self._first_req_broker_at = None
        self._first_dl_broker = None
        self._first_ul_broker = None
        self._first_ul_broker_at = None
        self._first_ul_bid = None
        self._unmatched_topics.clear()
        self._unmatched_samples.clear()
        self._blind.clear()
        self._absent.clear()
        self._offline.clear()
        self._late.clear()
        self._offline_samples.clear()
        self._offline_latest.clear()
        self._last_rx.clear()
        self._last_online.clear()
        self._last_bcast_at = None
        self._req_send_win.clear()
        self._srv_seen.clear()
        self._srv_first = None
        self._srv_disagree_hosts.clear()
        self._sub_topics.clear()
        self._sub_seeded.clear()
        self._brokers.clear()
        # _g 按初值模板整体归零：min/max 类字段必须回到 None（置 0 会把
        # 后续 min 卡死在 0），sum 类回 0.0/0，计数回 0——不按类型猜。
        self._g = dict(self._G_INIT)
        # 样本库/峰值/时间切片同样随窗口清零；连接状态保留当前值，
        # 只归零累计量，让 online_ratio 在新窗口从此刻重新计。
        self._samples.clear()
        self._gsamples.clear()
        self._peak_mids = 0
        self._peak_early = 0
        now = self._t0
        for c in self._conn_state.values():
            c["online_s"] = 0.0
            c["offline_s"] = 0.0
            c["connects"] = 0
            c["disconnects"] = 0
            c["last_connect_at_s"] = None
            c["last_disconnect_at_s"] = None
            c["since"] = now
        self._snapshots.clear()
        self._snap_at = now
        self._snap_anchor = {"brokers": {}, "g": dict(self._G_INIT),
                             "conn": {}}


# 模块级单例：与 parts 一起 exec 进扁平命名空间，因此 1188 的 /r= 可直接
# 访问 first_packet_stats.get_report()；create 在 import 期无副作用。
first_packet_stats = globals().setdefault("first_packet_stats",
                                          FirstPacketStats())
