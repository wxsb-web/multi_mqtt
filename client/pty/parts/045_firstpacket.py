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

    def __init__(self):
        self._lock = threading.Lock()
        self._t0 = time.monotonic()
        self._born = self._t0          # 对象创建时刻，reset 不重置
        self._pid = os.getpid()
        self._created_epoch = time.time()
        self._installed = False
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
        self._brokers = {}        # host -> 计数器 dict
        self._g = {
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
            c._fp_wrapped = True
        except Exception:
            pass

    # ---------------------------- 上行 ----------------------------

    def _publish_broadcast(self, orig, topic, payload_dict, args, kw):
        """包住一次广播：建批次 → 记每个 broker 的 publish/on_publish。"""
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

    def _add_send(self, host, lat):
        """记一条发送耗时样本（持锁调用）。"""
        st = self._broker(host)
        self._lat_add(st, "up_send", lat)
        self._lat_add(self._g, "up_send", lat)

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
                    self._lat_add(st, "hb", (now - last) * 1000.0)

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

    def get_report(self, reset: bool = False) -> dict:
        """返回 JSON 可序列化的统计报告；reset=True 读完清零。"""
        with self._lock:
            # 顺带做一次兜底清理：让 _MID_TTL 不再只在“有上行流量”时才生效，
            # 长时间只读不写的进程也能把超时 mid 及时回收。
            self._prune()
            brokers = {k: dict(v) for k, v in self._brokers.items()}
            g = dict(self._g)
            first_ul = self._first_ul_broker
            first_ul_at = self._first_ul_broker_at
            first_ul_bid = self._first_ul_bid
            first_req_at = self._first_req_broker_at
            unmatched_topics = dict(self._unmatched_topics)
            unmatched_samples = list(self._unmatched_samples)
            unmatched_sample_max = self._UNMATCHED_SAMPLE_MAX
            blind = dict(self._blind)
            absent = dict(self._absent)
            offline = dict(self._offline)
            late = dict(self._late)
            offline_latest = {k: dict(v)
                              for k, v in self._offline_latest.items()}
            online_latest = dict(self._last_online)
            offline_samples = [dict(s) for s in self._offline_samples]
            sids = dict(self._sids)
            pending_batches = len(self._batches)
            pending_mids = len(self._mid)
            early_pending = len(self._early_acks)
            early_expired_hosts = dict(self._early_expired_hosts)
            early_expired_samples = [dict(s)
                                     for s in self._early_expired_samples]
            pending_sum = sum(b["pending"] for b in self._batches.values())
            stale_mids = sum(1 for bid, _t0 in self._mid.values()
                             if bid not in self._batches)
            uptime = time.monotonic() - self._t0
            last_bcast_at = self._last_bcast_at
            srv_first = self._srv_first
            srv_disagree_hosts = dict(self._srv_disagree_hosts)
            sub_counts = {h: len(t) for h, t in self._sub_topics.items()}
            # 派生：离线之后又被检查到在线的 host（online_latest >
            # offline_latest.at_s）。启动期离线、之后才连上的 host 会出现在这里。
            # 用严格 >：at_s 只精确到 0.1s，若两者相等，说明同一粒度内既被
            # 检查到离线、也被检查到在线，先后方向不可判，单列 same_tick，
            # 不并入 recovered（否则可能把“最后仍离线”误报成已恢复）。
            recovered = sorted(
                h for h, at in online_latest.items()
                if h in offline_latest
                and at > offline_latest[h].get("at_s", -1))
            same_tick = sorted(
                h for h, at in online_latest.items()
                if h in offline_latest
                and at == offline_latest[h].get("at_s"))
            pid = self._pid
            current_pid = os.getpid()
            object_age = time.monotonic() - self._born
            expected_topics = set()
            try:
                net = self._net
                if net is not None:
                    expected_topics = {
                        str(t) for t in
                        (getattr(net, "subscribed_topics", None) or ())}
            except Exception:
                expected_topics = set()
            if reset:
                self._reset_locked()
        connectivity = self._connectivity()
        total_req = sum(b["req_wins"] for b in brokers.values())
        total_dl = sum(b["dl_wins"] for b in brokers.values())
        total_ul = sum(b["up_wins"] for b in brokers.values())
        total_srv = sum(b["srv_wins"] for b in brokers.values())

        def pct(v, total):
            return round(100.0 * v / total, 1) if total else 0.0

        req_rows = []
        for host, st in sorted(brokers.items(),
                               key=lambda kv: -kv[1]["req_wins"]):
            if not (st["req_wins"] or st["req_lat_n"]
                    or st["req_foreign"]):
                continue
            row = {
                "broker": host,
                "wins": st["req_wins"],
                "win_pct": pct(st["req_wins"], total_req),
                "latency_ms": self._lat_block(
                    st["req_lat_n"], st["req_lat_min"],
                    st["req_lat_sum"], st["req_lat_max"]),
            }
            if st["req_foreign"]:
                row["unmatched"] = st["req_foreign"]
            req_rows.append(row)

        dl_rows = []
        for host, st in sorted(brokers.items(),
                               key=lambda kv: -kv[1]["dl_wins"]):
            if not st["dl_rx"]:
                continue
            row = {
                "broker": host,
                "rx": st["dl_rx"],
                "wins": st["dl_wins"],
                "dups": st["dl_dups"],
                "win_pct": pct(st["dl_wins"], total_dl),
            }
            if st["hb_cnt"]:
                row["hb_frames"] = st["hb_cnt"]
            if st["hb_dups"]:
                row["hb_dups"] = st["hb_dups"]
            # 与 hb_dups 并列：该 host 当前活 topic filter 数。hb_dups>0 且
            # subscription_count>1 -> 多个 filter 同时匹配同一帧（形态 A）；
            # hb_dups>0 但 subscription_count==1 -> 单订阅仍重复，通常是
            # 同 host 的第二条活连接/旧 client 残留（形态 B）。
            if host in sub_counts:
                row["subscription_count"] = sub_counts[host]
            # 逐 host 残差：rx 里既没算进 unique/dup，也没算进 hb 的帧
            # （无 sid/seq 的非心跳下行，例如只有 pty 字段的控制帧）。
            # = rx − wins − dups − hb_frames − hb_dups，正常应为 0；
            # 非 0 说明还有未分类的下行路径。
            row["unclassified"] = (
                st["dl_rx"] - st["dl_wins"] - st["dl_dups"]
                - st["hb_cnt"] - st["hb_dups"])
            if st["hb_n"]:
                row["hb_gap_ms"] = self._lat_block(
                    st["hb_n"], st["hb_min"], st["hb_sum"], st["hb_max"])
            dl_rows.append(row)

        ul_rows = []
        for host, st in sorted(brokers.items(),
                               key=lambda kv: -kv[1]["up_queued"]):
            if not (st["up_queued"] or st["up_failed"] or st["up_send_n"]):
                continue
            ul_rows.append({
                "broker": host,
                "queued": st["up_queued"],
                "failed": st["up_failed"],
                "wins": st["up_wins"],
                "win_pct": pct(st["up_wins"], total_ul),
                "send_ms": self._lat_block(
                    st["up_send_n"], st["up_send_min"],
                    st["up_send_sum"], st["up_send_max"]),
            })

        srv_rows = []
        for host, st in sorted(brokers.items(),
                               key=lambda kv: -kv[1]["srv_wins"]):
            if not st["srv_wins"]:
                continue
            srv_rows.append({
                "broker": host,
                "wins": st["srv_wins"],
                "win_pct": pct(st["srv_wins"], total_srv),
            })

        return {
            "uptime_s": round(uptime, 1),
            "installed": self._installed,
            # 跨进程排查：pid 是创建本统计对象时的进程，current_pid 是现在
            # 提供这份报告的进程。forked=True 说明统计状态被 fork 继承
            # （计数器沿用父进程，此时 hb_frames 与 uptime 可能对不上）；
            # 正常应为 False；object_age_s 是对象创建至今（reset 不影响），
            # 与 uptime_s 不等说明期间做过 reset，计数只覆盖 reset 之后。
            "process": {"pid": pid, "current_pid": current_pid,
                        "forked": current_pid != pid,
                        "created_at": round(self._created_epoch, 1),
                        "object_age_s": round(object_age, 1)},
            "subscriptions": {"count": len(expected_topics),
                              "topics": sorted(expected_topics)},
            "connectivity": connectivity,
            "requests": {
                "n": g["req_n"],
                "observed": g["req_obs"],
                "unmatched": g["req_foreign"],
                "unmatched_topics": unmatched_topics,
                "unmatched_samples": unmatched_samples,
                # 当前生效的样本上限，可用
                # /r=first_packet_stats.set_unmatched_sample_max(n) 运行期调整。
                "unmatched_sample_max": unmatched_sample_max,
                "latency_ms": self._lat_block(
                    g["req_lat_n"], g["req_lat_min"],
                    g["req_lat_sum"], g["req_lat_max"]),
                "first_packet_broker": self._first_req_broker,
                "first_packet_broker_at_s": (
                    round(first_req_at, 1) if first_req_at is not None
                    else None),
                "broker_distribution": req_rows,
            },
            "pty_frames": {
                "unique": g["dl_unique"],
                "duplicates": g["dl_dups"],
                # 本窗口见过多少个不同 sid，= 条并发/残留的 PTY 会话数。
                # 心跳是全局唯一源、与会话数无关：不论几个会话都约每 5s 一帧，
                # 因此 sessions>1 不会让 hb_frames 变成 sessions×uptime/5。
                "sessions": len(sids),
                "first_frame_broker": self._first_frame_broker(sids, brokers),
                "broker_distribution": dl_rows,
            },
            "uplink_publish": {
                "calls": g["up_calls"],
                "brokers_queued": self._lat_block(
                    g["up_pub_n"], g["up_pub_min"],
                    g["up_pub_sum"], g["up_pub_max"]),
                "first_packet_broker": first_ul,
                "first_packet_broker_batch": first_ul_bid,
                "first_packet_broker_at_s": (
                    round(first_ul_at, 1) if first_ul_at is not None
                    else None),
                "send_ms": self._lat_block(
                    g["up_send_n"], g["up_send_min"],
                    g["up_send_sum"], g["up_send_max"]),
                # 覆盖诊断：
                #   predicted = 广播开始前预测该发的 (host,批) 次数
                #   attempted = predicted ∪ seen，恒有 seen <= attempted，
                #               且 attempted == seen + blind
                #   seen      = 实际进了 publish 包装器的次数
                #   blind     = 预测该发却没进包装器（client 被重建/未包装/
                #               扇出前掉线）
                #   late      = 预测时还没连上、真正扇出时已连上（seen 多出来的
                #               那部分，旧口径下会造成 seen > attempted）
                #   absent    = 配置里有、但当时不在 net.clients 的 host
                "coverage": {
                    "predicted": g["up_predicted"],
                    "attempted": g["up_attempted"],
                    "seen": g["up_seen"],
                    "blind": g["up_blind"],
                    "blind_hosts": blind,
                    "late": g["up_late"],
                    "late_hosts": late,
                    "offline": g["up_offline"],
                    "offline_hosts": offline,
                    "absent_hosts": absent,
                    "offline_samples": offline_samples,
                    # 每个 host 只留最近一次离线现场：稳态离线不会被时间轴
                    # 滚动窗口挤掉，用于回答“它现在到底还离线吗”。
                    "offline_latest": offline_latest,
                    # host -> 最近一次检查时 is_connected() 为真的 at_s。
                    # 与 offline_latest 配对：online_latest > offline_latest.at_s
                    # 表示离线后已恢复；两者都停在启动期则说明广播只发生在启动期。
                    "online_latest": online_latest,
                    # 派生：离线后又被检查到在线的 host 名单。
                    "recovered_since_offline": recovered,
                    "recovery_same_tick": same_tick,
                    # 最近一次 broadcast 距今多少秒；None=本窗口还没广播过。
                    # 数值大说明广播早就停了，此时 offline/online_latest 停止
                    # 更新是正常现象，不代表 host 现在仍离线。
                    "broadcast_idle_s": (
                        None if last_bcast_at is None
                        else round(uptime - last_bcast_at, 1)),
                },
                "server_ack": {
                    # 只统计本机匹配到 pending_requests、且回包里带合法
                    # server_from 的响应；同一 req_id 只计一次。
                    "observed": g["srv_observed"],
                    "broker_distribution": srv_rows,
                    "first_packet_broker": srv_first,
                    # 与同一 req_id 的发送侧首个 on_publish broker 对照：
                    # agree=同一条路；disagree=包先排出 socket 的 broker 与
                    # 服务端实际先收到的 broker 不同（disagree_hosts 按
                    # server_from 计）。发送侧首包未知（批次还没等到 ack）
                    # 时两者都不计。
                    "vs_send_first": {
                        "agree": g["srv_agree"],
                        "disagree": g["srv_disagree"],
                        "disagree_hosts": srv_disagree_hosts,
                    },
                },
                "broker_distribution": ul_rows,
            },
            "in_flight": {"batches": pending_batches, "mids": pending_mids,
                          "pending_sum": pending_sum,
                          "stale_mids": stale_mids,
                          "orphan_writes": g["mid_orphan_write"],
                          "mid_ttl_dropped": g["mid_ttl_dropped"],
                          "ack_early_pending": early_pending,
                          "ack_early_matched": g["ack_early_matched"],
                          "ack_before_reg": g["ack_before_reg"],
                          "ack_after_drop": g["ack_after_drop"],
                          "ack_without_mid": g["ack_without_mid"],
                          "ack_foreign": g["ack_foreign"],
                          # 早到 ack 的来源分流：matched=登记时匹配上的业务
                          # ack；ping_dropped=被 _foreign_mids 认领的非广播
                          # （stats QoS1 ping / 业务直发）；unknown_expired=
                          # 既没等到登记、也没被认领，只能记为未归属。
                          "early_ack_source": {
                              "business_matched": g["ack_early_matched"],
                              "ping_dropped": g["ack_foreign"],
                              "unknown_expired": g["ack_before_reg"]},
                          # 现场口径自证：_early_acks 的 key 是否带 host。
                          "early_acks_key": "(host, mid)",
                          "early_ttl_s": self._EARLY_TTL,
                          # ack_before_reg 里已过期（TTL/容量淘汰）的 ack 按
                          # host 分布，以及最近几条 {host, mid, age_ms,
                          # source, reason}。source 恒为 unknown，因为被认领的
                          # 已走 ack_foreign，不会留在这里。
                          "ack_before_reg_hosts": early_expired_hosts,
                          "ack_before_reg_samples": early_expired_samples},
            "note": ("纯被动统计：0 额外网络请求，时间全部基于本地单调钟，"
                     "不解析也不信赖远端时间戳。下行首包=第一份副本先到的 "
                     "broker；上行首包=on_publish 先完成的 broker，QoS0 下只"
                     "代表包已发离客户端（不代表 broker 已回执）；"
                     "uplink_publish.first_packet_broker=首个完成批次的首个 "
                     "ack 所在 broker，first_packet_broker_batch/at_s 标明是"
                     "哪一批、装好统计后第几秒定下来的，可自证不是被后续批次"
                     "覆盖。hb_gap_ms=同一 broker 相邻两次不同心跳的本地到达"
                     "间隔，同帧重复投递记入 hb_dups。subscription_count=该 host"
                     " 当前活 topic filter 数（paho 2.1 已不维护 c._subscriptions，"
                     "由 manager 的 subscribed_topics 做初值、再包装 subscribe/"
                     "unsubscribe 增量维护；同 filter 重订阅按 MQTT 语义是替换，"
                     "不累加）：hb_dups>0 且 subscription_count>1 是形态 A（多个"
                     "filter 同时匹配同一帧），hb_dups>0 但 subscription_count==1"
                     " 是形态 B（单订阅仍重复，通常是同 host 的第二条活连接/旧 "
                     "client 残留）；subscription_count>1 且 hb_dups=0 是形态 C"
                     "（filter 不重叠，正常，不算双投）。unclassified=rx − wins"
                     " − dups − hb_frames − hb_dups，是既无 sid/seq、也不是心跳"
                     "的未分类下行帧数；unclassified>0 通常与多会话或新消息类型"
                     "同时出现，字段本身只表示残差，不代表统计错误。stale_mids="
                     "仍留在 _mid 里、但所属批次已不在 _batches 的 mid 数。"
                     "requests.observed=所有带 req_id 的帧，unmatched=匹配不到"
                     "本机 pending_requests（共用 reply topic 时多为其他设备的"
                     "回包），后者不计入首包分布与延迟，unmatched_topics/"
                     "unmatched_samples 用来确认它来自哪个 topic、哪个 broker"
                     "（滚动保留最近 unmatched_sample_max 条，默认 5，可在 RPC 里"
                     "用 first_packet_stats.set_unmatched_sample_max(n) 调 0..256；"
                     "每条带 at_s=记录时的 uptime 秒，便于和外部请求时间对齐；"
                     "只留键名，不保存内容）。in_flight 健康时恒有 "
                     "mids==pending_sum 且 stale_mids==orphan_writes==0："
                     "stale_mids>0 表示有 mid 指向已回收批次，orphan_writes>0 "
                     "表示写入时批次就已不存在。mid_ttl_dropped=等不到 "
                     "on_publish、超过 120s 被 _prune 回收的 mid 次数；"
                     "broker_distribution 的 queued 与 send_ms.n 之差应等于它"
                     "（QoS0 的 on_publish 只在包写出 socket 时触发；连接在写出"
                     "前失效、或旧 client 被重建而未写完，就永远等不到回执，只能"
                     "在 TTL 到期时静默回收，这里把这条路径显式计数）。"
                     "ack_without_mid 恒等于 ack_before_reg + ack_after_drop："
                     "早到 ack 先暂存进 _early_acks，登记时匹配成功记 "
                     "ack_early_matched（正常计入 send_ms/首包，不进 without_mid）；"
                     "ack_before_reg=早到但超过 _EARLY_TTL 仍没等到登记（正常"
                     "应≈0；非 0 说明有 ack 不属于任何 broadcast publish，或登记"
                     "路径没走到）。early_ack_source 按来源分流同一批早到 ack："
                     "business_matched=登记时匹配上的业务 ack（正常路径），"
                     "ping_dropped=被 _foreign_mids 认领的非广播 ack（stats QoS1 "
                     "ping 等，不进业务 in-flight），unknown_expired=既没等到登记"
                     "也没被认领（即 ack_before_reg，需要继续找来源）。"
                     "early_acks_key=\"(host, mid)\" 自证早到 ack 的索引带 host，"
                     "不同 broker 的相同 mid 不会互相覆盖；early_ttl_s 是 "
                     "_EARLY_TTL 的当前实际值。ack_before_reg_hosts 给出过期 ack "
                     "按 host 的分布，ack_before_reg_samples 给出最近几条 "
                     "{host, mid, age_ms, source, reason}（reason=ttl 或 evicted），"
                     "source 恒为 unknown（被认领的已计入 ack_foreign）。"
                     "ack_foreign=非 broadcast publish（stats 的 "
                     "QoS1 ping、业务直发等）的 on_publish 次数：按 (host, mid) 打"
                     "标后直接丢弃，不计入 ack_without_mid；若 ack_before_reg 仍"
                     "增长，说明还有未识别的直发来源。"
                     "ack_after_drop=_mid 已被 TTL 回收之后才到的迟到 ack。"
                     "ack_early_pending=当前暂存、还没等到登记的早到 ack 数。"
                     "注意：ack_without_mid 与 mid_ttl_dropped 同量级只能证明"
                     "「时间错位存在」（早到或迟到至少一类大量出现），不能证明"
                     "方向；方向要看匹配后 mid_ttl_dropped 是否下降。若早到匹配"
                     "生效，mid_ttl_dropped 应显著下降、send_ms.n 相应上升，且 "
                     "queued == send_ms.n + mid_ttl_dropped + mids 仍成立。"
                     "uplink_publish.coverage 用来"
                     "定位“某个 broker 上行全缺席”：predicted=广播开始前预测该"
                     "发的 host 次数，attempted=predicted∪seen（因此恒有 "
                     "seen<=attempted 且 attempted==seen+blind），seen=实际进了"
                     "publish 包装器的次数，blind_hosts=预测该发却没抓到（client"
                     "被重建/未包装/扇出前掉线），late_hosts=预测时没连上但真正"
                     "扇出时已连上（旧口径下正是 seen>attempted 的来源），"
                     "offline_hosts=在 clients 里但当时 is_connected() 为假，"
                     "absent_hosts=配置里有但当时不在 net.clients。"
                     "offline_samples（最近 10 条滚动样本）给出每次判离线时的 "
                     "at_s/state/socket/rx_age_s：rx_age_s 很小（该 host 刚收过"
                     "下行）却 is_connected()=False，说明是 paho 状态判断或 client"
                     " 对象错位（僵尸连接），不是真掉线；socket=False 才是真断。"
                     "offline_latest 按 host 只留最近一次离线现场（数量受 host 数"
                     "上限约束），因此稳态的离线事件不会像时间轴样本那样被挤掉。"
                     "at_s 的口径是「最近一次在 broadcast 检查时被判离线的时刻」，"
                     "不是「进入离线状态的时刻」（统计不追踪状态跃迁），所以若一个"
                     "host 之后又恢复，at_s 只会停在最后一次判离线的那一批，不会"
                     "指示它何时恢复。要判断是否恢复，配合同一份报告里的 "
                     "online_latest（最近一次检查时 is_connected() 为真的 at_s）："
                     "online_latest > offline_latest.at_s 表示离线后已恢复；两者都"
                     "停在启动期，说明所有 broadcast 都发生在启动初期。"
                     "注意 online_latest 也只在 broadcast 时采样：若广播全部发生在"
                     "启动初期而之后没有新广播，它不会更新，此时判断某 host 是否"
                     "已上线要看 pty_frames.hb_frames 与 uptime/5 的差值（每少 1 帧"
                     "≈ 晚 5 秒上线），而不是看 online_latest。"
                     "recovered_since_offline=离线后又被检查到在线的 host 名单"
                     "（online_latest > offline_latest.at_s）：只包含被广播检查到"
                     "恢复的 host；若广播停止后 host 才恢复（如 dashboard/hivemq），"
                     "它不会出现在此列表，需看 hb_frames 与 uptime/5 的差值判断"
                     "实际上线时间（心跳是全局唯一源：不论 sessions 多少都约每"
                     " 5s 一帧，hb_frames ≈ uptime/5 与 sessions 无关，多会话"
                     "不会让帧数翻倍；明显少于 uptime/5 才是丢帧或晚上线）。"
                     "recovery_same_tick=online_latest 与 offline_latest.at_s 相等的"
                     "host：at_s 只精确到 0.1s，相等表示同一粒度内既检查到离线也"
                     "检查到在线，先后不可判，故单列而不并入 recovered_since_offline"
                     "（保持严格 > 的保守口径）。"
                     "process.pid 是创建统计对象时的进程，current_pid 是当前提供"
                     "报告的进程，forked=True 说明统计状态被 fork 继承（计数器跨"
                     "进程延续，hb_frames 与 uptime 会失去可比性）；object_age_s 是"
                     "对象年龄（reset 不清零），与 uptime_s 不等说明做过 reset。"
                     "subscriptions.topics 是本进程登记在 subscribed_topics 的 filter"
                     "集合：某 broker 能收回包却疑似收不到 pty 时，先核对 pty topic"
                     "是否在此集合、再看该 host 的 subscription_count 与 rx。"
                     "subscription_count 常态为 2，是 pty out 与 device response 两条"
                     "不同 filter；只有 hb_dups>0 时才是同一帧被多条匹配投递。"
                     "某 host wins=0 但 rx 很大、dups≈rx-hb_frames，说明它收到了帧、"
                     "只是每次都比别的 broker 晚，不是没订阅上。"
                     "broadcast_idle_s=最近一次 broadcast 距今秒数（None=还没广播），"
                     "它很大时 offline/online_latest 停止更新属正常，不能读成"
                     "“现在仍离线”。"
                     "uplink_publish.server_ack 用回包里的 server_from（服务端"
                     "首个接到该请求的 broker）做端到端上行确认：observed=带该"
                     "字段的匹配回包数（同一 req_id 只计一次），broker_distribution"
                     "按 server_from 统计；vs_send_first 与同一 req_id 的发送侧首个"
                     "on_publish broker 对照（agree/disagree，disagree_hosts 按 "
                     "server_from 计）。旧版服务端不写 server_from 时整块为 0，"
                     "不影响其它统计；client_from 与本统计的下行 broker 同源，"
                     "不重复统计。QoS0 下判断上行首达应以 server_ack."
                     "first_packet_broker 为准（服务端实际首个收到请求的 broker），"
                     "uplink_publish.first_packet_broker 仅代表本地写出顺序；"
                     "两者系统性不一致时以前者为准，偏差程度看 vs_send_first "
                     "的 agree/disagree 与 disagree_hosts。"
                     "注意 offline_hosts 与 pty_frames.hb_frames 可以看似矛盾："
                     "offline 只在 broadcast 那一刻采样，先看 offline_samples"
                     "的 at_s 是否集中在启动初期（那时确实还没连上），之后连上"
                     "并收满心跳并不矛盾；只有 at_s 与收心跳时段重叠，才说明是"
                     "状态判断或 client 对象错位。brokers_queued 是窗口内每批"
                     "实际入队数，connectivity.connected 是查询瞬间值，不同源。"
                     "requests.observed 依赖 multi_mqtt 在 message_callback 之前"
                     "按 req_id 的全局去重（dedup_ttl=max(30, timestamp_window+30)，"
                     "默认 timestamp_window=400 → 430s），同一 req_id 在该窗口内"
                     "只计一次；超过窗口的重复投递会再计一次。统计器自己不维护"
                     "req_id 缓存，observed 直接来自回调，所以口径就是去重后的结果。"),
        }

    def _connectivity(self):
        """配置数/客户端对象数/在线数/掉线 host：只读，异常全部吞掉。"""
        try:
            net = self._net
            if net is None:
                return {"configured": None, "clients": None,
                        "connected": None, "disconnected_hosts": []}
            configured = len(getattr(net, "brokers", None)
                             or getattr(net, "broker_list", None) or ())
            clients = getattr(net, "clients", None) or {}
            # 重连会增删 clients，直接迭代 dict 可能 RuntimeError（会被外层
            # 吞成全 None）。先取快照，保证报告永远有可用数字。
            try:
                items = list(clients.items())
            except Exception:
                items = []
            alive, dead = [], []
            for host, c in items:
                try:
                    (alive if c.is_connected() else dead).append(host)
                except Exception:
                    dead.append(host)
            return {"configured": configured, "clients": len(clients),
                    "connected": len(alive), "disconnected_hosts": dead}
        except Exception:
            return {"configured": None, "clients": None,
                    "connected": None, "disconnected_hosts": []}

    @staticmethod
    def _first_frame_broker(sids, brokers):
        """本进程首个 PTY 帧的 broker（多会话时取样本最多的首帧 broker）。"""
        if not sids:
            return None
        cnt = {}
        for host in sids.values():
            cnt[host] = cnt.get(host, 0) + 1
        return max(cnt.items(), key=lambda kv: kv[1])[0]

    @staticmethod
    def _lat_block(n, mn, total, mx):
        if not n:
            return {"n": 0, "min": None, "avg": None, "max": None}
        return {"n": n, "min": round(mn, 1),
                "avg": round(total / n, 1), "max": round(mx, 1)}

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
        self._brokers.clear()
        for k in self._g:
            self._g[k] = 0 if isinstance(self._g[k], int) else None
        self._g["req_lat_sum"] = 0.0
        self._g["up_pub_sum"] = 0
        self._g["up_send_sum"] = 0.0


# 模块级单例：与 parts 一起 exec 进扁平命名空间，因此 1188 的 /r= 可直接
# 访问 first_packet_stats.get_report()；create 在 import 期无副作用。
first_packet_stats = globals().setdefault("first_packet_stats",
                                          FirstPacketStats())
