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
    _SEEN_MAX = 16384             # 下行 seq 去重表上限
    _SID_MAX = 512                # 会话首帧 broker 表上限

    def __init__(self):
        self._lock = threading.Lock()
        self._t0 = time.monotonic()
        self._installed = False
        self._net = None
        self._node = None
        self._tls = threading.local()
        self._batches = {}        # bid -> {"t0","pending","win","published"}
        self._mid = {}            # (host, mid) -> (bid, publish_start)
        self._batch_seq = 0
        self._dl_seen = {}        # (sid, seq) -> 首达 broker
        self._sids = {}           # sid -> 首帧 broker（仅展示用）
        self._first_req_broker = None
        self._first_dl_broker = None
        self._first_ul_broker = None
        self._brokers = {}        # host -> 计数器 dict
        self._g = {
            "req_n": 0,
            "req_lat_n": 0, "req_lat_min": None,
            "req_lat_sum": 0.0, "req_lat_max": None,
            "dl_unique": 0, "dl_dups": 0,
            "up_calls": 0,
            "up_pub_n": 0, "up_pub_min": None,
            "up_pub_sum": 0, "up_pub_max": None,
            "up_send_n": 0, "up_send_min": None,
            "up_send_sum": 0.0, "up_send_max": None,
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
        """给每个 paho client 挂 publish/on_publish 观察器；新旧连接都覆盖。"""
        net = self._net
        if net is None:
            return
        try:
            with net.lock:
                items = list(net.clients.items())
        except Exception:
            return
        for host, c in items:
            self._wrap_client(host, c)

    def _wrap_client(self, host, c):
        try:
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
            c._fp_wrapped = True
        except Exception:
            pass

    # ---------------------------- 上行 ----------------------------

    def _publish_broadcast(self, orig, topic, payload_dict, args, kw):
        """包住一次广播：建批次 → 记每个 broker 的 publish/on_publish。"""
        try:
            self._wrap_clients()   # 重连会换新 client，顺带补齐
        except Exception:
            pass
        bid = None
        try:
            with self._lock:
                self._batch_seq += 1
                bid = self._batch_seq
                self._batches[bid] = {"t0": time.perf_counter(),
                                      "pending": 0, "win": None,
                                      "published": None}
                self._g["up_calls"] += 1
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
            if isinstance(published, int):
                g = self._g
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
            if not ok:
                st["up_failed"] += 1
                return
            st["up_queued"] += 1
            if mid is None:
                return
            b = self._batches.get(bid)
            if b is not None:
                b["pending"] += 1
            if len(self._mid) >= self._MID_MAX:
                for k in list(self._mid)[:self._MID_MAX // 2]:
                    self._mid.pop(k, None)
            self._mid[(host, mid)] = (bid, t0)

    def _note_publish_ack(self, host, mid):
        """paho on_publish：包已发离该 broker 连接，算发送耗时/首达竞速。"""
        now = time.perf_counter()
        with self._lock:
            rec = self._mid.pop((host, mid), None)
            if rec is None:
                return
            bid, t0 = rec
            lat = (now - t0) * 1000.0
            st = self._broker(host)
            self._lat_add(st, "up_send", lat)
            self._lat_add(self._g, "up_send", lat)
            b = self._batches.get(bid)
            if b is not None:
                b["pending"] -= 1
                if b["win"] is None:
                    b["win"] = host
                    st["up_wins"] += 1
                    self._first_ul_broker = host
                if b["pending"] <= 0:
                    self._batches.pop(bid, None)

    # ---------------------------- 下行 ----------------------------

    def _note_downlink(self, topic, data, broker):
        if not isinstance(data, dict) or not broker:
            return
        req_id = data.get("req_id")
        if req_id:
            base = req_id.split("|", 1)[0] if isinstance(req_id, str) else req_id
            lat = None
            try:
                with self._node.lock:
                    ctx = self._node.pending_requests.get(base)
                if ctx is not None:
                    lat = (time.perf_counter()
                           - ctx.get("start_time", time.perf_counter())) * 1000.0
            except Exception:
                ctx = None
            with self._lock:
                g = self._g
                g["req_n"] += 1
                st = self._broker(broker)
                st["req_wins"] += 1
                if self._first_req_broker is None:
                    self._first_req_broker = broker
                if lat is not None and lat >= 0:
                    self._lat_add(st, "req_lat", lat)
                    self._lat_add(g, "req_lat", lat)
            return

        sid = data.get("pty") or data.get("s5") or data.get("stream")
        sid = str(sid) if sid is not None else None
        seq = data.get("seq")
        key = (sid, seq) if (sid is not None and seq is not None) else None
        with self._lock:
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
            # 心跳帧只用本地单调钟算「相邻两次到达的间隔」：不解析、也不
            # 信赖远端 hb 里的时间戳，因此统计完全与远端时钟/状态无关。
            now = time.monotonic()
            with self._lock:
                st = self._broker(broker)
                st["hb_cnt"] += 1
                last = st["hb_last"]
                st["hb_last"] = now
                if last is not None:
                    self._lat_add(st, "hb", (now - last) * 1000.0)

    # ---------------------------- 报告 ----------------------------

    def get_report(self, reset: bool = False) -> dict:
        """返回 JSON 可序列化的统计报告；reset=True 读完清零。"""
        with self._lock:
            brokers = {k: dict(v) for k, v in self._brokers.items()}
            g = dict(self._g)
            first_ul = self._first_ul_broker
            sids = dict(self._sids)
            pending_batches = len(self._batches)
            pending_mids = len(self._mid)
            uptime = time.monotonic() - self._t0
            if reset:
                self._reset_locked()
        total_req = sum(b["req_wins"] for b in brokers.values())
        total_dl = sum(b["dl_wins"] for b in brokers.values())
        total_ul = sum(b["up_wins"] for b in brokers.values())

        def pct(v, total):
            return round(100.0 * v / total, 1) if total else 0.0

        req_rows = []
        for host, st in sorted(brokers.items(),
                               key=lambda kv: -kv[1]["req_wins"]):
            if not st["req_wins"] and not st["req_lat_n"]:
                continue
            req_rows.append({
                "broker": host,
                "wins": st["req_wins"],
                "win_pct": pct(st["req_wins"], total_req),
                "latency_ms": self._lat_block(
                    st["req_lat_n"], st["req_lat_min"],
                    st["req_lat_sum"], st["req_lat_max"]),
            })

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

        return {
            "uptime_s": round(uptime, 1),
            "installed": self._installed,
            "requests": {
                "n": g["req_n"],
                "latency_ms": self._lat_block(
                    g["req_lat_n"], g["req_lat_min"],
                    g["req_lat_sum"], g["req_lat_max"]),
                "first_packet_broker": self._first_req_broker,
                "broker_distribution": req_rows,
            },
            "pty_frames": {
                "unique": g["dl_unique"],
                "duplicates": g["dl_dups"],
                "first_frame_broker": self._first_frame_broker(sids, brokers),
                "broker_distribution": dl_rows,
            },
            "uplink_publish": {
                "calls": g["up_calls"],
                "brokers_queued": self._lat_block(
                    g["up_pub_n"], g["up_pub_min"],
                    g["up_pub_sum"], g["up_pub_max"]),
                "first_packet_broker": first_ul,
                "send_ms": self._lat_block(
                    g["up_send_n"], g["up_send_min"],
                    g["up_send_sum"], g["up_send_max"]),
                "broker_distribution": ul_rows,
            },
            "in_flight": {"batches": pending_batches, "mids": pending_mids},
            "note": ("纯被动统计：0 额外网络请求，时间全部基于本地单调钟，"
                     "不读取也不信赖远端服务器的时间戳/状态。下行首包=第一"
                     "份副本先到的 broker；上行首包=on_publish 先完成的 "
                     "broker，QoS0 下只代表包已发离客户端（不代表 broker 已"
                     "回执）。hb_gap_ms=同一 broker 相邻两次心跳帧的本地到达"
                     "间隔（不含远端时钟）。"),
        }

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
                "hb_n": 0, "hb_min": None, "hb_sum": 0.0, "hb_max": None,
                "req_wins": 0, "req_lat_n": 0, "req_lat_min": None,
                "req_lat_sum": 0.0, "req_lat_max": None,
                "up_queued": 0, "up_failed": 0,
                "up_send_n": 0, "up_send_min": None,
                "up_send_sum": 0.0, "up_send_max": None,
                "up_wins": 0,
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
        """只在持锁时调用：给批次表兜底，防止 on_publish 永不回来。"""
        if len(self._batches) > self._BATCH_MAX:
            for k in list(self._batches)[:self._BATCH_MAX // 2]:
                self._batches.pop(k, None)

    def _reset_locked(self):
        self._t0 = time.monotonic()
        self._batches.clear()
        self._mid.clear()
        self._dl_seen.clear()
        self._sids.clear()
        self._first_req_broker = None
        self._first_dl_broker = None
        self._first_ul_broker = None
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
