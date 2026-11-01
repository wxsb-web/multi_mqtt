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
        self._offline_samples = []   # 最多 10 条 offline 现场，定位“假离线”
        self._last_rx = {}        # host -> 最近一次下行到达的 monotonic 秒
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
        try:
            with self._lock:
                last_rx = dict(self._last_rx)
        except Exception:
            last_rx = {}
        now_mono = time.monotonic()
        for host, c in items:
            present.add(host)
            try:
                connected = c.is_connected()
            except Exception:
                connected = False
            if connected:
                attemptable.add(host)
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
                "at_s": round(now_mono - self._t0, 1),
                "state": state,
                "socket": socket_alive,
                "rx_age_s": (None if last is None
                             else round(now_mono - last, 1)),
            })
        cfg = self._configured_hosts()
        absent = cfg - present if cfg else set()
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
                                      "offline": offline_info}
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
                self._offline_samples.append(dict(info))
            if len(self._offline_samples) > 10:
                del self._offline_samples[:len(self._offline_samples) - 10]
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
                return
            st["up_queued"] += 1
            if mid is None:
                return
            key = (host, mid)
            # 同一个 (host, mid) 可能因客户端重建而复用：先扣回旧记录的
            # pending，否则旧批次会永远等一个不会来的 ack。
            self._drop_mid(key)
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
                    if self._first_ul_broker is None:
                        self._first_ul_broker = host
                        self._first_ul_broker_at = (
                            time.monotonic() - self._t0)
                        self._first_ul_bid = bid
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

        只保留按 topic 的计数（最多 8 个）和最多 5 条样本；样本只含 topic、
        broker、req_id 与键名，绝不保存 code 等业务内容，避免统计变成数据副本。
        """
        try:
            t = str(topic)
            self._unmatched_topics[t] = self._unmatched_topics.get(t, 0) + 1
            if len(self._unmatched_topics) > 8:
                drop = min(self._unmatched_topics,
                           key=lambda k: self._unmatched_topics[k])
                self._unmatched_topics.pop(drop, None)
            if len(self._unmatched_samples) < 5:
                rid = data.get("req_id")
                self._unmatched_samples.append({
                    "topic": t,
                    "broker": broker,
                    "req_id": rid if isinstance(rid, str) else repr(rid),
                    "keys": sorted(str(k) for k in data.keys()),
                })
        except Exception:
            pass

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
            blind = dict(self._blind)
            absent = dict(self._absent)
            offline = dict(self._offline)
            late = dict(self._late)
            offline_samples = [dict(s) for s in self._offline_samples]
            sids = dict(self._sids)
            pending_batches = len(self._batches)
            pending_mids = len(self._mid)
            pending_sum = sum(b["pending"] for b in self._batches.values())
            stale_mids = sum(1 for bid, _t0 in self._mid.values()
                             if bid not in self._batches)
            uptime = time.monotonic() - self._t0
            if reset:
                self._reset_locked()
        connectivity = self._connectivity()
        total_req = sum(b["req_wins"] for b in brokers.values())
        total_dl = sum(b["dl_wins"] for b in brokers.values())
        total_ul = sum(b["up_wins"] for b in brokers.values())

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
            "connectivity": connectivity,
            "requests": {
                "n": g["req_n"],
                "observed": g["req_obs"],
                "unmatched": g["req_foreign"],
                "unmatched_topics": unmatched_topics,
                "unmatched_samples": unmatched_samples,
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
                },
                "broker_distribution": ul_rows,
            },
            "in_flight": {"batches": pending_batches, "mids": pending_mids,
                          "pending_sum": pending_sum,
                          "stale_mids": stale_mids,
                          "orphan_writes": g["mid_orphan_write"]},
            "note": ("纯被动统计：0 额外网络请求，时间全部基于本地单调钟，"
                     "不解析也不信赖远端时间戳。下行首包=第一份副本先到的 "
                     "broker；上行首包=on_publish 先完成的 broker，QoS0 下只"
                     "代表包已发离客户端（不代表 broker 已回执）；"
                     "uplink_publish.first_packet_broker=首个完成批次的首个 "
                     "ack 所在 broker，first_packet_broker_batch/at_s 标明是"
                     "哪一批、装好统计后第几秒定下来的，可自证不是被后续批次"
                     "覆盖。hb_gap_ms=同一 broker 相邻两次不同心跳的本地到达"
                     "间隔，同帧重复投递记入 hb_dups。stale_mids="
                     "仍留在 _mid 里、但所属批次已不在 _batches 的 mid 数。"
                     "requests.observed=所有带 req_id 的帧，unmatched=匹配不到"
                     "本机 pending_requests（共用 reply topic 时多为其他设备的"
                     "回包），后者不计入首包分布与延迟，unmatched_topics/"
                     "unmatched_samples 用来确认它来自哪个 topic、哪个 broker"
                     "（样本只留键名，不保存内容）。in_flight 健康时恒有 "
                     "mids==pending_sum 且 stale_mids==orphan_writes==0："
                     "stale_mids>0 表示有 mid 指向已回收批次，orphan_writes>0 "
                     "表示写入时批次就已不存在。uplink_publish.coverage 用来"
                     "定位“某个 broker 上行全缺席”：predicted=广播开始前预测该"
                     "发的 host 次数，attempted=predicted∪seen（因此恒有 "
                     "seen<=attempted 且 attempted==seen+blind），seen=实际进了"
                     "publish 包装器的次数，blind_hosts=预测该发却没抓到（client"
                     "被重建/未包装/扇出前掉线），late_hosts=预测时没连上但真正"
                     "扇出时已连上（旧口径下正是 seen>attempted 的来源），"
                     "offline_hosts=在 clients 里但当时 is_connected() 为假，"
                     "absent_hosts=配置里有但当时不在 net.clients。"
                     "offline_samples（最多 10 条滚动样本）给出每次判离线时的 "
                     "at_s/state/socket/rx_age_s：rx_age_s 很小（该 host 刚收过"
                     "下行）却 is_connected()=False，说明是 paho 状态判断或 client"
                     " 对象错位（僵尸连接），不是真掉线；socket=False 才是真断。"
                     "注意 offline_hosts 与 pty_frames.hb_frames 可以看似矛盾："
                     "heartbeat 短周期只落在断线窗口的间隙里就能收满，而广播恰好"
                     "撞在窗口内。brokers_queued 是窗口内每批实际入队数，"
                     "connectivity.connected 是查询瞬间值，两者不同源。"
                     "requests.observed 依赖 multi_mqtt 在 message_callback 之前"
                     "按 req_id 的全局去重（TTL≈60s），同一 req_id 在 TTL 内"
                     "只计一次；超过 TTL 的重复投递会再计一次。"),
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

    def _reset_locked(self):
        self._t0 = time.monotonic()
        self._batches.clear()
        self._mid.clear()
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
        self._last_rx.clear()
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
