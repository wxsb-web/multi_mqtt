#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""rpc_trace.py —— 单次 MQTT RPC 的全链路分步耗时观测（独立脚本）。

它不修改 multi_mqtt/server_mqtt 主流程，只在当前进程里旁路包住：
    request -> publish_broadcast -> 每个 paho client.publish -> on_publish
    -> paho on_message -> message_callback -> request 返回

同时做三类“尺子”：
    1) 本地高精度时间戳（perf_counter），测客户端内部每一步；
    2) 网络路径尺子：ICMP ping / TCP connect RTT / tracert 每一跳；
    3) 跨机时钟校准：连发几条极短 RPC，用最小 RTT 估 server-client 时钟差，
       把响应里的 server_time 锚点换算到客户端时间轴，从而切开上行/回程。

server 所在机器的反向路径（broker -> server 每一跳、server 的 Wi-Fi 最后一段）
通过一次可控的远端诊断代码实测；它只跑 ping / TCP connect / 可选 tracert，
不改服务端状态。默认轻量（不跑远端 tracert），--server-tracert 才逐跳。

用法示例：
    python tools/rpc_trace.py "3" -t x230 --to 10 --key "2**222" --mb 6
    python tools/rpc_trace.py "3" -t lxh --mb 6 --server-tracert --html trace.html
    python tools/rpc_trace.py --demo            # 不联网，预览渲染效果

口径说明（重要）：
    - 客户端时间戳是实测；server_time 是服务端“执行结束、回包发布前”的时刻；
    - 服务器与客户端时钟差由最小 RTT 样本估计，存在半 RTT 量级的不确定度；
    - broker 处理耗时 = QoS1 PUBACK RTT - TCP 连接 RTT（同一 host），是估计值；
    - 单向网络耗时统一按 RTT/2 折算（对称路径假设），报告里标注为“估”；
    - tracert 的每一跳 RTT 是 ICMP 往返值，相邻跳差值才近似“该跳引入的延迟”；
      中间路由器可能不响应 ICMP（显示 *），这不代表丢包。
"""

import argparse
import concurrent.futures
import html
import json
import math
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import traceback
import unicodedata

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# 保持输出干净：让 multi_mqtt 只挂 NullHandler，连接告警我们自己汇总打印。
os.environ.setdefault("CMQ_NO_STDERR_LOG", "1")

_MS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:ms|毫秒)", re.I)
_IP_RE = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3})\s*$")


# --------------------------------------------------------------------------
# 小工具
# --------------------------------------------------------------------------
def _now():
    return time.perf_counter(), time.time()


def _base_rid(rid):
    if rid is None:
        return None
    text = str(rid)
    return text.rsplit("|", 1)[0] if "|" in text else text


def _fmt_ms(value, digits=3):
    if value is None:
        return "-"
    try:
        return ("%." + str(digits) + "f") % float(value)
    except (TypeError, ValueError):
        return str(value)


def _round(value, digits=3):
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _clamp(value, low=0.0, high=None):
    if value is None:
        return None
    value = max(low, float(value))
    if high is not None:
        value = min(high, value)
    return value


def _dwidth(text):
    """终端显示宽度：CJK 全角字符占 2 列，用它才能把中文列对齐。"""
    width = 0
    for char in str(text):
        if unicodedata.combining(char):
            continue
        width += 2 if unicodedata.east_asian_width(char) in ("W", "F") else 1
    return width


def _ljust(text, width):
    text = str(text)
    return text + " " * max(0, width - _dwidth(text))


def _rjust(text, width):
    text = str(text)
    return " " * max(0, width - _dwidth(text)) + text


def _decode(raw):
    if not raw:
        return ""
    for enc in ("utf-8", locale_encoding(), "gbk"):
        if not enc:
            continue
        try:
            return raw.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return raw.decode("utf-8", "replace")


def locale_encoding():
    try:
        import locale
        return locale.getpreferredencoding(False)
    except Exception:
        return "utf-8"


def _run(cmd, timeout_s):
    """执行本机命令，返回 (text, error)。任何异常都转成文本，不抛出。"""
    try:
        proc = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            timeout=timeout_s,
        )
        return _decode(proc.stdout or b""), None
    except Exception as exc:
        return "", repr(exc)


def _latencies_on_reply_lines(text):
    values = []
    for line in (text or "").splitlines():
        # 只认真正的回包行（都带 TTL）；否则会把 ping 汇总行的
        # “time 2003ms / 时间=2003ms” 当成一次 2 秒的 RTT。
        if "ttl" not in line.lower():
            continue
        values.extend(_MS_RE.findall(line))
    return [float(v) for v in values]


def _local_ip():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.connect(("8.8.8.8", 80))
            return sock.getsockname()[0]
        finally:
            sock.close()
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return None


def _resolve(host):
    t0 = time.perf_counter()
    try:
        infos = socket.getaddrinfo(host, None)
    except Exception as exc:
        return {"ok": False, "error": repr(exc),
                "ms": _round((time.perf_counter() - t0) * 1000.0)}
    addrs = []
    for item in infos:
        addr = item[4][0]
        if addr not in addrs:
            addrs.append(addr)
    return {"ok": True, "ms": _round((time.perf_counter() - t0) * 1000.0),
            "addresses": addrs}


def icmp_ping(host, count=5, timeout_ms=800):
    """返回逐包 RTT 与 min/avg/max；ICMP 被禁时 samples 为空但不抛异常。"""
    if os.name == "nt":
        cmd = ["ping", "-n", str(int(count)), "-w", str(int(timeout_ms)), host]
    else:
        cmd = ["ping", "-c", str(int(count)), "-W", str(max(1, int(timeout_ms / 1000))), host]
    text, err = _run(cmd, timeout_s=max(5.0, count * (timeout_ms / 1000.0) + 2.0))
    samples = _latencies_on_reply_lines(text)
    return {
        "samples": [_round(v) for v in samples],
        "min": _round(min(samples)) if samples else None,
        "avg": _round(sum(samples) / len(samples)) if samples else None,
        "max": _round(max(samples)) if samples else None,
        "error": err,
        "raw_tail": (text or "")[-300:] if not samples else None,
    }


def tcp_connect_rtt(host, port, count=3, timeout_s=2.0):
    """TCP connect 往返时间（三次握手 ≈ 1 个网络 RTT），是 ping 不通时的兜底尺子。"""
    samples = []
    errors = []
    for _ in range(int(count)):
        sock = None
        t0 = time.perf_counter()
        try:
            sock = socket.create_connection((host, int(port)), timeout=timeout_s)
            samples.append(round((time.perf_counter() - t0) * 1000.0, 3))
        except Exception as exc:
            errors.append(repr(exc))
            break
        finally:
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass
    return {
        "samples": samples,
        "min": min(samples) if samples else None,
        "avg": round(sum(samples) / len(samples), 3) if samples else None,
        "max": max(samples) if samples else None,
        "error": errors[-1] if errors else None,
    }


def traceroute(host, max_hops=16, wait_ms=600, timeout_s=30):
    """逐跳 RTT。Windows 用 tracert，其它平台用 traceroute；缺命令返回 available=False。"""
    if os.name == "nt":
        # 注意：Windows 自带 tracert 没有 -q 参数，别加。
        cmd = ["tracert", "-d", "-4", "-h", str(int(max_hops)),
               "-w", str(int(wait_ms)), host]
    else:
        exe = shutil.which("traceroute")
        if not exe:
            return {"available": False, "reason": "traceroute 命令不存在"}
        cmd = [exe, "-n", "-4", "-m", str(int(max_hops)),
               "-q", "1", "-w", str(max(1, int(wait_ms / 1000.0))), host]
    text, err = _run(cmd, timeout_s=timeout_s)
    hops = []
    prev = None
    for line in (text or "").splitlines():
        match = re.match(r"^\s*(\d+)\s+", line)
        if not match:
            continue
        hop_no = int(match.group(1))
        values = [float(v) for v in _MS_RE.findall(line)]
        addr_match = _IP_RE.search(line)
        addr = addr_match.group(1) if addr_match else None
        hop_min = min(values) if values else None
        delta = None
        if hop_min is not None and prev is not None:
            delta = round(hop_min - prev, 3)
        if hop_min is not None:
            prev = hop_min
        hops.append({
            "hop": hop_no,
            "addr": addr,
            "rtt_ms": [_round(v) for v in values],
            "min": _round(hop_min),
            "delta": delta,
        })
    return {
        "available": True,
        "hops": hops,
        "reached": bool(hops) and hops[-1].get("addr") is not None,
        "error": err,
        "raw_tail": (text or "")[-1200:] if not hops else None,
    }


def normalize_remote_hops(hops):
    """远端 JSON 只带每跳 RTT 列表；补齐 min/delta，口径与本地 traceroute 一致。"""
    prev = None
    for hop in hops or []:
        values = hop.get("rtt_ms") or []
        hop["min"] = _round(min(values)) if values else None
        hop["delta"] = (_round(hop["min"] - prev)
                        if hop["min"] is not None and prev is not None else None)
        if hop["min"] is not None:
            prev = hop["min"]
    return hops


def probe_path(host, port, do_trace=True, max_hops=16, wait_ms=600):
    info = {
        "host": host,
        "port": int(port),
        "dns": _resolve(host),
        "tcp": tcp_connect_rtt(host, port, 3),
        "ping": icmp_ping(host, 4),
    }
    if do_trace:
        info["trace"] = traceroute(host, max_hops=max_hops, wait_ms=wait_ms)
    return info


# --------------------------------------------------------------------------
# 远端（server 侧）诊断代码：只读网络状态 + ping/TCP/可选 tracert
# --------------------------------------------------------------------------
_REMOTE_TEMPLATE = r"""
import json, os, platform, re, socket, subprocess, time
BROKER = __BROKER__
PORT = __PORT__
DO_TRACE = __DO_TRACE__
MAX_HOPS = __MAX_HOPS__

def _run(cmd, timeout=8):
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, timeout=timeout)
        return p.stdout.decode("utf-8", "replace")
    except Exception as e:
        return "ERR:" + repr(e)

def _ms(text):
    return [float(x) for x in re.findall(r"(\d+(?:\.\d+)?)\s*(?:ms|毫秒)", text, re.I)]

def _ping(host):
    if not host:
        return None
    if os.name == "nt":
        out = _run(["ping", "-n", "3", "-w", "700", host], 8)
    else:
        out = _run(["ping", "-c", "3", "-W", "1", host], 8)
    vals = []
    for line in out.splitlines():
        # 只认带回包 TTL 的行，避免把汇总行的 “time 2003ms” 当 RTT。
        if "ttl" not in line.lower():
            continue
        vals.extend(_ms(line))
    if not vals:
        return {"samples": [], "raw_tail": out[-200:]}
    return {"samples": vals, "min": min(vals),
            "avg": round(sum(vals) / len(vals), 3), "max": max(vals)}

res = {"start": time.time(), "platform": platform.platform(),
       "python": platform.python_version()}
try:
    u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    u.connect(("8.8.8.8", 80))
    res["ip"] = u.getsockname()[0]
    u.close()
except Exception as e:
    res["ip"] = None
    res["ip_error"] = repr(e)

gw = None
try:
    if os.path.exists("/proc/net/route"):
        with open("/proc/net/route") as f:
            for line in f:
                p = line.split()
                if len(p) > 2 and p[1] == "00000000":
                    h = p[2]
                    gw = ".".join(str(int(h[i:i+2], 16)) for i in (6, 4, 2, 0))
                    break
except Exception:
    pass
if not gw:
    out = _run(["ip", "route"], 4) if os.name != "nt" else _run(["route", "print", "0.0.0.0"], 4)
    m = re.search(r"default via (\d+\.\d+\.\d+\.\d+)", out) or \
        re.search(r"0\.0\.0\.0\s+0\.0\.0\.0\s+(\d+\.\d+\.\d+\.\d+)", out)
    if m:
        gw = m.group(1)
res["gateway"] = gw
res["ping_gateway"] = _ping(gw)
res["ping_broker"] = _ping(BROKER)

rtts = []
for _ in range(3):
    try:
        t0 = time.perf_counter()
        s = socket.create_connection((BROKER, PORT), timeout=3)
        rtts.append(round((time.perf_counter() - t0) * 1000.0, 3))
        s.close()
    except Exception as e:
        res.setdefault("tcp_errors", []).append(repr(e))
        break
res["tcp_rtt_ms"] = rtts

if DO_TRACE:
    if os.name == "nt":
        tr = _run(["tracert", "-d", "-4", "-h", str(MAX_HOPS),
                   "-w", "500", BROKER], 25)
    else:
        tr = _run(["traceroute", "-n", "-4", "-m", str(MAX_HOPS),
                   "-q", "1", "-w", "1", BROKER], 25)
    hops = []
    for line in tr.splitlines():
        m = re.match(r"^\s*(\d+)\s+", line)
        if not m:
            continue
        hops.append({"hop": int(m.group(1)), "rtt_ms": _ms(line)})
    res["tracert_hops"] = hops
    res["tracert_raw_tail"] = tr[-1500:]

res["end"] = time.time()
print(json.dumps(res, ensure_ascii=False))
"""


def build_remote_probe(host, port, do_trace=False, max_hops=12):
    # 主机名/端口来自 broker 配置，仍做一层白名单，避免拼出奇怪代码。
    safe_host = re.sub(r"[^A-Za-z0-9\.\-_:]", "", str(host or ""))
    safe_port = int(port or 1883)
    return (_REMOTE_TEMPLATE
            .replace("__BROKER__", repr(safe_host))
            .replace("__PORT__", str(safe_port))
            .replace("__DO_TRACE__", repr(bool(do_trace)))
            .replace("__MAX_HOPS__", str(int(max_hops))))


def parse_last_json(text):
    for line in reversed((text or "").strip().splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            return obj
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


# --------------------------------------------------------------------------
# 旁路记录器：只观察，不改变任何收发语义
# --------------------------------------------------------------------------
class Recorder(object):
    def __init__(self, reply_topic):
        # record_for() -> snapshot() 会嵌套取锁，必须可重入。
        self.lock = threading.RLock()
        self.reply_topic = reply_topic
        self.records = {}
        self.raw_all = []
        self.acks = {}
        self.mid_index = {}
        self.current = None
        self.last_record = None

    def _rec(self, rid):
        rec = self.records.get(rid)
        if rec is None:
            rec = {
                "req_id": rid,
                "entry": None,
                "code": None,
                "broadcast": [],
                "publishes": [],
                "raw": [],
                "dispatch": None,
                "returned": None,
                "response": None,
            }
            self.records[rid] = rec
        return rec

    def begin(self, code):
        t, e = _now()
        with self.lock:
            self.current = {"t": t, "epoch": e, "code": code, "rid": None,
                            "waits": []}
        return t

    def note_wait(self, min_count, online, seconds, poll_ms=None):
        """记录 request() 入口 wait_connected() 的阻塞时长（50ms 轮询会凑整）。"""
        with self.lock:
            if self.current is None:
                return
            self.current.setdefault("waits", []).append({
                "min_count": min_count,
                "online": online,
                "ms": round(float(seconds) * 1000.0, 3),
                "poll_ms": poll_ms,
            })

    def _attach_current(self, rec, rid=None):
        """把当前请求的入口时间挂到 publish/broadcast 首次看到的 req_id 上。"""
        if not self.current:
            return
        if rid and not self.current.get("rid"):
            self.current["rid"] = rid
        if rec["entry"] is None:
            rec["entry"] = dict(self.current)
            rec["code"] = self.current.get("code")

    def end(self, response):
        t, e = _now()
        rid = _base_rid((response or {}).get("req_id"))
        with self.lock:
            if not rid and self.current:
                rid = self.current.get("rid")
            if not rid:
                # 请求在 publish 前就失败时，仍留一条记录，报告才能显示
                # “发出去但没回来”，而不是误用上一条校准记录。
                rid = "__trace_%d" % int(t * 1000000)
            rec = self._rec(rid)
            rec["returned"] = {"t": t, "epoch": e}
            rec["response"] = response
            self._attach_current(rec, rid)
            self.last_record = rec
            self.current = None
        return t

    def note_broadcast(self, rid, topic, t0, e0, t1, e1, published):
        rid = _base_rid(rid)
        if not rid:
            return
        with self.lock:
            rec = self._rec(rid)
            self._attach_current(rec, rid)
            rec["broadcast"].append({
                "topic": topic,
                "t0": t0, "epoch0": e0,
                "t1": t1, "epoch1": e1,
                "published": published,
            })

    def note_publish(self, host, mid, qos, t0, e0, t1, e1, rid, topic):
        rid = _base_rid(rid)
        if not rid:
            return
        with self.lock:
            rec = self._rec(rid)
            self._attach_current(rec, rid)
            entry = {
                "host": host, "mid": mid, "qos": int(qos or 0),
                "t0": t0, "epoch0": e0, "t1": t1, "epoch1": e1,
                "topic": topic, "ack": None,
            }
            ack = self.acks.pop((host, mid), None)
            if ack is not None:
                entry["ack"] = ack
            else:
                self.mid_index[(host, mid)] = (rid, len(rec["publishes"]))
            rec["publishes"].append(entry)

    def note_ack(self, host, mid, t, e):
        with self.lock:
            hit = self.mid_index.pop((host, mid), None)
            if hit is None:
                self.acks[(host, mid)] = (t, e)
                return
            rid, idx = hit
            rec = self.records.get(rid)
            if rec is not None and 0 <= idx < len(rec["publishes"]):
                rec["publishes"][idx]["ack"] = (t, e)

    def note_raw(self, host, topic, payload, t, e):
        rid = None
        try:
            obj = json.loads(bytes(payload).decode("utf-8"))
            if isinstance(obj, dict):
                rid = _base_rid(obj.get("req_id"))
        except Exception:
            rid = None
        item = {
            "host": host, "topic": topic,
            "t": t, "epoch": e, "req_id": rid,
            "bytes": len(payload or b""), "done": None,
        }
        with self.lock:
            self.raw_all.append(item)
            if topic == self.reply_topic and rid:
                self._rec(rid)["raw"].append(item)
        return item

    def note_dispatch(self, topic, data, host, t, e):
        rid = _base_rid(data.get("req_id")) if isinstance(data, dict) else None
        if not rid:
            return
        with self.lock:
            rec = self._rec(rid)
            if rec["dispatch"] is None:
                rec["dispatch"] = {
                    "t": t, "epoch": e, "host": host, "topic": topic,
                }

    def record_for(self, response):
        rid = _base_rid((response or {}).get("req_id"))
        with self.lock:
            if not rid or rid not in self.records:
                rid = self.last_record["req_id"] if self.last_record else None
        return self.snapshot(rid)

    def snapshot(self, rid=None):
        with self.lock:
            if rid is None:
                rec = self.last_record
                if rec is None:
                    return None
                rid = rec["req_id"]
            rec = self.records.get(rid)
            if rec is None:
                return None
            return {
                "req_id": rec["req_id"],
                "entry": dict(rec["entry"]) if rec["entry"] else None,
                "code": rec["code"],
                "broadcast": [dict(x) for x in rec["broadcast"]],
                "publishes": [dict(x) for x in rec["publishes"]],
                "raw": [dict(x) for x in rec["raw"]],
                "dispatch": dict(rec["dispatch"]) if rec["dispatch"] else None,
                "returned": dict(rec["returned"]) if rec["returned"] else None,
                "response": dict(rec["response"]) if rec["response"] else None,
            }

    def probe_acks(self, rid):
        rid = _base_rid(rid)
        with self.lock:
            rec = self.records.get(rid)
            if not rec:
                return {}
            out = {}
            for entry in rec["publishes"]:
                if entry.get("ack"):
                    out[entry["host"]] = {
                        "publish_ms": _round((entry["t1"] - entry["t0"]) * 1000.0),
                        "ack_ms": _round((entry["ack"][0] - entry["t0"]) * 1000.0),
                    }
            return out


def _payload_rid(payload):
    try:
        if isinstance(payload, (bytes, bytearray)):
            payload = bytes(payload).decode("utf-8")
        obj = json.loads(payload)
        if isinstance(obj, dict):
            return _base_rid(obj.get("req_id"))
    except Exception:
        pass
    return None


def install_hooks(node, recorder):
    net = node.mqtt_net

    orig_on_message = net._on_message

    def on_message(client, userdata, msg):
        t, e = _now()
        item = recorder.note_raw(userdata, msg.topic, msg.payload, t, e)
        try:
            return orig_on_message(client, userdata, msg)
        finally:
            item["done"] = time.perf_counter()

    net._on_message = on_message

    orig_on_publish = net._on_publish

    def on_publish(client, userdata, mid, *args, **kwargs):
        t, e = _now()
        recorder.note_ack(userdata, mid, t, e)
        return orig_on_publish(client, userdata, mid, *args, **kwargs)

    net._on_publish = on_publish

    orig_wait = net.wait_connected

    def wait_connected(min_count=1, timeout=10.0, poll_interval=0.05):
        t0, _e0 = _now()
        online = orig_wait(min_count, timeout, poll_interval)
        recorder.note_wait(min_count, online, time.perf_counter() - t0,
                           round(float(poll_interval) * 1000.0))
        return online

    net.wait_connected = wait_connected

    orig_broadcast = net.publish_broadcast

    def publish_broadcast(topic, payload_dict, client_private_key_bytes=None, **kwargs):
        t0, e0 = _now()
        published = orig_broadcast(
            topic, payload_dict,
            client_private_key_bytes=client_private_key_bytes, **kwargs)
        t1, e1 = _now()
        rid = None
        if isinstance(payload_dict, dict):
            rid = _base_rid(payload_dict.get("req_id"))
        recorder.note_broadcast(rid, topic, t0, e0, t1, e1, published)
        return published

    net.publish_broadcast = publish_broadcast

    orig_callback = net.message_callback
    if orig_callback is not None:
        def message_callback(topic, data, host):
            t, e = _now()
            recorder.note_dispatch(topic, data, host, t, e)
            return orig_callback(topic, data, host)

        net.set_on_message(message_callback)


def wrap_client_publishes(net, recorder):
    """逐 paho client 包住 publish，记录“哪个 broker、何时交网络层、何时 ack”。"""
    with net.lock:
        clients = list(net.clients.items())

    for host, client in clients:
        if getattr(client, "_trace_publish_wrapped", False):
            continue
        orig_publish = client.publish

        def make(host=host, orig_publish=orig_publish):
            def publish(topic, payload=None, qos=0, retain=False, *args, **kwargs):
                t0, e0 = _now()
                info = orig_publish(topic, payload, qos, retain, *args, **kwargs)
                t1, e1 = _now()
                rid = _payload_rid(payload)
                recorder.note_publish(
                    host, getattr(info, "mid", None), qos,
                    t0, e0, t1, e1, rid, topic)
                return info
            return publish

        try:
            client.publish = make()
            client._trace_publish_wrapped = True
        except Exception:
            pass


# --------------------------------------------------------------------------
# 请求与校准
# --------------------------------------------------------------------------
def run_request(node, recorder, client_mqtt, code, topic, timeout,
                key, allow_no_pub, min_brokers):
    recorder.begin(code)
    response = {}
    try:
        response = node.request(
            code,
            request_topic=topic,
            timeout=timeout,
            client_private_key_bytes=key,
            allow_no_server_pubkey_response=bool(allow_no_pub),
            reply_topic=client_mqtt.REPLY_TOPIC,
            min_broker_count=int(min_brokers),
        )
    finally:
        recorder.end(response)
    return response or {}


def _pick_raw(rec):
    if not rec:
        return None
    raws = rec.get("raw") or []
    if not raws:
        return None
    ordered = sorted(raws, key=lambda x: x["t"])
    for item in ordered:
        if item.get("done") is not None:
            return item
    return ordered[0]


def calibrate_clock(node, recorder, client_mqtt, topic, args, rounds):
    samples = []
    for index in range(max(0, int(rounds))):
        response = run_request(
            node, recorder, client_mqtt, "pass", topic,
            min(float(args.timeout), 10.0), args.key, args.allow_no_pub, 0)
        rec = recorder.record_for(response)
        raw = _pick_raw(rec)
        bcasts = (rec or {}).get("broadcast") or []
        if not raw or not bcasts:
            continue
        server_time = response.get("server_time")
        if not isinstance(server_time, (int, float)):
            continue
        t0 = bcasts[0]["t0"]
        rtt_ms = (raw["t"] - t0) * 1000.0
        offset_ms = (float(server_time) / 1000.0
                     - (bcasts[0]["epoch0"] + raw["epoch"]) / 2.0) * 1000.0
        samples.append({
            "round": index,
            "rtt_ms": _round(rtt_ms),
            "offset_ms": _round(offset_ms),
            "req_id": response.get("req_id"),
            "server_from": response.get("server_from"),
            "client_from": response.get("client_from"),
        })
    if not samples:
        return {"samples": [], "offset_ms": None, "uncertainty_ms": None}
    best = min(samples, key=lambda item: item["rtt_ms"] or 1e9)
    offsets = [item["offset_ms"] for item in samples if item["offset_ms"] is not None]
    spread = (max(offsets) - min(offsets)) / 2.0 if offsets else None
    uncertainty = max(spread or 0.0, (best["rtt_ms"] or 0.0) / 2.0)
    return {
        "samples": samples,
        "offset_ms": best["offset_ms"],
        "uncertainty_ms": _round(uncertainty),
        "best": best,
    }


def broker_ack_probes(node, recorder, rounds=2, wait_s=1.5):
    """一次 QoS1 广播，逐 broker 收 PUBACK；返回 host -> 最小 ack RTT。"""
    if rounds <= 0:
        return {}
    result = {}
    for index in range(int(rounds)):
        probe_id = "trace_probe_%d_%d" % (int(time.time() * 1000), index)
        payload = {"req_id": probe_id, "trace_probe": index}
        try:
            node.mqtt_net.publish_broadcast(
                "multi_mqtt/ping_rtt/trace_probe", payload, qos=1)
        except Exception:
            continue
        deadline = time.time() + max(0.2, float(wait_s))
        while time.time() < deadline:
            acked = recorder.probe_acks(probe_id)
            if acked:
                for host, item in acked.items():
                    current = result.get(host)
                    if current is None or (item.get("ack_ms") is not None
                                           and item["ack_ms"] < current):
                        result[host] = item.get("ack_ms")
            time.sleep(0.01)
    return result


def snapshot_hosts(net):
    """只读快照：(在线 host 列表, 离线 host 列表)。"""
    try:
        with net.lock:
            clients = dict(net.clients)
    except Exception:
        return [], []
    online, offline = [], []
    for host, client in clients.items():
        try:
            (online if client.is_connected() else offline).append(host)
        except Exception:
            offline.append(host)
    return online, offline


def broker_port_map(net):
    ports = {}
    for entry in getattr(net, "brokers", []) or []:
        try:
            unpacked = net._unpack_broker(entry)
        except Exception:
            unpacked = None
        if unpacked:
            ports[unpacked[0]] = unpacked[1]
    try:
        with net.lock:
            hosts = list(net.clients.keys())
    except Exception:
        hosts = []
    for host in hosts:
        ports.setdefault(host, 1883)
    return ports


# --------------------------------------------------------------------------
# 分析：把记录变成时间轴 + 分段
# --------------------------------------------------------------------------
def build_segments(rec, response, clock):
    """返回 (segments, anchors, raw, total_ms)。时间都是客户端 perf_counter 轴上的 ms。"""
    if not rec or not rec.get("entry"):
        return [], {}, None, None
    entry = rec["entry"]
    t_entry = entry["t"]
    bcasts = rec.get("broadcast") or []
    bcast = bcasts[0] if bcasts else None
    raw = _pick_raw(rec)
    dispatch = rec.get("dispatch")
    returned = rec.get("returned")
    if bcast is None:
        return [], {}, raw, None

    total_ms = (raw["t"] - t_entry) * 1000.0 if raw else None
    if total_ms is None and returned is not None:
        total_ms = (returned["t"] - t_entry) * 1000.0
    offsets = clock.get("offset_ms")
    server_time = response.get("server_time")
    anchor_perf = None
    anchor_note = "未校准"
    if raw and offsets is not None and isinstance(server_time, (int, float)):
        anchor_epoch = float(server_time) / 1000.0 - float(offsets) / 1000.0
        anchor_perf = raw["t"] - (raw["epoch"] - anchor_epoch)
        if anchor_perf < bcast["t1"]:
            anchor_perf = bcast["t1"]
            anchor_note = "时钟差导致锚点越界，已夹到发送完成时刻"
        else:
            anchor_note = "server_time 校时锚点"

    def rel(t):
        return (t - t_entry) * 1000.0

    segments = []
    t_prep = (bcast["t0"] - t_entry) * 1000.0
    waits = entry.get("waits") or []
    wait_ms = sum((item.get("ms") or 0.0) for item in waits)
    if wait_ms > 0:
        last = waits[-1]
        segments.append({
            "name": "等待 broker 就绪",
            "start_ms": 0.0,
            "dur_ms": round(wait_ms, 3),
            "kind": "exact",
            "note": "min_broker_count=%s，在线数不足时按 %sms 轮询；返回在线=%s"
                    % (last.get("min_count"), last.get("poll_ms"),
                       last.get("online")),
        })
    segments.append({
        "name": "客户端准备",
        "start_ms": round(max(0.0, wait_ms), 3),
        "dur_ms": round(max(0.0, t_prep - wait_ms), 3),
        "kind": "exact",
        "note": "生成 req_id、组包、登记 pending（到 publish_broadcast 入口）；"
                "签名/加密在下一段内",
    })
    t_fan = (bcast["t1"] - bcast["t0"]) * 1000.0
    segments.append({
        "name": "广播扇出入队",
        "start_ms": round(rel(bcast["t0"]), 3),
        "dur_ms": round(max(0.0, t_fan), 3),
        "kind": "exact",
        "note": "向 %s 个 broker 逐个 publish（QoS0：写入本机 socket 即返回）"
                % bcast.get("published"),
    })
    if anchor_perf is not None:
        up_start = bcast["t1"]
        up_ms = (anchor_perf - up_start) * 1000.0
        segments.append({
            "name": "上行 + broker + server",
            "start_ms": round(rel(up_start), 3),
            "dur_ms": round(max(0.0, up_ms), 3),
            "kind": "anchor",
            "note": "从扇出完成到 server_time（服务端执行结束）；可继续拆成网络/broker/执行",
        })
        down_ms = (raw["t"] - anchor_perf) * 1000.0
        segments.append({
            "name": "回程 server → broker → client",
            "start_ms": round(rel(anchor_perf), 3),
            "dur_ms": round(max(0.0, down_ms), 3),
            "kind": "anchor",
            "note": "server 发布回包 → broker 路由 → 客户端收到首包（含服务端发布排队）",
        })
    else:
        if raw is not None:
            up_ms = (raw["t"] - bcast["t1"]) * 1000.0
            segments.append({
                "name": "等待首包（未校时）",
                "start_ms": round(rel(bcast["t1"]), 3),
                "dur_ms": round(max(0.0, up_ms), 3),
                "kind": "exact",
                "note": "缺少时钟校准，无法在 server_time 处切开上行/回程",
            })
        elif returned is not None:
            segments.append({
                "name": "等待回包（超时未收到）",
                "start_ms": round(rel(bcast["t1"]), 3),
                "dur_ms": round((returned["t"] - bcast["t1"]) * 1000.0, 3),
                "kind": "exact",
                "note": "timeout 内没有收到匹配 req_id 的回包（可看 brokers 表确认发到了谁）",
            })
    if raw:
        if raw.get("done") is not None:
            segments.append({
                "name": "客户端解包/验签/去重/入队",
                "start_ms": round(rel(raw["t"]), 3),
                "dur_ms": round((raw["done"] - raw["t"]) * 1000.0, 3),
                "kind": "exact",
                "note": "paho 网络线程里完成，之后才进分发队列",
            })
        if dispatch:
            start = raw.get("done") or raw["t"]
            segments.append({
                "name": "分发队列 → 业务回调",
                "start_ms": round(rel(start), 3),
                "dur_ms": round((dispatch["t"] - start) * 1000.0, 3),
                "kind": "exact",
                "note": "串行分发线程排队；回包首包匹配 pending_requests",
            })
        if dispatch and returned:
            segments.append({
                "name": "唤醒调用方",
                "start_ms": round(rel(dispatch["t"]), 3),
                "dur_ms": round((returned["t"] - dispatch["t"]) * 1000.0, 3),
                "kind": "exact",
                "note": "event.set() 到 request() 返回",
            })
    anchors = {
        "entry": 0.0,
        "broadcast_start": round(rel(bcast["t0"]), 3),
        "broadcast_end": round(rel(bcast["t1"]), 3),
        "raw_recv": round(rel(raw["t"]), 3) if raw else None,
        "server_time": round(rel(anchor_perf), 3) if anchor_perf is not None else None,
        "server_time_note": anchor_note,
        "total_ms": _round(total_ms),
    }
    return segments, anchors, raw, total_ms


def build_segment_breakdown(segments, path_info, server_probe, target_response,
                            broker_acks, ports):
    """在“上行+broker+server”里继续做估计拆分，返回可展示的明细行。"""
    rows = []
    uplink = None
    for segment in segments:
        if segment["name"].startswith("上行 + broker + server"):
            uplink = segment
            break
    if uplink is None or uplink.get("dur_ms") is None:
        return rows

    total = float(uplink["dur_ms"])
    host_up = (target_response or {}).get("server_from")
    net_up = None
    if host_up and path_info.get(host_up):
        tcp = path_info[host_up].get("tcp") or {}
        net_up = tcp.get("min")
    if net_up is None and host_up and path_info.get(host_up):
        ping = path_info[host_up].get("ping") or {}
        net_up = ping.get("min")
    tcp_up = (net_up / 2.0) if net_up is not None else None

    broker_est = None
    if host_up and broker_acks.get(host_up) is not None and net_up is not None:
        broker_est = max(0.0, float(broker_acks[host_up]) - float(net_up))

    server_net = None
    remote_tcp = None
    if server_probe and server_probe.get("data"):
        values = server_probe["data"].get("tcp_rtt_ms") or []
        if values:
            remote_tcp = min(values)
            server_net = remote_tcp / 2.0

    used = 0.0
    if tcp_up is not None:
        used += tcp_up
        rows.append({
            "name": "client → broker（上行网络）",
            "ms": _round(tcp_up),
            "kind": "est",
            "note": "TCP connect RTT/2；broker=%s" % (host_up or "?"),
        })
    if broker_est is not None:
        used += broker_est
        broker_note = "QoS1 PUBACK RTT - TCP RTT；含 broker 路由/排队"
        if broker_est <= 0.5:
            broker_note += "（差值在 RTT 抖动内，测不出）"
        rows.append({
            "name": "broker 入站处理（估）",
            "ms": _round(broker_est),
            "kind": "est",
            "note": broker_note,
        })
    if server_net is not None:
        used += server_net
        rows.append({
            "name": "broker → server（估）",
            "ms": _round(server_net),
            "kind": "est",
            "note": "server 侧 TCP connect RTT/2（同一 broker）",
        })
    residual = _round(max(0.0, total - used))
    rows.append({
        "name": "server 排队 + 解析 + 执行（残差）",
        "ms": residual,
        "kind": "residual",
        "note": "= 到 server_time 总时长 - 上面已估网络/broker 段；含未知的单向不对称",
    })
    return rows


def build_report(args, topic, node, recorder, target_response, clock,
                 calib_before, calib_after, broker_acks, paths, server_probe,
                 connect_info, target_rec):
    response = target_response or {}
    segments, anchors, raw, total_ms = build_segments(target_rec, response, clock)

    ports = connect_info.get("ports") or {}
    path_info = {}
    for item in paths:
        if item:
            path_info[item["host"]] = item

    breakdown = build_segment_breakdown(
        segments, path_info, server_probe, response, broker_acks, ports)

    broker_rows = []
    publishes = (target_rec or {}).get("publishes") or []
    seen = set()
    for item in publishes:
        if item.get("topic") != topic:
            continue
        host = item.get("host")
        seen.add(host)
        ack = item.get("ack")
        broker_rows.append({
            "host": host,
            "qos": item.get("qos"),
            "publish_ms": _round((item["t1"] - item["t0"]) * 1000.0),
            "ack_ms": _round((ack[0] - item["t0"]) * 1000.0) if ack else None,
            "tcp_rtt_ms": ((path_info.get(host) or {}).get("tcp") or {}).get("min"),
            "ping_ms": ((path_info.get(host) or {}).get("ping") or {}).get("min"),
            "broker_proc_ms": (
                _round(max(0.0, (ack[0] - item["t0"]) * 1000.0
                           - (((path_info.get(host) or {}).get("tcp") or {}).get("min") or 0.0)))
                if ack and ((path_info.get(host) or {}).get("tcp") or {}).get("min")
                else None),
            "puback_probe_ms": broker_acks.get(host),
            "uplink_winner": host == response.get("server_from"),
            "downlink_winner": host == response.get("client_from"),
        })

    raw_item = raw or {}
    report = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "request": {
            "code": args.code,
            "topic": topic,
            "timeout_s": args.timeout,
            "min_broker_count": args.min_broker,
        },
        "response": {
            "req_id": response.get("req_id"),
            "ok": response.get("ok"),
            "r": response.get("r"),
            "error": response.get("error"),
            "latency_ms": response.get("latency_ms"),
            "server_time": response.get("server_time"),
            "server_from": response.get("server_from"),
            "client_from": response.get("client_from"),
            "stdout": (response.get("stdout") or "")[:4000],
        },
        "connect": connect_info,
        "clock": {
            "before": calib_before,
            "after": calib_after,
            "offset_ms": clock.get("offset_ms"),
            "uncertainty_ms": clock.get("uncertainty_ms"),
            "samples": clock.get("samples"),
        },
        "anchors": anchors,
        "segments": segments,
        "breakdown": breakdown,
        "brokers": broker_rows,
        "claim": {
            "raw_host": raw_item.get("host"),
            "dispatch_host": ((target_rec or {}).get("dispatch") or {}).get("host"),
            "raw_bytes": raw_item.get("bytes"),
        },
        "paths": path_info,
        "server_probe": {
            "enabled": bool(server_probe),
            "broker": (server_probe or {}).get("broker"),
            "data": (server_probe or {}).get("data"),
            "error": (server_probe or {}).get("error"),
            "client_to_server_ping": (server_probe or {}).get("client_to_server_ping"),
            "client_to_server_trace": (server_probe or {}).get("client_to_server_trace"),
        },
        "total_ms": _round(total_ms),
    }
    return report


# --------------------------------------------------------------------------
# 终端渲染
# --------------------------------------------------------------------------
class Colors(object):
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    CYAN = "\033[36m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    MAGENTA = "\033[35m"
    RED = "\033[31m"
    GRAY = "\033[90m"


def _enable_color(enabled):
    if not enabled:
        for key in dir(Colors):
            if key.isupper():
                setattr(Colors, key, "")
    return enabled


def _bar(fraction, width=36):
    try:
        fraction = max(0.0, min(1.0, float(fraction)))
    except (TypeError, ValueError):
        fraction = 0.0
    filled = int(round(fraction * width))
    return "#" * filled + "." * (width - filled)


def _short_repr(value, limit=120):
    """单行短 repr：避免长的回包把表头撑破。"""
    if isinstance(value, str) and len(value) > limit:
        return value[:limit] + "..."
    text = repr(value)
    if len(text) > limit:
        text = text[:limit] + "..."
    return text


def _kind_tag(kind):
    return {
        "exact": "实测",
        "anchor": "校时",
        "est": "估算",
        "residual": "残差",
    }.get(kind, kind or "")


def render_console(report):
    c = Colors
    req = report["request"]
    resp = report["response"]
    anchors = report["anchors"]
    lines = []
    lines.append("")
    lines.append(c.BOLD + c.CYAN + "=" * 78 + c.RESET)
    lines.append(c.BOLD + c.CYAN + " 单次 MQTT RPC 全链路分步耗时观测" + c.RESET)
    lines.append(c.BOLD + c.CYAN + "=" * 78 + c.RESET)
    lines.append("请求  code=%r  topic=%s  timeout=%.1fs  mb=%d" % (
        req["code"], req["topic"], req["timeout_s"], req["min_broker_count"]))
    lines.append("回包  req_id=%s  ok=%s  r=%s" % (
        resp.get("req_id"), resp.get("ok"), _short_repr(resp.get("r"))))
    lines.append("       latency_ms=%s  server_time=%s" % (
        resp.get("latency_ms"), resp.get("server_time")))
    lines.append("       server_from=%s  client_from=%s" % (
        resp.get("server_from"), resp.get("client_from")))
    conn = report.get("connect") or {}
    lines.append("连接  配置 %s / 在线 %s / 离线 %s" % (
        conn.get("configured"), conn.get("online"),
        len(conn.get("offline") or [])))
    if conn.get("offline"):
        lines.append("      离线: %s" % ", ".join(conn["offline"][:8]))
    clock = report.get("clock") or {}
    if clock.get("offset_ms") is not None:
        lines.append("校时  server-client offset = %+.3f ms  (不确定度 ±%s ms, 样本 %d)" % (
            clock["offset_ms"], clock.get("uncertainty_ms"),
            len(clock.get("samples") or [])))
    else:
        lines.append("校时  失败：无法把 server_time 换算到客户端时间轴")

    lines.append("")
    lines.append(c.BOLD + "-- 时间轴（相对 request 入口）" + c.RESET)
    if anchors.get("total_ms") is not None:
        lines.append("  总往返(首包) = %.3f ms" % anchors["total_ms"])
    for segment in report.get("segments") or []:
        start = segment.get("start_ms")
        dur = segment.get("dur_ms")
        if dur is None:
            continue
        lines.append("  %s ms  %s %s ms  %s  %s" % (
            _rjust(_fmt_ms(start), 9), _ljust(segment["name"], 30),
            _rjust(_fmt_ms(dur), 9),
            _bar(dur / max(1e-9, anchors.get("total_ms") or dur)),
            c.DIM + segment.get("note", "") + c.RESET))

    if report.get("breakdown"):
        lines.append("")
        lines.append(c.BOLD + "-- 上行段再拆分（估算口径）" + c.RESET)
        for row in report["breakdown"]:
            if row.get("ms") is None:
                continue
            lines.append("  %s %s ms  [%s]  %s" % (
                _ljust(row["name"], 36), _rjust(_fmt_ms(row["ms"]), 9),
                _kind_tag(row.get("kind")), row.get("note", "")))

    lines.append("")
    lines.append(c.BOLD + "-- 每个 broker（本次请求的上行扇出）" + c.RESET)
    lines.append("  %-32s %8s %8s %8s %8s %8s" % (
        "broker", "publish", "ack", "TCP RTT", "broker估", "PUBACK探测"))
    for row in report.get("brokers") or []:
        tag = ""
        if row.get("uplink_winner"):
            tag += " ↑U"
        if row.get("downlink_winner"):
            tag += " ↓D"
        lines.append("  %s %8s %8s %8s %8s %8s%s" % (
            _ljust(row["host"][:32], 32), _fmt_ms(row.get("publish_ms")),
            _fmt_ms(row.get("ack_ms")), _fmt_ms(row.get("tcp_rtt_ms")),
            _fmt_ms(row.get("broker_proc_ms")), _fmt_ms(row.get("puback_probe_ms")),
            tag))

    paths = report.get("paths") or {}
    if paths:
        lines.append("")
        lines.append(c.BOLD + "-- 客户端 → broker 逐跳（ICMP RTT）" + c.RESET)
        for host, info in paths.items():
            trace = info.get("trace") or {}
            if not trace.get("available"):
                lines.append("  %s: tracert 不可用 (%s)" % (
                    host, trace.get("reason") or trace.get("error")))
                continue
            lines.append("  %s  (TCP RTT min=%s ms, ping min=%s ms)" % (
                host, _fmt_ms((info.get("tcp") or {}).get("min")),
                _fmt_ms((info.get("ping") or {}).get("min"))))
            for hop in trace.get("hops") or []:
                delta = hop.get("delta")
                lines.append("    %2s  %-16s RTT=%8s ms  Δ=%8s ms%s" % (
                    hop.get("hop"), hop.get("addr") or "*",
                    _fmt_ms(hop.get("min")), _fmt_ms(delta),
                    "  <- 本地网关/Wi-Fi" if hop.get("hop") == 1 else ""))
            lines.append(c.DIM + "    Δ=相对上一可见跳的 RTT 增量；* 表示该跳不响应 ICMP（不代表丢包）"
                         + c.RESET)

    sp = report.get("server_probe") or {}
    data = sp.get("data")
    if data:
        lines.append("")
        lines.append(c.BOLD + "-- server 侧反向路径（远端代码实测）" + c.RESET)
        lines.append("  server ip=%s  gateway=%s  platform=%s" % (
            data.get("ip"), data.get("gateway"), data.get("platform")))
        pg = data.get("ping_gateway") or {}
        if pg.get("min") is not None:
            lines.append("  server → gateway（Wi-Fi 最后一段）: min=%s avg=%s max=%s ms" % (
                _fmt_ms(pg.get("min")), _fmt_ms(pg.get("avg")), _fmt_ms(pg.get("max"))))
        pb = data.get("ping_broker") or {}
        if pb.get("min") is not None:
            lines.append("  server → broker ping: min=%s avg=%s max=%s ms" % (
                _fmt_ms(pb.get("min")), _fmt_ms(pb.get("avg")), _fmt_ms(pb.get("max"))))
        tcp_values = data.get("tcp_rtt_ms") or []
        if tcp_values:
            lines.append("  server → broker TCP connect RTT: %s ms" % tcp_values)
        for hop in data.get("tracert_hops") or []:
            lines.append("    %2s  RTT=%8s ms  Δ=%8s ms%s" % (
                hop.get("hop"), _fmt_ms(hop.get("min")), _fmt_ms(hop.get("delta")),
                "  <- server 的本地网关/Wi-Fi" if hop.get("hop") == 1 else ""))
        c2s = sp.get("client_to_server_ping") or {}
        if c2s.get("min") is not None:
            lines.append("  client → server 直连 ping（非 MQTT 路径）: min=%s avg=%s max=%s ms" % (
                _fmt_ms(c2s.get("min")), _fmt_ms(c2s.get("avg")), _fmt_ms(c2s.get("max"))))
    elif sp.get("enabled"):
        lines.append("")
        lines.append(c.YELLOW + "-- server 侧反向路径：未取到结果 (%s)" % (
            sp.get("error") or "stdout 非 JSON") + c.RESET)

    lines.append("")
    lines.append(c.DIM + "口径: 实测=客户端 perf_counter; 校时=用最小 RTT 估计时钟差后以 server_time 为锚; "
                 "估算=TCP/ICMP RTT 折算; 残差=无法在客户端直接观测的部分。" + c.RESET)
    lines.append(c.DIM + "      中间路由器只能看到 RTT 增量，看不到它转发这一包的精确时刻；"
                 "broker→server 方向由 server 侧反向 traceroute 实测。" + c.RESET)
    lines.append("")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# HTML 渲染（自包含，无外部资源）
# --------------------------------------------------------------------------
_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>RPC 全链路耗时 · {{REQ_ID}}</title>
<style>
  :root {
    --bg: #0b1020; --panel: #121a30; --panel2: #0f1729; --line: #24304e;
    --text: #e6edf7; --muted: #8b9bb8; --cyan: #4dd0e1; --green: #66bb6a;
    --yellow: #ffca28; --red: #ef5350; --purple: #b388ff; --blue: #64b5f6;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--text);
    font: 14px/1.6 "Segoe UI", "Microsoft YaHei", system-ui, sans-serif; }
  .wrap { max-width: 1180px; margin: 0 auto; padding: 28px 20px 60px; }
  h1 { font-size: 22px; margin: 0 0 4px; }
  h2 { font-size: 16px; margin: 28px 0 10px; color: var(--cyan); }
  .sub { color: var(--muted); font-size: 12px; margin-bottom: 18px; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; }
  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 12px 14px; }
  .card .k { color: var(--muted); font-size: 11px; letter-spacing: .04em; }
  .card .v { font-size: 18px; font-weight: 600; margin-top: 4px; word-break: break-all; }
  .card .v.small { font-size: 13px; font-weight: 500; }
  .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 12px; padding: 16px; }
  .row { display: grid; grid-template-columns: 250px 1fr 120px; gap: 12px; align-items: center;
    padding: 7px 0; border-bottom: 1px dashed rgba(255,255,255,.06); }
  .row:last-child { border-bottom: 0; }
  .row .lbl { font-size: 13px; }
  .row .lbl small { display: block; color: var(--muted); font-size: 11px; }
  .track { position: relative; height: 18px; background: var(--panel2); border-radius: 4px; overflow: hidden; }
  .bar { position: absolute; top: 3px; height: 12px; border-radius: 3px; min-width: 2px; }
  .bar.exact { background: var(--green); }
  .bar.anchor { background: var(--cyan); }
  .bar.est { background: var(--yellow); opacity: .85; }
  .bar.residual { background: var(--purple); opacity: .85; }
  .val { text-align: right; font-variant-numeric: tabular-nums; font-size: 12px; color: var(--muted); }
  .val b { color: var(--text); font-size: 13px; }
  table { width: 100%; border-collapse: collapse; font-size: 12.5px; }
  th, td { padding: 7px 9px; border-bottom: 1px solid var(--line); text-align: left; }
  th { color: var(--muted); font-weight: 500; font-size: 11px; letter-spacing: .03em; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  .tag { display: inline-block; padding: 1px 6px; border-radius: 999px; font-size: 10px;
    background: rgba(77,208,225,.12); color: var(--cyan); margin-left: 6px; }
  .tag.up { background: rgba(102,187,106,.14); color: var(--green); }
  .tag.down { background: rgba(179,136,255,.14); color: var(--purple); }
  .legend { display: flex; gap: 16px; flex-wrap: wrap; color: var(--muted); font-size: 11px; margin-top: 10px; }
  .dot { display: inline-block; width: 9px; height: 9px; border-radius: 2px; margin-right: 5px; }
  .note { color: var(--muted); font-size: 11.5px; margin-top: 10px; }
  code { background: #0a0f1e; border: 1px solid var(--line); border-radius: 4px;
    padding: 1px 5px; font-family: Consolas, monospace; font-size: 12px; }
  .good { color: var(--green); } .warn { color: var(--yellow); } .bad { color: var(--red); }
</style>
</head>
<body>
<div class="wrap">
  <h1>单次 MQTT RPC 全链路耗时</h1>
  <div class="sub">{{GENERATED_AT}} · {{TOPIC}} · {{REQ_ID}}</div>

  <div class="cards">
    {{CARDS}}
  </div>

  <h2>时间轴</h2>
  <div class="panel">
    {{TIMELINE}}
    <div class="legend">
      <span><i class="dot" style="background:var(--green)"></i>实测</span>
      <span><i class="dot" style="background:var(--cyan)"></i>校时锚点</span>
      <span><i class="dot" style="background:var(--yellow)"></i>估算</span>
      <span><i class="dot" style="background:var(--purple)"></i>残差</span>
    </div>
  </div>

  <h2>上行段再拆分</h2>
  <div class="panel">{{BREAKDOWN}}</div>

  <h2>Broker 视角</h2>
  <div class="panel">{{BROKERS}}</div>

  <h2>网络路径</h2>
  <div class="panel">{{PATHS}}</div>

  <h2>Server 侧反向路径</h2>
  <div class="panel">{{SERVER}}</div>

  <div class="note">{{NOTES}}</div>
</div>
</body>
</html>
"""


def _seg_color(kind):
    return {"exact": "exact", "anchor": "anchor",
            "est": "est", "residual": "residual"}.get(kind, "exact")


def render_html(report):
    total = report.get("total_ms") or 0.0
    scale = max(1e-9, float(total))

    def timeline_rows():
        out = []
        for segment in report.get("segments") or []:
            if segment.get("dur_ms") is None:
                continue
            start = max(0.0, float(segment.get("start_ms") or 0.0))
            width = max(0.0, float(segment["dur_ms"]))
            out.append(
                '<div class="row"><div class="lbl">%s<small>%s</small></div>'
                '<div class="track"><div class="bar %s" style="left:%.3f%%;width:%.3f%%" '
                'title="%s: %.3f ms"></div></div>'
                '<div class="val"><b>%s</b> ms</div></div>' % (
                    html.escape(segment["name"]),
                    html.escape(segment.get("note", "")),
                    _seg_color(segment.get("kind")),
                    min(100.0, start / scale * 100.0),
                    min(100.0, width / scale * 100.0),
                    html.escape(segment["name"]), width,
                    _fmt_ms(width)))
        cursor = float(report.get("anchors", {}).get("broadcast_start") or 0.0)
        for row in report.get("breakdown") or []:
            if row.get("ms") is None:
                continue
            width = float(row["ms"])
            start = cursor
            cursor += width
            out.append(
                '<div class="row"><div class="lbl">↳ %s<small>%s</small></div>'
                '<div class="track"><div class="bar %s" style="left:%.3f%%;width:%.3f%%" '
                'title="%s: %.3f ms"></div></div>'
                '<div class="val"><b>%s</b> ms</div></div>' % (
                    html.escape(row["name"]),
                    html.escape(row.get("note", "")),
                    _seg_color(row.get("kind")),
                    min(100.0, start / scale * 100.0),
                    min(100.0, width / scale * 100.0),
                    html.escape(row["name"]), width,
                    _fmt_ms(width)))
        return "".join(out) if out else '<div class="note">无可用分段</div>'

    def breakdown_rows():
        rows = report.get("breakdown") or []
        if not rows:
            return '<div class="note">无（缺少时钟校准或路径数据）</div>'
        body = "".join(
            '<tr><td>%s</td><td class="num">%s</td><td>%s</td><td>%s</td></tr>' % (
                html.escape(row["name"]), _fmt_ms(row.get("ms")),
                _kind_tag(row.get("kind")), html.escape(row.get("note", "")))
            for row in rows)
        return ('<table><thead><tr><th>阶段</th><th>耗时 ms</th><th>口径</th>'
                '<th>说明</th></tr></thead><tbody>%s</tbody></table>' % body)

    def broker_rows():
        rows = report.get("brokers") or []
        if not rows:
            return '<div class="note">没有捕获到本次请求的 publish</div>'
        body = []
        for row in rows:
            tags = ""
            if row.get("uplink_winner"):
                tags += '<span class="tag up">首达 server</span>'
            if row.get("downlink_winner"):
                tags += '<span class="tag down">首达 client</span>'
            body.append(
                '<tr><td>%s%s</td><td class="num">%s</td><td class="num">%s</td>'
                '<td class="num">%s</td><td class="num">%s</td><td class="num">%s</td></tr>' % (
                    html.escape(row["host"]), tags,
                    _fmt_ms(row.get("publish_ms")), _fmt_ms(row.get("ack_ms")),
                    _fmt_ms(row.get("tcp_rtt_ms")), _fmt_ms(row.get("broker_proc_ms")),
                    _fmt_ms(row.get("puback_probe_ms"))))
        return ('<table><thead><tr><th>Broker</th><th>publish ms</th><th>ack ms</th>'
                '<th>TCP RTT</th><th>broker 估</th><th>PUBACK 探测</th></tr></thead>'
                '<tbody>%s</tbody></table>' % "".join(body))

    def path_section():
        paths = report.get("paths") or {}
        if not paths:
            return '<div class="note">未做路径探测</div>'
        chunks = []
        for host, info in paths.items():
            trace = info.get("trace") or {}
            if not trace.get("available"):
                chunks.append('<div class="note">%s: tracert 不可用（%s）</div>' % (
                    html.escape(host), html.escape(str(trace.get("reason") or trace.get("error")))))
                continue
            rows = []
            for hop in trace.get("hops") or []:
                rows.append(
                    '<tr><td class="num">%s</td><td>%s</td><td class="num">%s</td>'
                    '<td class="num">%s</td><td>%s</td></tr>' % (
                        hop.get("hop"), html.escape(str(hop.get("addr") or "*")),
                        _fmt_ms(hop.get("min")), _fmt_ms(hop.get("delta")),
                        "本地网关 / Wi-Fi" if hop.get("hop") == 1 else ""))
            chunks.append(
                '<h3 style="font-size:13px;margin:14px 0 6px">%s '
                '<span class="note">TCP RTT min=%s / ping min=%s ms</span></h3>'
                '<table><thead><tr><th>跳</th><th>地址</th><th>RTT ms</th>'
                '<th>Δ ms</th><th>备注</th></tr></thead><tbody>%s</tbody></table>'
                '<div class="note">Δ=相对上一可见跳的 RTT 增量；* 表示该跳不响应 ICMP（不代表丢包）。</div>' % (
                    html.escape(host),
                    _fmt_ms((info.get("tcp") or {}).get("min")),
                    _fmt_ms((info.get("ping") or {}).get("min")),
                    "".join(rows)))
        return "".join(chunks)

    def server_section():
        sp = report.get("server_probe") or {}
        data = sp.get("data")
        if not data:
            return '<div class="note">未启用或未取到 server 侧结果（%s）</div>' % (
                html.escape(str(sp.get("error") or "stdout 非 JSON")))
        rows = []
        pg = data.get("ping_gateway") or {}
        if pg.get("min") is not None:
            rows.append("<tr><td>server → gateway / Wi-Fi 最后一段</td><td class=\"num\">%s</td>"
                        "<td class=\"num\">%s</td><td class=\"num\">%s</td></tr>" % (
                            _fmt_ms(pg.get("min")), _fmt_ms(pg.get("avg")),
                            _fmt_ms(pg.get("max"))))
        pb = data.get("ping_broker") or {}
        if pb.get("min") is not None:
            rows.append("<tr><td>server → broker ping</td><td class=\"num\">%s</td>"
                        "<td class=\"num\">%s</td><td class=\"num\">%s</td></tr>" % (
                            _fmt_ms(pb.get("min")), _fmt_ms(pb.get("avg")),
                            _fmt_ms(pb.get("max"))))
        tcp_values = data.get("tcp_rtt_ms") or []
        if tcp_values:
            rows.append("<tr><td>server → broker TCP connect RTT</td>"
                        "<td class=\"num\">%s</td><td class=\"num\">-</td>"
                        "<td class=\"num\">-</td></tr>" % html.escape(str(tcp_values)))
        c2s = sp.get("client_to_server_ping") or {}
        if c2s.get("min") is not None:
            rows.append("<tr><td>client → server 直连 ping（非 MQTT 路径）</td>"
                        "<td class=\"num\">%s</td><td class=\"num\">%s</td>"
                        "<td class=\"num\">%s</td></tr>" % (
                            _fmt_ms(c2s.get("min")), _fmt_ms(c2s.get("avg")),
                            _fmt_ms(c2s.get("max"))))
        hops = data.get("tracert_hops") or []
        hop_rows = "".join(
            '<tr><td class="num">%s</td><td class="num">%s</td><td class="num">%s</td>'
            '<td>%s</td></tr>' % (
                hop.get("hop"), _fmt_ms(hop.get("min")), _fmt_ms(hop.get("delta")),
                "server 的本地网关 / Wi-Fi" if hop.get("hop") == 1 else "")
            for hop in hops)
        return (
            '<div class="note">server ip=%s · gateway=%s · %s</div>'
            '<table><thead><tr><th>阶段</th><th>min ms</th><th>avg ms</th>'
            '<th>max ms</th></tr></thead><tbody>%s</tbody></table>%s' % (
                html.escape(str(data.get("ip"))), html.escape(str(data.get("gateway"))),
                html.escape(str(data.get("platform"))), "".join(rows),
                ('<h3 style="font-size:13px;margin:14px 0 6px">server → broker 逐跳</h3>'
                 '<table><thead><tr><th>跳</th><th>RTT ms</th><th>Δ ms</th><th>备注</th></tr>'
                 '</thead><tbody>%s</tbody></table>' % hop_rows) if hop_rows else ""))

    resp = report.get("response") or {}
    clock = report.get("clock") or {}
    conn = report.get("connect") or {}
    cards = [
        ("首包往返", "%s ms" % _fmt_ms(report.get("total_ms")), ""),
        ("服务端校时锚点", "%s ms" % _fmt_ms((report.get("anchors") or {}).get("server_time")), ""),
        ("client 时钟差", "%+.2f ms" % (clock.get("offset_ms") or 0.0)
         if clock.get("offset_ms") is not None else "-", ""),
        ("server_from", str(resp.get("server_from")), "small"),
        ("client_from", str(resp.get("client_from")), "small"),
        ("在线 broker", "%s / %s" % (conn.get("online"), conn.get("configured")), ""),
    ]
    cards_html = "".join(
        '<div class="card"><div class="k">%s</div><div class="v %s">%s</div></div>' % (
            html.escape(k), cls, html.escape(str(v))) for k, v, cls in cards)

    notes = ("口径：实测=客户端 perf_counter；校时=最小 RTT 估计时钟差后以 server_time 为锚；"
             "估算=TCP/ICMP RTT 折算或 QoS1 PUBACK 反推；残差=客户端无法直接观测的服务端阶段。"
             "单向网络耗时统一按 RTT/2 折算（对称假设）。中间路由器只能看到 RTT 增量，"
             "看不到它转发这一包的精确时刻；broker→server 方向由 server 侧反向 traceroute 实测。"
             "对非平凡 code，残差会包含服务端执行时间。")

    return (_HTML_TEMPLATE
            .replace("{{REQ_ID}}", html.escape(str(resp.get("req_id"))))
            .replace("{{GENERATED_AT}}", html.escape(report.get("generated_at", "")))
            .replace("{{TOPIC}}", html.escape(str((report.get("request") or {}).get("topic"))))
            .replace("{{CARDS}}", cards_html)
            .replace("{{TIMELINE}}", timeline_rows())
            .replace("{{BREAKDOWN}}", breakdown_rows())
            .replace("{{BROKERS}}", broker_rows())
            .replace("{{PATHS}}", path_section())
            .replace("{{SERVER}}", server_section())
            .replace("{{NOTES}}", html.escape(notes)))


# --------------------------------------------------------------------------
# demo：不联网，验证渲染
# --------------------------------------------------------------------------
def demo_report():
    segments = [
        {"name": "客户端准备", "start_ms": 0.0, "dur_ms": 0.041, "kind": "exact",
         "note": "生成 req_id、组包、登记 pending；签名/加密在下一段内"},
        {"name": "广播扇出入队", "start_ms": 0.041, "dur_ms": 0.612, "kind": "exact",
         "note": "向 12 个 broker 逐个 publish（QoS0：写入本机 socket 即返回）"},
        {"name": "上行 + broker + server", "start_ms": 0.653, "dur_ms": 65.221,
         "kind": "anchor", "note": "从扇出完成到 server_time（服务端执行结束）"},
        {"name": "回程 server → broker → client", "start_ms": 65.874, "dur_ms": 85.186,
         "kind": "anchor", "note": "server 发布回包 → broker 路由 → 客户端收到首包"},
        {"name": "客户端解包/验签/去重/入队", "start_ms": 151.060, "dur_ms": 0.214,
         "kind": "exact", "note": "paho 网络线程里完成，之后才进分发队列"},
        {"name": "分发队列 → 业务回调", "start_ms": 151.274, "dur_ms": 0.061,
         "kind": "exact", "note": "串行分发线程排队；回包首包匹配 pending_requests"},
        {"name": "唤醒调用方", "start_ms": 151.335, "dur_ms": 0.015,
         "kind": "exact", "note": "event.set() 到 request() 返回"},
    ]
    breakdown = [
        {"name": "client → broker（上行网络）", "ms": 15.7, "kind": "est",
         "note": "TCP connect RTT/2；broker=broker.codenow.cn"},
        {"name": "broker 入站处理（估）", "ms": 1.2, "kind": "est",
         "note": "QoS1 PUBACK RTT - TCP RTT；含 broker 路由/排队"},
        {"name": "broker → server（估）", "ms": 18.4, "kind": "est",
         "note": "server 侧 TCP connect RTT/2（同一 broker）"},
        {"name": "server 排队 + 解析 + 执行（残差）", "ms": 29.9, "kind": "residual",
         "note": "= 到 server_time 总时长 - 上面已估网络/broker 段"},
    ]
    return {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "request": {"code": "3", "topic": "14c", "timeout_s": 10, "min_broker_count": 6},
        "response": {"req_id": "20261112_212417.837", "ok": True, "r": "3",
                     "latency_ms": 151.35, "server_time": 1794489946039,
                     "server_from": "broker.codenow.cn",
                     "client_from": "broker.codenow.cn", "stdout": ""},
        "connect": {"configured": 14, "online": 12,
                    "offline": ["broker.hivemq.com", "test.mosquitto.org"],
                    "ports": {"broker.codenow.cn": 1883}},
        "clock": {"offset_ms": 1234.5, "uncertainty_ms": 16.0,
                  "samples": [{"round": 0, "rtt_ms": 31.9, "offset_ms": 1234.5}]},
        "anchors": {"entry": 0.0, "broadcast_start": 0.041,
                    "broadcast_end": 0.653, "raw_recv": 151.06,
                    "server_time": 65.874, "total_ms": 151.35},
        "segments": segments,
        "breakdown": breakdown,
        "brokers": [
            {"host": "broker.codenow.cn", "publish_ms": 0.12, "ack_ms": 31.6,
             "tcp_rtt_ms": 31.4, "broker_proc_ms": 0.2, "puback_probe_ms": 32.1,
             "uplink_winner": True, "downlink_winner": True},
            {"host": "mqtt.flespi.io", "publish_ms": 0.09, "ack_ms": 426.6,
             "tcp_rtt_ms": 425.0, "broker_proc_ms": 1.6, "puback_probe_ms": 427.2},
            {"host": "broker.emqx.io", "publish_ms": 0.08, "ack_ms": 282.9,
             "tcp_rtt_ms": 280.0, "broker_proc_ms": 2.9, "puback_probe_ms": 283.5},
        ],
        "claim": {"raw_host": "broker.codenow.cn",
                  "dispatch_host": "broker.codenow.cn", "raw_bytes": 120},
        "paths": {
            "broker.codenow.cn": {
                "host": "broker.codenow.cn", "port": 1883,
                "tcp": {"min": 31.4}, "ping": {"min": 31.2},
                "trace": {"available": True, "hops": [
                    {"hop": 1, "addr": "192.168.1.1", "min": 3.1, "delta": None},
                    {"hop": 2, "addr": "100.64.0.1", "min": 5.2, "delta": 2.1},
                    {"hop": 3, "addr": "219.158.8.1", "min": 17.0, "delta": 11.8},
                    {"hop": 4, "addr": "203.0.113.9", "min": 29.4, "delta": 12.4},
                    {"hop": 5, "addr": "47.96.1.10", "min": 31.0, "delta": 1.6},
                ]},
            }
        },
        "server_probe": {
            "enabled": True, "broker": "broker.codenow.cn",
            "data": {"ip": "192.168.1.50", "gateway": "192.168.1.1",
                     "platform": "Linux-5.10 aarch64",
                     "ping_gateway": {"min": 2.1, "avg": 3.4, "max": 5.0},
                     "ping_broker": {"min": 18.3, "avg": 20.1, "max": 26.0},
                     "tcp_rtt_ms": [18.3, 19.0, 18.7],
                     "tracert_hops": [
                         {"hop": 1, "rtt_ms": [2.2]},
                         {"hop": 2, "rtt_ms": [6.4]},
                         {"hop": 3, "rtt_ms": [18.9]},
                     ]},
            "error": None,
            "client_to_server_ping": {"min": 2.4, "avg": 3.0, "max": 4.1},
        },
        "total_ms": 151.35,
    }


# --------------------------------------------------------------------------
# 主流程
# --------------------------------------------------------------------------
def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="旁路观测单次 MQTT RPC 的全链路分步耗时",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("code", nargs="?", default="pass",
                        help="要执行的远端代码（默认 pass，只做一次极短请求）")
    parser.add_argument("-t", "--topic", default=None,
                        help="request topic；默认取 client_mqtt.REQUEST_TOPIC")
    parser.add_argument("--to", "--timeout", dest="timeout", type=float, default=15.0,
                        help="单次请求超时秒数（默认 15）")
    parser.add_argument("-k", "--key", default=None,
                        help="客户端私钥：文件路径 / PEM / 整数表达式（如 2**222）")
    parser.add_argument("--mb", "--min-broker", dest="min_broker", type=int, default=1,
                        help="要求至少多少个 broker 在线（默认 1）")
    parser.add_argument("-a", "--allow-no-pub", action="store_true",
                        help="允许服务端未验签的原样回包")
    parser.add_argument("--broker-add", action="append", default=[],
                        help="强制加入 broker，格式 host[:port] 或 user:pass@host[:port]，可重复")
    parser.add_argument("--connect-timeout", type=float, default=15.0,
                        help="等待 broker 连接的超时秒数（默认 15）")
    parser.add_argument("--calib", type=int, default=4,
                        help="请求前后各做几条极短 RPC 做时钟校准（默认 4）")
    parser.add_argument("--no-calib", action="store_true", help="跳过时钟校准")
    parser.add_argument("--no-path", action="store_true", help="跳过 ping/tracert 路径探测")
    parser.add_argument("--path-all", action="store_true",
                        help="对所有在线 broker 做 tracert（默认只测本次命中的 2 个）")
    parser.add_argument("--hops", type=int, default=16, help="tracert 最大跳数（默认 16）")
    parser.add_argument("--server-path", dest="server_path", action="store_true",
                        default=True, help="启用 server 侧反向路径探测（默认开）")
    parser.add_argument("--no-server-path", dest="server_path", action="store_false",
                        help="关闭 server 侧反向路径探测")
    parser.add_argument("--server-tracert", action="store_true",
                        help="server 侧也跑 tracert（会占用服务端执行器数秒）")
    parser.add_argument("--server-hops", type=int, default=12,
                        help="server 侧 tracert 最大跳数（默认 12）")
    parser.add_argument("--ack-probe", type=int, default=2,
                        help="QoS1 PUBACK 探测轮数（默认 2，0 关闭）")
    parser.add_argument("--json-out", default=None, help="把结构化报告写到 JSON 文件")
    parser.add_argument("--html", default=None, help="生成自包含 HTML 时间轴")
    parser.add_argument("--no-color", action="store_true", help="关闭终端颜色")
    parser.add_argument("--demo", action="store_true", help="不联网，用示例数据渲染")
    return parser.parse_args(argv)


def run_live(args):
    from client import client_mqtt

    topic = args.topic or client_mqtt.REQUEST_TOPIC
    node = client_mqtt.MQTTClientNode(
        client_private_key_bytes=args.key,
        allow_no_server_pubkey_response=bool(args.allow_no_pub),
        stats_print_interval=0,
        stats_ping_interval=0,
        broker_add=(args.broker_add or None),
    )
    recorder = Recorder(client_mqtt.REPLY_TOPIC)
    install_hooks(node, recorder)
    net = node.mqtt_net

    connect_info = {}
    try:
        node.start(connect_timeout=args.connect_timeout)
        wrap_client_publishes(net, recorder)
        online = net.wait_connected(min_count=1, timeout=max(1.0, args.connect_timeout))
        with net.lock:
            clients = dict(net.clients)
        online_hosts, offline_hosts = snapshot_hosts(net)
        connect_info = {
            "configured": len(getattr(net, "brokers", []) or clients),
            "clients": len(clients),
            "online": len(online_hosts),
            "online_hosts": online_hosts,
            "offline": offline_hosts,
            "local_ip": _local_ip(),
            "ports": broker_port_map(net),
        }
        print("[trace] 已启动：配置 %d / 对象 %d / 在线 %d" % (
            connect_info["configured"], connect_info["clients"], connect_info["online"]))
        if online < args.min_broker:
            print("[trace] 在线 broker %d < 要求 %d；仍继续发送（请求可能失败）" % (
                online, args.min_broker))

        calib_before = {"samples": [], "offset_ms": None, "uncertainty_ms": None}
        calib_after = {"samples": [], "offset_ms": None, "uncertainty_ms": None}
        if not args.no_calib and args.calib > 0:
            print("[trace] 时钟校准（请求前 %d 条极短 RPC）..." % args.calib)
            calib_before = calibrate_clock(
                node, recorder, client_mqtt, topic, args, args.calib)

        # 重连可能重建 paho client，重新包一遍，保证目标请求的逐 broker 时间戳完整。
        wrap_client_publishes(net, recorder)
        print("[trace] 发送目标请求：code=%r topic=%s" % (args.code, topic))
        target_response = run_request(
            node, recorder, client_mqtt, args.code, topic, args.timeout,
            args.key, args.allow_no_pub, args.min_broker)
        # 必须立刻快照：后面的校准请求会覆盖 last_record。
        target_rec = recorder.record_for(target_response)
        # 连接状态在启动后仍在变化，报告里的“在线/离线”改用目标请求刚结束时
        # 的快照，避免“发给 6 个 broker，报告却写在线 1”。
        online_hosts, offline_hosts = snapshot_hosts(net)
        connect_info["online"] = len(online_hosts)
        connect_info["online_hosts"] = online_hosts
        connect_info["offline"] = offline_hosts
        print("[trace] 目标请求结束时在线 %d / 离线 %d" % (
            len(online_hosts), len(offline_hosts)))

        if not args.no_calib and args.calib > 0:
            print("[trace] 时钟校准（请求后 %d 条）..." % args.calib)
            calib_after = calibrate_clock(
                node, recorder, client_mqtt, topic, args, args.calib)

        clock = calib_before
        if clock.get("offset_ms") is None and calib_after.get("offset_ms") is not None:
            clock = calib_after
        elif (calib_after.get("offset_ms") is not None
              and calib_before.get("offset_ms") is not None):
            # 取前后两次里不确定度更小的那次
            if (calib_after.get("uncertainty_ms") or 1e9) < (calib_before.get("uncertainty_ms") or 1e9):
                clock = calib_after

        broker_acks = {}
        if args.ack_probe > 0:
            wrap_client_publishes(net, recorder)
            print("[trace] QoS1 PUBACK 探测（%d 轮）..." % args.ack_probe)
            broker_acks = broker_ack_probes(node, recorder, args.ack_probe, 1.5)

        server_probe = {}
        if args.server_path and target_response.get("server_from"):
            broker_host = target_response.get("server_from")
            port = (connect_info.get("ports") or {}).get(broker_host, 1883)
            print("[trace] server 侧反向探测：broker=%s:%s tracert=%s ..." % (
                broker_host, port, bool(args.server_tracert)))
            probe_code = build_remote_probe(
                broker_host, port,
                do_trace=bool(args.server_tracert),
                max_hops=args.server_hops)
            probe_resp = run_request(
                node, recorder, client_mqtt, probe_code, topic,
                max(args.timeout, 30.0), args.key, args.allow_no_pub, 0)
            probe_data = parse_last_json(probe_resp.get("stdout") or "")
            if probe_data is None:
                probe_data = parse_last_json(str(probe_resp.get("r") or ""))
            if probe_data:
                normalize_remote_hops(probe_data.get("tracert_hops"))
            server_probe = {
                "enabled": True,
                "broker": broker_host,
                "data": probe_data,
                "error": None if probe_data else (
                    probe_resp.get("error") or "server 未返回可解析 JSON"),
            }
            server_ip = (probe_data or {}).get("ip")
            if server_ip and server_ip != connect_info.get("local_ip"):
                print("[trace] client → server 直连 ping：%s ..." % server_ip)
                server_probe["client_to_server_ping"] = icmp_ping(server_ip, 4)
                if args.server_tracert:
                    server_probe["client_to_server_trace"] = traceroute(
                        server_ip, max_hops=args.hops, wait_ms=500)

        paths = []
        if not args.no_path:
            if args.path_all:
                with net.lock:
                    hosts = [h for h, c in net.clients.items() if c.is_connected()]
            else:
                hosts = []
                for host in (target_response.get("server_from"),
                             target_response.get("client_from")):
                    if host and host not in hosts:
                        hosts.append(host)
            if hosts:
                print("[trace] 客户端 → broker 路径探测：%s" % ", ".join(hosts))
                ports = connect_info.get("ports") or {}
                with concurrent.futures.ThreadPoolExecutor(
                        max_workers=max(1, min(4, len(hosts)))) as pool:
                    futures = {
                        host: pool.submit(probe_path, host, ports.get(host, 1883),
                                          True, args.hops, 600)
                        for host in hosts
                    }
                    for host, future in futures.items():
                        try:
                            paths.append(future.result(timeout=90))
                        except Exception as exc:
                            paths.append({"host": host, "error": repr(exc),
                                          "trace": {"available": False,
                                                    "reason": repr(exc)}})

        report = build_report(
            args, topic, node, recorder, target_response, clock,
            calib_before, calib_after, broker_acks, paths, server_probe,
            connect_info, target_rec)
        return report
    finally:
        try:
            node.stop()
        except Exception:
            pass


def main(argv=None):
    args = parse_args(argv)
    _enable_color(not args.no_color and sys.stdout.isatty())
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    if args.demo:
        report = demo_report()
    else:
        try:
            report = run_live(args)
        except KeyboardInterrupt:
            print("\n[trace] 已中断")
            return 130
        except Exception as exc:
            print("[trace] 运行失败：%r" % (exc,))
            traceback.print_exc()
            return 1

    print(render_console(report))

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2)
        print("[trace] JSON 报告已写入 %s" % os.path.abspath(args.json_out))
    if args.html:
        with open(args.html, "w", encoding="utf-8") as handle:
            handle.write(render_html(report))
        print("[trace] HTML 时间轴已写入 %s" % os.path.abspath(args.html))
    return 0


if __name__ == "__main__":
    sys.exit(main())
