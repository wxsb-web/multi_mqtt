#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""oppo_adb —— 通过 MQTT RPC 驱动安卓侧辅助功能，动态开启无线调试并接入 adb。

本文件不包含任何设备信息：topic、私钥、broker、adb 路径、端口全部由命令行参数传入。

总体链路（服务端只要求手机里跑着 server_mqtt + feature_touch）：

    本机 adb  <-- 127.0.0.1:本地转发口 --+-- socks5_client_mqtt(127.0.0.1:socks_port)
                                       |
                                       +-- MQTT broker --> 手机 server_mqtt
                                                          --> 手机自身 localhost:<无线调试端口>

手机端动作全部走无障碍节点树（feature_touch.dump_ui / tap_text / tap）：

1. ``on``      ：拉起「开发者选项」列表，读「无线调试」行的 Switch 状态，关着就点开；
                 再进入详情页读「IP 地址和端口」里的调试端口。
2. ``pair``    ：进入详情页点「使用配对码配对设备」，读出 6 位配对码与配对端口，
                 立刻在本机执行 ``adb pair``（代码只在对话框打开期间有效）。
3. ``connect`` ：按当前调试端口建隧道并 ``adb connect``。
4. ``shell``   ：建隧道后直接 ``adb shell`` 执行传入命令。
5. ``off``     ：把「无线调试」开关关掉。
6. ``status``  ：只读巡检，返回当前界面文本与调试端口。

用法示例（参数按自己环境填）：

    python3 scripts/oppo_adb.py --request-topic <手机 topic> --key '<私钥表达式>' on
    python3 scripts/oppo_adb.py --request-topic <手机 topic> --key '<私钥表达式>' pair
    python3 scripts/oppo_adb.py --request-topic <手机 topic> --key '<私钥表达式>' connect
    python3 scripts/oppo_adb.py --request-topic <手机 topic> --key '<私钥表达式>' shell 'getprop ro.product.model'

--client-dir 指向 multi_mqtt 的 client 目录（默认自动按 ../multi_mqtt/client 探测），
--adb 指向 adb 可执行文件（默认走 PATH）。

注意：socks5 隧道带宽有限（MQTT 分帧），适合 shell/文件小操作，不要拿它传大包。
"""
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import struct
import subprocess
import sys
import threading
import time


# ============================== 通用工具 ==============================


def _find_client_dir(explicit: str | None) -> str:
    if explicit:
        return os.path.abspath(explicit)
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, "..", "..", "multi_mqtt", "client"),
        os.path.join(here, "..", "multi_mqtt", "client"),
        os.path.join(here, "..", "app", "src", "main", "python", "multi_mqtt", "client"),
    ]
    for path in candidates:
        if os.path.isfile(os.path.join(os.path.abspath(path), "client_mqtt.py")):
            return os.path.abspath(path)
    raise SystemExit(
        "找不到 multi_mqtt/client（可用 --client-dir 指定），已尝试：\n  "
        + "\n  ".join(os.path.abspath(p) for p in candidates)
    )


def _normalize(value):
    """不同版本的 client_mqtt 对回包 r 的处理不一致（有的给对象，有的给 repr 字符串）。"""
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except Exception:
        pass
    try:
        import ast

        return ast.literal_eval(value)
    except Exception:
        return value


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        return True
    except Exception:
        return False
    finally:
        s.close()


def _socks_open(proxy, host: str, port: int, timeout: float = 15.0, keepalive_timeout: float = 60.0):
    """走本地 SOCKS5 建立一条到 host:port 的 TCP 连接（域名解析在远端做）。"""
    s = socket.socket()
    s.settimeout(timeout)
    s.connect(proxy)
    s.sendall(b"\x05\x01\x00")
    if s.recv(2) != b"\x05\x00":
        raise RuntimeError("socks5 方法协商被拒绝")
    s.sendall(b"\x05\x01\x00\x01" + socket.inet_aton(host) + struct.pack(">H", port))
    rep = s.recv(4)
    if len(rep) < 4 or rep[1] != 0:
        raise RuntimeError("socks5 CONNECT 失败: %r" % (rep,))
    atyp = rep[3]
    if atyp == 1:
        s.recv(6)
    elif atyp == 3:
        s.recv(s.recv(1)[0] + 2)
    elif atyp == 4:
        s.recv(18)
    s.settimeout(keepalive_timeout)
    return s


class LocalForwarder:
    """本机监听口 → SOCKS5 → 远端固定 host:port；目标端口可在运行时改（无线调试端口会变）。"""

    def __init__(self, listen_port: int, sock_host: str, sock_port: int, target_host: str, target_port: int = 0):
        self._proxy = (sock_host, sock_port)
        self._target_host = target_host
        self._target_port = int(target_port or 0)
        self._listen_port = listen_port
        self._srv = None
        self._lock = threading.Lock()

    @property
    def listen_port(self) -> int:
        return self._listen_port

    def set_target_port(self, port: int) -> None:
        with self._lock:
            self._target_port = int(port)

    def _target(self) -> int:
        with self._lock:
            return self._target_port

    def start(self) -> None:
        if self._srv is not None:
            return
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", self._listen_port))
        srv.listen(20)
        self._srv = srv
        threading.Thread(target=self._loop, args=(srv,), daemon=True).start()

    def _loop(self, srv) -> None:
        while True:
            try:
                conn, _addr = srv.accept()
            except Exception:
                return
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn) -> None:
        try:
            port = self._target()
            if not port:
                raise RuntimeError("目标端口未设置")
            remote = _socks_open(self._proxy, self._target_host, port)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            return
        threading.Thread(target=self._pump, args=(remote, conn), daemon=True).start()
        self._pump(conn, remote)

    @staticmethod
    def _pump(src, dst) -> None:
        try:
            while True:
                data = src.recv(16384)
                if not data:
                    break
                dst.sendall(data)
        except Exception:
            pass
        finally:
            try:
                dst.shutdown(socket.SHUT_WR)
            except Exception:
                pass
            time.sleep(0.5)
            for x in (src, dst):
                try:
                    x.close()
                except Exception:
                    pass


# ============================== 手机端 RPC 代码 ==============================

# 公共片段：dump 节点、文本、开关定位。所有片段末尾都显式给 r 赋值，
# 保证返回的一定是本次执行结果（不使用命名空间残留值）。
_DEV_COMMON = r'''
import json, re, time, feature_touch

def _nodes(limit=600):
    return json.loads(feature_touch.dump_ui(limit, True)).get("nodes") or []

def _texts(limit=600):
    out = []
    for n in _nodes(limit):
        t = (n.get("t") or "").strip()
        if t and t not in out:
            out.append(t)
    return out

def _is_addr(v):
    return bool(re.fullmatch(r"(?:(?:\d{1,3}\.){3}\d{1,3}):\d{2,5}", v or ""))

def _port_of(v):
    return int(v.rsplit(":", 1)[1]) if _is_addr(v) else None

def _debug_port():
    ts = _texts()
    for i, t in enumerate(ts):
        if t == "IP 地址和端口" and i + 1 < len(ts) and _is_addr(ts[i + 1]):
            return _port_of(ts[i + 1])
    return None

def _in_detail():
    return "使用配对码配对设备" in _texts()

def _switch_near(label):
    """返回 label 文本行附近那个 Switch 节点（含当前状态文本 开启/关闭）。"""
    ns = _nodes()
    idx = None
    for i, n in enumerate(ns):
        if (n.get("t") or "").strip() == label:
            idx = i
            break
    if idx is None:
        return None
    for j in range(idx + 1, min(idx + 4, len(ns))):
        c = (ns[j].get("c") or "")
        if c.endswith("Switch"):
            l, t, rr, b = ns[j].get("b")
            return {"state": (ns[j].get("t") or "").strip(),
                    "x": int((l + rr) // 2), "y": int((t + b) // 2),
                    "bounds": ns[j].get("b")}
    return None

def _in_list():
    ts = _texts()
    return ("无线调试" in ts) and ("使用配对码配对设备" not in ts) and (_switch_near("无线调试") is not None)

def _open_list():
    if _in_list():
        return True
    # 详情页也有“无线调试”字样：先退回列表页，再拉起开发者选项
    feature_touch.global_action(1)
    time.sleep(0.9)
    feature_touch.start_activity("android.settings.APPLICATION_DEVELOPMENT_SETTINGS")
    for _ in range(30):
        time.sleep(0.4)
        if _in_list():
            return True
    return _in_list()

def _open_detail():
    if _in_detail():
        return True
    if not _open_list():
        return False
    d = json.loads(feature_touch.find_text("无线调试", 600, True))
    hits = d.get("hits") or []
    if not hits:
        return False
    feature_touch.tap(hits[0]["x"], hits[0]["y"], 90)
    for _ in range(20):
        time.sleep(0.4)
        if _in_detail():
            return True
        if _debug_port() is not None:
            return True
    return _in_detail()
'''

_DEV_ENSURE_ON = _DEV_COMMON + r'''
steps = {}
fast = _in_detail() and _debug_port() is not None
steps["fast_path"] = fast
if fast:
    steps["port"] = _debug_port()
else:
    if _open_list():
        sw = _switch_near("无线调试")
        steps["switch_before"] = sw
        if sw and sw["state"] != "开启":
            feature_touch.tap(sw["x"], sw["y"], 90)
            time.sleep(1.8)
            steps["switch_after"] = _switch_near("无线调试")
        steps["opened"] = _open_detail()
        steps["port"] = _debug_port() if _in_detail() else None
    else:
        steps["error"] = "developer_options_not_found"
steps["on"] = steps.get("port") is not None
r = steps
'''

_DEV_PAIR_INFO = _DEV_COMMON + r'''
steps = {}
"""打开配对码对话框并读出配对码/端口；保持对话框打开（配对码只在打开期间有效）。"""
steps["detail"] = _open_detail()
# 调试端口必须在配对对话框打开前读：对话框里“IP 地址和端口”指的是配对端口
steps["debug_port"] = _debug_port()
# 先关掉可能残留的失败弹窗与旧对话框
for label in ("确定", "取消"):
    d = json.loads(feature_touch.tap_text(label, 0, 600, True))
    if d.get("ok"):
        time.sleep(1.0)
steps["detail"] = _open_detail()
steps["debug_port"] = steps["debug_port"] or _debug_port()
d = json.loads(feature_touch.tap_text("使用配对码配对设备", 0, 600, True))
steps["entry"] = bool(d.get("ok"))
time.sleep(2.4)
ts = _texts()
steps["texts"] = ts
code = None
pair_port = None
for i, t in enumerate(ts):
    if t == "WLAN 配对码" and i + 1 < len(ts):
        v = ts[i + 1]
        if re.fullmatch(r"\d{6}", v):
            code = v
    if t == "IP 地址和端口" and i + 1 < len(ts) and _is_addr(ts[i + 1]):
        pair_port = _port_of(ts[i + 1])
steps["code"] = code
steps["pair_port"] = pair_port
r = steps
'''

_DEV_OFF = _DEV_COMMON + r'''
steps = {}
if not _open_list():
    steps["error"] = "developer_options_not_found"
else:
    sw = _switch_near("无线调试")
    steps["switch_before"] = sw
    if sw and sw["state"] == "开启":
        feature_touch.tap(sw["x"], sw["y"], 90)
        time.sleep(1.6)
        steps["switch_after"] = _switch_near("无线调试")
r = steps
'''

_DEV_STATUS = _DEV_COMMON + r'''
ts = _texts()
r = {"pkg": None, "texts": ts, "debug_port": _debug_port(), "in_detail": _in_detail(),
     "switch": _switch_near("无线调试"),
     "pair_dialog_open": ("WLAN 配对码" in ts)}
'''


# ============================== 控制器 ==============================


class OppoAdb:
    def __init__(self, args):
        self.args = args
        self.client_dir = _find_client_dir(args.client_dir)
        sys.path.insert(0, self.client_dir)
        import client_mqtt  # noqa: E402  仅在运行时导入
        self._client_mqtt = client_mqtt
        self.key = args.key
        self.topic = args.request_topic
        self.socks_port = args.socks_port
        self._socks_proc = None
        self._pair_fwd = LocalForwarder(args.pair_local_port, "127.0.0.1", self.socks_port, "127.0.0.1")
        self._adb_fwd = LocalForwarder(args.adb_local_port, "127.0.0.1", self.socks_port, "127.0.0.1")

    # ---- MQTT ----
    def rpc(self, code: str, timeout: float | None = None):
        kwargs = {"a": 1, "mb": 1}
        if self.key:
            kwargs["k"] = eval(self.key) if not self.key.lstrip("-").isdigit() else int(self.key)
        data = self._client_mqtt.rpc(code, self.topic, timeout=timeout or self.args.timeout, **kwargs)
        if not isinstance(data, dict):
            return {"ok": False, "error": "bad_reply:%r" % (data,)}
        data["r"] = _normalize(data.get("r"))
        return data

    # ---- 隧道 ----
    def ensure_socks(self) -> None:
        if _port_open("127.0.0.1", self.socks_port):
            return
        script = os.path.join(self.client_dir, "socks5_client_mqtt.py")
        if not os.path.isfile(script):
            raise SystemExit("缺少 socks5_client_mqtt.py：%s" % script)
        cmd = [
            sys.executable, "-u", script,
            "-t", self.topic, "-a",
            "-p", str(self.socks_port), "--rpc", "0",
        ]
        if self.key:
            cmd += ["-k", self.key]
        log_path = os.path.join(self.args.log_dir, "oppo_adb_socks5.log")
        os.makedirs(self.args.log_dir, exist_ok=True)
        log = open(log_path, "ab")
        self._socks_proc = subprocess.Popen(
            cmd, stdout=log, stderr=log, stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        deadline = time.time() + 30
        while time.time() < deadline:
            if _port_open("127.0.0.1", self.socks_port) and self._socks_ready():
                return
            if self._socks_proc.poll() is not None:
                raise SystemExit("socks5 客户端已退出，见日志：%s" % log_path)
            time.sleep(1.0)
        raise SystemExit("socks5 客户端 30s 内未就绪，见日志：%s" % log_path)

    def _socks_ready(self) -> bool:
        s = socket.socket()
        s.settimeout(0.5)
        try:
            s.connect(("127.0.0.1", self.socks_port))
            return True
        except Exception:
            return False
        finally:
            s.close()

    def ensure_forwarders(self) -> None:
        self._pair_fwd.start()
        self._adb_fwd.start()

    # ---- adb ----
    def adb(self, *argv, check: bool = False) -> subprocess.CompletedProcess:
        cmd = [self.args.adb] + list(argv)
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=self.args.adb_timeout)
        out = res.stdout.decode("utf-8", "replace")
        if check and res.returncode != 0:
            raise SystemExit("%s\n%s" % (" ".join(cmd), out))
        return res

    # ---- 动作 ----
    def action_on(self) -> dict:
        data = self.rpc(_DEV_ENSURE_ON)
        if not data.get("ok"):
            return data
        r = data.get("r") or {}
        port = r.get("port")
        if port:
            self._adb_fwd.set_target_port(port)
        return r

    def action_pair(self) -> dict:
        self.ensure_socks()
        self.ensure_forwarders()
        data = self.rpc(_DEV_PAIR_INFO)
        if not data.get("ok"):
            return data
        r = data.get("r") or {}
        code = r.get("code")
        pair_port = r.get("pair_port")
        if not code or not pair_port:
            r["ok"] = False
            r["error"] = "pair_code_or_port_missing"
            return r
        self._pair_fwd.set_target_port(pair_port)
        res = self.adb("pair", "127.0.0.1:%d" % self.args.pair_local_port, code)
        r["pair_output"] = (res.stdout or b"").decode("utf-8", "replace").strip()
        r["pair_ok"] = res.returncode == 0
        if r["pair_ok"] and r.get("debug_port"):
            self._adb_fwd.set_target_port(r["debug_port"])
        return r

    def action_connect(self) -> dict:
        self.ensure_socks()
        self.ensure_forwarders()
        data = self.rpc(_DEV_ENSURE_ON)
        if not data.get("ok"):
            return data
        r = data.get("r") or {}
        port = r.get("port")
        if not port:
            r["ok"] = False
            r["error"] = "wireless_debugging_off"
            return r
        self._adb_fwd.set_target_port(port)
        serial = "127.0.0.1:%d" % self.args.adb_local_port
        # 上一次进程退出后转发器已消失，adb 侧会残留 offline 条目：先断开再连
        state = self._device_state(serial)
        if state and state != "device":
            self.adb("disconnect", serial)
            time.sleep(0.5)
        res = self.adb("connect", serial)
        r["connect_output"] = (res.stdout or b"").decode("utf-8", "replace").strip()
        deadline = time.time() + self.args.connect_wait
        state = None
        while time.time() < deadline:
            state = self._device_state(serial)
            if state == "device":
                break
            time.sleep(0.5)
        devices = self.adb("devices").stdout.decode("utf-8", "replace")
        r["devices"] = devices.strip()
        r["serial"] = serial
        r["state"] = state
        r["connected"] = state == "device"
        return r

    def _device_state(self, serial: str):
        out = self.adb("devices").stdout.decode("utf-8", "replace")
        for line in out.splitlines():
            parts = line.split()
            if parts and parts[0] == serial:
                return parts[1] if len(parts) > 1 else ""
        return None

    def action_shell(self, command: str) -> dict:
        info = self.action_connect()
        if not info.get("connected"):
            return info
        serial = info["serial"]
        res = self.adb("-s", serial, "shell", command)
        info["shell_output"] = (res.stdout or b"").decode("utf-8", "replace")
        info["shell_rc"] = res.returncode
        return info

    def action_status(self) -> dict:
        data = self.rpc(_DEV_STATUS)
        return data.get("r") if data.get("ok") else data

    def action_off(self) -> dict:
        data = self.rpc(_DEV_OFF)
        return data.get("r") if data.get("ok") else data


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="通过 MQTT RPC 动态开启安卓无线调试并接入 adb（参数全部来自命令行）")
    p.add_argument("--request-topic", "-t", required=True, help="手机端 server_mqtt 的 request_topic")
    p.add_argument("--key", "-k", default="", help="请求签名私钥（整数表达式；留空不签名）")
    p.add_argument("--client-dir", default=None, help="multi_mqtt/client 目录，默认自动探测")
    p.add_argument("--adb", default="adb", help="adb 可执行文件路径，默认走 PATH")
    p.add_argument("--socks-port", type=int, default=10800, help="本地 SOCKS5 端口（默认 10800）")
    p.add_argument("--pair-local-port", type=int, default=16001, help="配对用本地转发端口（默认 16001）")
    p.add_argument("--adb-local-port", type=int, default=16002, help="调试用本地转发端口（默认 16002）")
    p.add_argument("--timeout", type=float, default=60.0, help="RPC 超时秒数（默认 60）")
    p.add_argument("--adb-timeout", type=float, default=120.0, help="单条 adb 命令超时秒数（默认 120）")
    p.add_argument("--connect-wait", type=float, default=20.0, help="adb connect 后等待设备就绪的秒数（默认 20）")
    p.add_argument("--log-dir", default="/tmp", help="socks5 客户端日志目录（默认 /tmp）")

    sub = p.add_subparsers(dest="action", required=True)
    sub.add_parser("on", help="确保无线调试开启并读取调试端口")
    sub.add_parser("pair", help="打开配对对话框并执行 adb pair")
    sub.add_parser("connect", help="确保开启并 adb connect")
    sub.add_parser("off", help="关闭无线调试")
    sub.add_parser("status", help="只读巡检")
    sp = sub.add_parser("shell", help="adb shell 执行命令")
    sp.add_argument("command", nargs=argparse.REMAINDER, help="要执行的命令")

    args = p.parse_args(argv)
    ctl = OppoAdb(args)

    if args.action == "on":
        result = ctl.action_on()
    elif args.action == "pair":
        result = ctl.action_pair()
    elif args.action == "connect":
        result = ctl.action_connect()
    elif args.action == "off":
        result = ctl.action_off()
    elif args.action == "status":
        result = ctl.action_status()
    else:
        command = " ".join(args.command).strip()
        if not command:
            p.error("shell 需要命令")
        result = ctl.action_shell(command)

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if isinstance(result, dict) and result.get("ok", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
