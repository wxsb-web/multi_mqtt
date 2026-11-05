#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""xime_ui —— 浏览器 ↔ xime_mcp ↔ 手机 的实时元素视图数据面。

结构
====
- 浏览器 ↔ 本进程：WebSocket ``/uiws``（页面由 ``ui_html(p)`` 输出）；
- 本进程 ↔ 手机：控制面沿用原 MQTT request/response RPC；数据面新开一对
  topic（``xime/ui/<sid>/in`` 上行、``xime/ui/<sid>/out`` 下推），一次 RPC
  在服务端进程里打开会话，之后由设备无障碍事件版本号驱动 dump 推送。

多个标签页共享**一个**会话与一条 MQTT 连接（引用计数）；最后一个标签页
断开时发 stop 帧，远端另带 TTL 看门狗兜底。

本模块不包含任何设备连接信息：transport 与 RPC 配置全部由 ``configure()``
注入的 ``xime_mcp`` 回调读取 rpc.json 的 ``xime_mcp`` 段。
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
import uuid

logger = logging.getLogger("xime_mcp.ui")

WS_PATH = "/uiws"

# 由 xime_mcp 注入：控制面 RPC、配置读取、进程内事件记录。
_SEND_CODE = None       # (code, op, timeout, config) -> dict
_GET_CONFIG = None      # () -> dict
_LOG_EVENT = None       # (kind, **fields) -> None

_UI_MAX_NODES = 600
_SESSION_TTL = 60.0
_CAPTURE_MS = 20000
_POLL_MS = 300
_HEARTBEAT_SEC = 3.0


def configure(send_code=None, get_config_fn=None, log_event=None):
    """由 xime_mcp 在定义好 ``_send_device_code`` / ``get_config`` 后调用。"""
    global _SEND_CODE, _GET_CONFIG, _LOG_EVENT
    if send_code is not None:
        _SEND_CODE = send_code
    if get_config_fn is not None:
        _GET_CONFIG = get_config_fn
    if log_event is not None:
        _LOG_EVENT = log_event


def _event(kind, **fields):
    if _LOG_EVENT is None:
        return
    try:
        _LOG_EVENT(kind, **fields)
    except Exception:
        pass


# ----------------------------------------------------------------------------
# 远端会话模板：在手机 server_mqtt 进程里执行
# ----------------------------------------------------------------------------

_UI_SESSION_TEMPLATE = r'''
def _cmq_xime_ui_start():
    import json as _j, queue as _qe, threading as _th, time as _t
    _a = _j.loads(__PAYLOAD__)
    _sid = str(_a["sid"]); _in = str(_a["in_topic"]); _out = str(_a["out_topic"])
    _ttl = max(15.0, float(_a.get("ttl", 60)))
    _poll = min(max(0.1, float(_a.get("poll_ms", 300)) / 1000.0), 2.0)
    _max_nodes = max(60, int(_a.get("max_nodes", 600)))
    _capture_ms = max(3000, int(_a.get("capture_ms", 20000)))

    try:
        import feature_touch as _ft
    except Exception as _exc:
        return _j.dumps({"ok": False, "op": "ui_open",
                         "error": "import feature_touch failed: %s" % _exc},
                        ensure_ascii=False)

    def _pj(_v):
        if isinstance(_v, (dict, list)):
            return _v
        try:
            return _j.loads(_v)
        except Exception:
            return {"ok": False, "error": "bad device payload: %r" % (str(_v)[:160],)}

    def _ftcall(_name, *_args):
        # 兼容旧版 apk：缺 windows/event_state 等新接口时返回结构化错误，
        # 而不是让 snapshot 线程直接抛 AttributeError。
        _fn = getattr(_ft, _name, None)
        if _fn is None:
            return {"ok": False, "missing": _name,
                    "error": "feature_touch 缺少 %s()，请更新手机上的 xime apk" % _name}
        try:
            return _pj(_fn(*_args))
        except Exception as _exc:
            return {"ok": False, "error": "feature_touch.%s 失败: %s" % (_name, _exc)}

    def _wake():
        # 无障碍 API 不能点亮屏幕：用 PowerManager 的 SCREEN_BRIGHT_WAKE_LOCK +
        # ACQUIRE_CAUSES_WAKEUP（0x1000000A）。只亮屏，不清除密码锁屏。
        try:
            from java import jclass as _jc
            _app = _jc("com.chaquo.python.Python").getPlatform().getApplication()
            _pm = _app.getSystemService("power")
            _before = bool(_pm.isInteractive())
            if not _before:
                _pm.newWakeLock(0x1000000A, "xime:ui_wake").acquire(10000)
            return {"ok": True, "op": "wake", "interactive_before": _before,
                    "interactive": bool(_pm.isInteractive())}
        except Exception as _exc:
            return {"ok": False, "op": "wake", "error": str(_exc)}

    _g = globals()
    _lock = _g.get("_cmq_ui_sess_lock")
    if _lock is None:
        _lock = _th.Lock(); _g["_cmq_ui_sess_lock"] = _lock
    _sessions = _g.setdefault("_cmq_ui_sessions", {})
    with _lock:
        _old = _sessions.get(_sid)
    if _old is not None:
        return _j.dumps({"ok": True, "op": "ui_open", "sid": _sid,
                         "reused": True}, ensure_ascii=False)

    def _find_net():
        # 复用服务端进程里现成的 MQTT 网络层（xime 即 mqtt_server.mqtt_net）。
        for _v in list(_g.values()):
            _mn = getattr(_v, "mqtt_net", None)
            if _mn is not None and hasattr(_mn, "publish_broadcast"):
                _cb = getattr(_v, "handle_message", None)
                return _mn, (_cb if callable(_cb) else None)
        return None, None

    _net, _srv_cb = _find_net()
    if _net is None:
        return _j.dumps({"ok": False, "op": "ui_open",
                         "error": "服务端没有可复用的 MQTT 管理器"},
                        ensure_ascii=False)

    _end = _th.Event()
    _state = {"seq": 0, "dirty": True, "client_last": _t.time(),
              "last_version": None, "stopped": None}

    def _push(_obj):
        if _end.is_set():
            return
        try:
            _state["seq"] += 1
            _frame = {"stream": _sid, "kind": "ui", "seq": _state["seq"],
                      "server_t": int(_t.time() * 1000)}
            _frame.update(_obj)
            _net.publish_broadcast(_out, _frame)
        except Exception:
            pass

    def _router(_topic, _data, _broker):
        # UI topic 帧入队后不再下传；其余消息原样交给安装前的链底回调。
        _q = _net._cmq_ui_router.get(_topic)
        if _q is not None and isinstance(_data, dict):
            _q.put(_data)
            return None
        _orig = getattr(_net, "_cmq_ui_orig", None)
        if _orig is not None:
            return _orig(_topic, _data, _broker)
        return None

    # 独立幂等安装：只装一次，绝不回读自身作为链底，避免自调用递归。
    if not getattr(_net, "_cmq_ui_installed", False):
        _base = _srv_cb if _srv_cb is not None else _net.message_callback
        _net._cmq_ui_orig = _base
        _net._cmq_ui_router = {}
        _net._cmq_ui_installed = True
        _net.set_on_message(_router)

    _inq = _qe.Queue()
    _net._cmq_ui_router[_in] = _inq
    try:
        _net.subscribe(_in)
    except Exception as _exc:
        _net._cmq_ui_router.pop(_in, None)
        return _j.dumps({"ok": False, "op": "ui_open",
                         "error": "subscribe failed: %s" % _exc},
                        ensure_ascii=False)

    def _drop(_why):
        if _state["stopped"] is not None:
            return
        _state["stopped"] = _why
        try:
            _push({"type": "bye", "reason": _why})
        finally:
            _end.set()
            try:
                _net._cmq_ui_router.pop(_in, None)
            except Exception:
                pass
            try:
                _un = getattr(_net, "unsubscribe", None)
                if callable(_un):
                    _un(_in)
            except Exception:
                pass
            _ftcall("event_capture", False)
            with _lock:
                _sessions.pop(_sid, None)

    def _snapshot():
        _w = _ftcall("windows")
        _d = _ftcall("dump_ui", _max_nodes, True)
        _es = _ftcall("event_state")
        _state["last_version"] = _es.get("version") if isinstance(_es, dict) else None
        _state["dirty"] = False
        _err = None
        for _part in (_d, _w, _es):
            if isinstance(_part, dict) and _part.get("error"):
                _err = _part["error"]
                break
        _push({"type": "snapshot",
               "pkg": _d.get("pkg", "") if isinstance(_d, dict) else "",
               "count": _d.get("count") if isinstance(_d, dict) else 0,
               "nodes": (_d.get("nodes") or []) if isinstance(_d, dict) else [],
               "windows": (_w.get("windows") or []) if isinstance(_w, dict) else [],
               "version": _state["last_version"],
               "ok": bool(_d.get("ok")) if isinstance(_d, dict) else False,
               "error": _err or ("dump_ui failed" if not isinstance(_d, dict) else None)})

    def _poll_loop():
        _last_capture = 0.0
        if _ftcall("event_capture", True, _capture_ms).get("ok"):
            _last_capture = _t.time()
        try:
            _snapshot()
        except Exception as _exc:
            _push({"type": "error", "op": "snapshot", "error": str(_exc)})
        while not _end.is_set():
            _t.sleep(_poll)
            try:
                # 事件订阅有 ttl，会话存活期间周期续期。
                if _t.time() - _last_capture > max(3.0, _capture_ms / 2000.0):
                    if _ftcall("event_capture", True, _capture_ms).get("ok"):
                        _last_capture = _t.time()
                _es = _ftcall("event_state")
                _v = _es.get("version") if isinstance(_es, dict) else None
                if _state["dirty"] or (_v is not None and _v != _state["last_version"]):
                    _snapshot()
            except Exception:
                pass
            if _t.time() - _state["client_last"] > _ttl:
                _drop("ttl")
                return
        _drop(_state["stopped"] or "ended")

    def _in_loop():
        _last_iseq = None
        while not _end.is_set():
            try:
                _fr = _inq.get(timeout=0.5)
            except _qe.Empty:
                continue
            try:
                if not isinstance(_fr, dict) or _fr.get("stream") != _sid:
                    continue
                _iq = _fr.get("iseq")
                if _iq is not None:
                    _iq = int(_iq)
                    if _last_iseq is not None and _iq <= _last_iseq:
                        continue
                    _last_iseq = _iq
                _state["client_last"] = _t.time()
                _type = _fr.get("type")
                if _type in ("stop", "detach"):
                    _drop("client_stop")
                    return
                if _type == "ping":
                    _push({"type": "pong", "t": _fr.get("t")})
                    continue
                if _type == "refresh":
                    _state["dirty"] = True
                    _push({"type": "action_result", "action": "refresh",
                           "ok": True, "t": _fr.get("t")})
                    continue
                if _type == "back":
                    # 1 = GLOBAL_ACTION_BACK，与 MCP 的 global_action(1) 同一条路径
                    _res = _ftcall("global_action", 1)
                elif _type == "wake":
                    _res = _pj(_wake())
                elif _type == "tap":
                    _res = _ftcall("tap", float(_fr.get("x", 0)),
                                   float(_fr.get("y", 0)),
                                   int(_fr.get("duration_ms", 80)))
                elif _type == "swipe":
                    _res = _ftcall("swipe",
                                   float(_fr.get("x1", 0)),
                                   float(_fr.get("y1", 0)),
                                   float(_fr.get("x2", 0)),
                                   float(_fr.get("y2", 0)),
                                   int(_fr.get("duration_ms", 300)))
                elif _type == "set_text":
                    _ref = str(_fr.get("ref") or "")
                    _txt = str(_fr.get("text") or "")
                    _res = _ftcall("set_text", _ref, _txt)
                    if isinstance(_res, dict) and _res.get("ok") is False:
                        _ftcall("perform_action", _ref, "focus")
                        _res = _ftcall("set_text", _ref, _txt)
                elif _type == "perform_action":
                    _res = _ftcall("perform_action",
                                   str(_fr.get("ref") or ""),
                                   str(_fr.get("action") or "click"),
                                   str(_fr.get("text") or ""))
                else:
                    continue
                _state["dirty"] = True
                _push({"type": "action_result", "action": _type,
                       "ok": bool(_res.get("ok")) if isinstance(_res, dict) else False,
                       "error": _res.get("error") if isinstance(_res, dict) else None,
                       "result": _res, "t": _fr.get("t")})
            except Exception as _exc:
                _push({"type": "action_result",
                       "action": _fr.get("type") if isinstance(_fr, dict) else "?",
                       "ok": False, "error": str(_exc),
                       "t": _fr.get("t") if isinstance(_fr, dict) else None})
        _ftcall("event_capture", False)

    _threads = [_th.Thread(target=_poll_loop,
                           name="xime-ui-poll-%s" % _sid, daemon=True),
                _th.Thread(target=_in_loop,
                           name="xime-ui-in-%s" % _sid, daemon=True)]
    with _lock:
        _sessions[_sid] = {"end": _end, "threads": _threads, "topics": [_in]}
    for _t0 in _threads:
        _t0.start()
    return _j.dumps({"ok": True, "op": "ui_open", "sid": _sid,
                     "in_topic": _in, "out_topic": _out}, ensure_ascii=False)

r = _cmq_xime_ui_start()
'''


# ----------------------------------------------------------------------------
# 共享 transport（全进程一条 MQTT 连接）
# ----------------------------------------------------------------------------

_TRANSPORT = None
_TRANSPORT_LOCK = threading.Lock()


def _get_transport():
    global _TRANSPORT
    with _TRANSPORT_LOCK:
        if _TRANSPORT is None:
            from client.cmd_client_mqtt import MqttTransport

            config = _GET_CONFIG() if _GET_CONFIG is not None else {}
            _TRANSPORT = MqttTransport(
                request_topic=config.get("request_topic"),
                reply_topic=config.get("reply_topic") or "sys/device/response",
                private_key=config.get("private_key"),
                allow_no_pub=bool(config.get("allow_no_server_pubkey_response", True)),
            )
        return _TRANSPORT


# ----------------------------------------------------------------------------
# WS 客户端槽：每连接一个有界出站队列 + 写线程
# ----------------------------------------------------------------------------

class _ClientSlot:
    def __init__(self, connection):
        self.cid = uuid.uuid4().hex[:8]
        self.connection = connection
        self.q = queue.Queue(maxsize=64)
        self.alive = True
        self.thread = threading.Thread(target=self._write_loop,
                                       name="xime-ui-ws-%s" % self.cid,
                                       daemon=True)

    def start(self):
        self.thread.start()

    def send(self, obj):
        if not self.alive:
            return
        try:
            data = json.dumps(obj, ensure_ascii=False)
        except (TypeError, ValueError):
            return
        try:
            self.q.put_nowait(data)
        except queue.Full:
            # 慢客户端只保留最新帧，避免拖累 MQTT 分发线程。
            try:
                self.q.get_nowait()
            except queue.Empty:
                pass
            try:
                self.q.put_nowait(data)
            except queue.Full:
                pass

    def _write_loop(self):
        while self.alive:
            try:
                data = self.q.get(timeout=0.5)
            except queue.Empty:
                continue
            if data is None:
                break
            try:
                self.connection.send(data)
            except Exception:
                self.alive = False
                break

    def close(self):
        self.alive = False
        try:
            self.q.put_nowait(None)
        except queue.Full:
            pass


# ----------------------------------------------------------------------------
# UiHub：会话打开/关闭、帧去重、fan-out、延迟测量
# ----------------------------------------------------------------------------

class _Session:
    def __init__(self, sid, in_topic, out_topic):
        self.sid = sid
        self.in_topic = in_topic
        self.out_topic = out_topic
        self.seq = 0
        self.last_seq = 0
        self.ready = False
        self.opened_at = time.time()


class _UiHub:
    def __init__(self):
        self.lock = threading.RLock()
        self.clients = {}
        self.session = None
        self.opening = False
        self.iseq = 0
        self.pending = {}
        self.last_mqtt_rtt = None
        self.last_ping = 0.0
        self.shutdown_flag = False
        self._frame_handler = self._on_frame
        self._heartbeat = threading.Thread(target=self._heartbeat_loop,
                                           name="xime-ui-heartbeat",
                                           daemon=True)
        self._heartbeat.start()

    # ---- 客户端生命周期 ----

    def attach(self, connection):
        slot = _ClientSlot(connection)
        need_open = False
        with self.lock:
            self.clients[slot.cid] = slot
            if self.session is None and not self.opening:
                self.opening = True
                need_open = True
        slot.start()
        slot.send(self.hello())
        if need_open:
            threading.Thread(target=self._open_session,
                             name="xime-ui-open", daemon=True).start()
        return slot

    def detach(self, slot):
        slot.close()
        sess = None
        with self.lock:
            self.clients.pop(slot.cid, None)
            if not self.clients and self.session is not None:
                sess = self.session
        if sess is not None:
            threading.Thread(target=self._close_session, args=(sess,),
                             name="xime-ui-close", daemon=True).start()

    def hello(self):
        with self.lock:
            sess = self.session
            opening = self.opening
        if sess is not None and sess.ready:
            return {"type": "hello", "state": "ready", "sid": sess.sid,
                    "in_topic": sess.in_topic, "out_topic": sess.out_topic,
                    "mqtt_rtt_ms": self.last_mqtt_rtt}
        if opening:
            return {"type": "hello", "state": "opening"}
        return {"type": "hello", "state": "idle"}

    def _broadcast(self, obj):
        with self.lock:
            clients = list(self.clients.values())
        for slot in clients:
            slot.send(obj)

    # ---- 会话打开/关闭 ----

    def _open_session(self):
        sid = uuid.uuid4().hex[:12]
        in_topic = "xime/ui/%s/in" % sid
        out_topic = "xime/ui/%s/out" % sid
        sess = _Session(sid, in_topic, out_topic)
        with self.lock:
            self.session = sess
        transport = None
        try:
            if _SEND_CODE is None or _GET_CONFIG is None:
                raise RuntimeError("xime_ui 未 configure()，无法下发远端会话代码")
            transport = _get_transport()
            # 先订阅再开会话，避免漏掉远端首帧 snapshot。
            transport.stream_subscribe(out_topic, self._frame_handler)
            config = _GET_CONFIG() or {}
            payload = {"sid": sid, "in_topic": in_topic, "out_topic": out_topic,
                       "ttl": _SESSION_TTL, "poll_ms": _POLL_MS,
                       "max_nodes": _UI_MAX_NODES, "capture_ms": _CAPTURE_MS}
            code = _UI_SESSION_TEMPLATE.replace(
                "__PAYLOAD__", repr(json.dumps(payload, ensure_ascii=False)))
            result = _SEND_CODE(code, "ui_open", config.get("timeout"), config)
            if not (isinstance(result, dict) and result.get("ok")):
                raise RuntimeError((result or {}).get("error") or "远端会话打开失败")
            sess.ready = True
            _event("ui_open", sid=sid, in_topic=in_topic, out_topic=out_topic)
            self._broadcast({"type": "state", "state": "ready", "sid": sid,
                             "in_topic": in_topic, "out_topic": out_topic,
                             "elapsed_ms": result.get("elapsed_ms")})
        except Exception as exc:
            logger.warning("元素视图会话打开失败: %s: %s", type(exc).__name__, exc)
            _event("ui_open_error", error="%s: %s" % (type(exc).__name__, exc))
            with self.lock:
                if self.session is sess:
                    self.session = None
            if transport is not None:
                try:
                    transport.stream_unsubscribe(out_topic, self._frame_handler)
                except Exception:
                    pass
            self._broadcast({"type": "state", "state": "error",
                             "error": "%s: %s" % (type(exc).__name__, exc)})
        finally:
            empty = False
            with self.lock:
                self.opening = False
                empty = not self.clients
            if empty:
                self._close_session(sess)

    def _close_session(self, sess):
        with self.lock:
            if self.session is sess:
                self.session = None
        try:
            transport = _get_transport()
        except Exception:
            transport = None
        if transport is None:
            return
        try:
            if sess.ready:
                # 多 broker 重复投递：发多份 stop，远端 iseq 闸门只认第一份。
                for _ in range(3):
                    self._send_uplink(sess, {"type": "stop"}, track=False)
                    time.sleep(0.05)
            transport.stream_unsubscribe(sess.out_topic, self._frame_handler)
            _event("ui_close", sid=sess.sid)
            self._broadcast({"type": "state", "state": "idle"})
        except Exception as exc:
            logger.warning("元素视图会话关闭异常: %r", exc)

    # ---- 上行/下行 ----

    def _send_uplink(self, sess, body, track=True):
        t_ms = int(time.monotonic() * 1000)
        with self.lock:
            self.iseq += 1
            frame = {"stream": sess.sid, "kind": "ui",
                     "iseq": self.iseq, "t": t_ms}
            frame.update(body)
            if track:
                self.pending[t_ms] = (time.monotonic(), sess.sid)
                if len(self.pending) > 64:
                    for key in list(self.pending)[:-64]:
                        self.pending.pop(key, None)
        transport = _get_transport()
        transport.publish(sess.in_topic, frame)

    def _on_frame(self, data):
        """MQTT 分发线程回调：只做去重、fan-out，绝不同步写 WS。"""
        try:
            with self.lock:
                sess = self.session
            if sess is None or not isinstance(data, dict):
                return
            if data.get("stream") != sess.sid:
                return
            seq = data.get("seq")
            if seq is not None:
                seq = int(seq)
                if seq <= sess.last_seq:
                    return
                sess.last_seq = seq
            # 上行帧回显的 t 用来算一次完整 MQTT 往返（发布→远端处理→回推）。
            t_key = data.get("t")
            rtt = None
            if t_key is not None:
                with self.lock:
                    entry = self.pending.pop(t_key, None)
                if entry is not None:
                    started, sid = entry
                    if sid == sess.sid:
                        rtt = round((time.monotonic() - started) * 1000, 1)
                        self.last_mqtt_rtt = rtt
                        data["mqtt_rtt_ms"] = rtt
            if rtt is not None:
                self._broadcast({"type": "latency", "mqtt_rtt_ms": rtt})
            if data.get("type") == "bye":
                _event("ui_bye", sid=sess.sid, reason=data.get("reason"))
                with self.lock:
                    if self.session is sess:
                        self.session = None
            self._broadcast(data)
        except Exception:
            logger.exception("ui frame dispatch error")

    def on_client_msg(self, slot, msg):
        mtype = str(msg.get("type") or "")
        if mtype == "ws_ping":
            slot.send({"type": "ws_pong", "t": msg.get("t")})
            return
        if mtype == "reconnect":
            if self.session is None and not self.opening:
                with self.lock:
                    if self.session is None and not self.opening:
                        self.opening = True
                threading.Thread(target=self._open_session,
                                 name="xime-ui-open", daemon=True).start()
            else:
                slot.send(self.hello())
            return
        with self.lock:
            sess = self.session
        if sess is None or not sess.ready:
            slot.send({"type": "state", "state": "opening"})
            return
        if mtype in ("tap", "swipe", "set_text", "perform_action",
                     "refresh", "wake", "back"):
            body = {"type": mtype}
            if mtype == "swipe":
                body.update({"x1": msg.get("x1"), "y1": msg.get("y1"),
                             "x2": msg.get("x2"), "y2": msg.get("y2"),
                             "duration_ms": msg.get("duration_ms", 300)})
            elif mtype == "tap":
                body.update({"x": msg.get("x"), "y": msg.get("y"),
                             "duration_ms": msg.get("duration_ms", 80)})
            elif mtype == "set_text":
                body.update({"ref": msg.get("ref"), "text": msg.get("text", "")})
            elif mtype == "perform_action":
                body.update({"ref": msg.get("ref"), "action": msg.get("action"),
                             "text": msg.get("text", "")})
            try:
                self._send_uplink(sess, body)
            except Exception as exc:
                slot.send({"type": "action_result", "action": mtype,
                           "ok": False, "error": str(exc)})

    # ---- MQTT 延迟心跳 ----

    def _heartbeat_loop(self):
        while not self.shutdown_flag:
            time.sleep(1.0)
            now = time.monotonic()
            with self.lock:
                sess = self.session
                has_clients = bool(self.clients)
                due = now - self.last_ping >= _HEARTBEAT_SEC
            if sess is None or not sess.ready or not has_clients or not due:
                continue
            self.last_ping = now
            try:
                self._send_uplink(sess, {"type": "ping"})
            except Exception:
                pass

    def shutdown(self):
        self.shutdown_flag = True
        with self.lock:
            clients = list(self.clients.values())
            sess = self.session
            self.session = None
            self.clients.clear()
        for slot in clients:
            slot.close()
        if sess is not None:
            try:
                transport = _get_transport()
                for _ in range(3):
                    self._send_uplink(sess, {"type": "stop"}, track=False)
                    time.sleep(0.05)
                transport.stream_unsubscribe(sess.out_topic, self._frame_handler)
            except Exception:
                pass
        global _TRANSPORT
        with _TRANSPORT_LOCK:
            transport = _TRANSPORT
            _TRANSPORT = None
        if transport is not None:
            try:
                transport.close()
            except Exception:
                pass


_HUB = _UiHub()


# ----------------------------------------------------------------------------
# WebSocket / HTTP 页面入口
# ----------------------------------------------------------------------------

def ws_handler(connection, _handler=None):
    """server_http 原生 WebSocket 处理器（``/uiws``）。"""
    slot = _HUB.attach(connection)
    try:
        while True:
            text = connection.receive()
            if text is None:
                break
            try:
                msg = json.loads(text)
            except (TypeError, ValueError):
                continue
            if isinstance(msg, dict):
                _HUB.on_client_msg(slot, msg)
    except Exception:
        logger.exception("ui ws handler error")
    finally:
        _HUB.detach(slot)


def shutdown():
    _HUB.shutdown()


def ui_html(p):
    """元素视图页面（HTTP RPC：``/ui_html(p)``，根路径自动跳转）。"""
    p.set_header("Content-Type", "text/html; charset=utf-8")
    p.set_header("Cache-Control", "no-store")
    p.set_data(_UI_PAGE)


_UI_PAGE = r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>xime 元素视图</title>
<style>
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; overflow: hidden; background: #0b0f14;
  color: #d7e0ea; font: 13px/1.4 system-ui, -apple-system, "Segoe UI", sans-serif; }
#side { position: absolute; top: 0; right: 0; bottom: 0; width: 280px;
  display: flex; flex-direction: column; gap: 10px; padding: 14px 12px;
  border-left: 1px solid #1e2a36; background: #0e141b; overflow: auto; }
#side .name { font-weight: 600; color: #7ee787; white-space: nowrap; }
#conn { font-size: 12px; line-height: 1.5; color: #8ea3b7; word-break: break-all; }
#meta { font-size: 12px; line-height: 1.6; color: #8ea3b7; word-break: break-all; }
#lat { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; font-size: 12px; }
#lat div { background: #131b24; border: 1px solid #22303e; border-radius: 4px;
  padding: 6px 8px; }
#lat b { display: block; color: #d7e0ea; font-size: 14px; margin-top: 2px; }
button { background: #1b2733; color: #d7e0ea; border: 1px solid #2b3a4a;
  border-radius: 4px; padding: 6px 12px; cursor: pointer; font: inherit; width: 100%; }
button:hover { background: #243444; }
#editor { display: none; flex-direction: column; gap: 6px; padding: 8px;
  border: 1px solid #2b3a4a; border-radius: 4px; background: #131b24; }
#editor .ref { font-size: 11px; color: #8ea3b7; word-break: break-all; }
#editor input { width: 100%; background: #0b0f14; color: #d7e0ea;
  border: 1px solid #2b3a4a; border-radius: 4px; padding: 6px 8px; font: inherit; }
#editor .row { display: flex; gap: 6px; }
#stage { position: absolute; inset: 0 280px 0 0; display: flex; align-items: center;
  justify-content: center; padding: 8px; }
#ui { height: 100%; width: auto; max-width: 100%; background: #0f1620;
  border: 1px solid #22303e; border-radius: 2px; cursor: crosshair;
  touch-action: none; }
#ui rect:hover { stroke: #ffffff !important; stroke-width: 3px !important; }
#msg { padding: 6px 9px; border-radius: 4px; background: #131b24;
  border: 1px solid #2b3a4a; color: #d7e0ea; font-size: 12px; line-height: 1.5;
  word-break: break-all; }
#msg:empty { display: none; }
#msg.err { border-color: #a3342f; color: #ff9a94; }
#legend { margin-top: auto; display: flex; flex-direction: column; gap: 6px;
  padding-top: 10px; border-top: 1px solid #1e2a36; font-size: 11px;
  color: #8ea3b7; }
#legend span { display: flex; align-items: center; }
#legend i { display: inline-block; width: 11px; height: 11px; border-radius: 2px;
  margin-right: 4px; vertical-align: -1px; flex: none; }
#tostage { display: none; }
#toside { display: none; }
/* 手机 / 窄屏：元素视图整屏优先，控制侧栏排到右边，横向滑动页面查看 */
@media (max-width: 900px) {
  html { height: 100%; overflow-x: auto; overflow-y: hidden; }
  body { display: flex; height: 100vh; height: 100dvh; width: max-content;
    min-width: 100%; overflow: visible; }
  #stage { position: relative; inset: auto; flex: 0 0 100vw; width: 100vw;
    height: 100vh; height: 100dvh; padding: 6px; }
  #side { position: relative; top: auto; right: auto; bottom: auto;
    flex: 0 0 280px; width: 280px; height: 100vh; height: 100dvh; }
  #toside { display: block; position: fixed; top: 8px; right: 8px; width: auto;
    z-index: 9; padding: 6px 10px; opacity: .9; }
  #tostage { display: block; }
}
</style>
</head>
<body>
<button id="toside">控制 →</button>
<div id="stage"><svg id="ui" xmlns="http://www.w3.org/2000/svg"
  preserveAspectRatio="xMidYMid meet"></svg></div>
<aside id="side">
  <span class="name">xime 元素视图</span>
  <span id="conn">未连接</span>
  <button id="tostage">← 元素视图</button>
  <span id="meta">连接中…</span>
  <div id="lat">
    <div>WS 延迟<b id="ws-lat">—</b></div>
    <div>MQTT 延迟<b id="mqtt-lat">—</b></div>
  </div>
  <button id="refresh">刷新</button>
  <button id="back">返回键</button>
  <button id="wake">唤醒屏幕</button>
  <div id="editor">
    <span class="ref" id="ed-ref"></span>
    <input id="ed-input" type="text" placeholder="输入要写入的文本">
    <div class="row">
      <button id="ed-submit">提交</button>
      <button id="ed-cancel">取消</button>
    </div>
  </div>
  <div id="msg"></div>
  <div id="legend">
    <span><i style="background:linear-gradient(90deg,#4ea1ff,#39d0c4,#7ee787,#e3b341,#ff8a65,#c792ea)"></i>不同颜色=不同层级</span>
    <span><i style="background:transparent;border:2px solid #d7e0ea"></i>可点击(粗边)</span>
    <span><i style="background:#ff8a65"></i>可编辑</span>
    <span><i style="background:transparent;border:1px dashed #d7e0ea"></i>可滚动</span>
  </div>
</aside>
<script>
const NS = 'http://www.w3.org/2000/svg';
const PALETTE = ['#4ea1ff', '#39d0c4', '#7ee787', '#e3b341',
                 '#ff8a65', '#c792ea', '#f78fb3', '#9fb0c0'];
const svg = document.getElementById('ui');
const metaEl = document.getElementById('meta');
const msgEl = document.getElementById('msg');
const stateEl = document.getElementById('conn');
const wsLatEl = document.getElementById('ws-lat');
const mqttLatEl = document.getElementById('mqtt-lat');
const editorEl = document.getElementById('editor');
const edRefEl = document.getElementById('ed-ref');
const edInput = document.getElementById('ed-input');
let DATA = null;
let W = 0, H = 0;
let editing = null;
let ws = null, retry = 0, heartbeat = null;
let dragState = null;      // 按住拖动的起点/终点（设备坐标）
let dragLine = null;       // 拖动时的虚线反馈
let suppressClickUntil = 0;  // 拖动结束后短暂屏蔽 click，避免误发 tap

function el(tag, attrs) {
  const node = document.createElementNS(NS, tag);
  for (const key in attrs) node.setAttribute(key, attrs[key]);
  return node;
}

function shortClass(name) {
  const text = String(name || '');
  const parts = text.split('.');
  return parts[parts.length - 1] || text;
}

function labelOf(n) {
  return n.t || n.d || (n.id ? String(n.id).split('/').pop() : '')
         || shortClass(n.c) || '?';
}

function setMsg(text, isError) {
  msgEl.textContent = text;
  msgEl.className = isError ? 'err' : '';
}

function describe(n) {
  const flags = [];
  if (n.k) flags.push('可点击');
  if (n.e) flags.push('可编辑');
  if (n.s) flags.push('可滚动');
  if (n.f) flags.push('聚焦');
  const b = n.b || [0, 0, 0, 0];
  return shortClass(n.c) + '  [' + b.join(', ') + ']  ' + labelOf(n).slice(0, 40)
         + (flags.length ? '  ' + flags.join('/') : '');
}

function summary(data) {
  if (!data || !data.ok) {
    return '读取失败: ' + ((data && data.error) || '未知错误');
  }
  const parts = ['pkg=' + (data.pkg || '(未知)'),
                 '节点=' + (data.count !== undefined ? data.count : (data.nodes || []).length),
                 '窗口=' + ((data.windows || []).length),
                 '版本=' + (data.version !== undefined ? data.version : '?')];
  return parts.join(' · ');
}

function computeTree(nodes) {
  const stack = [];
  const depths = [];
  const hasChild = [];
  nodes.forEach(function (n, index) {
    const b = n.b;
    hasChild[index] = false;
    if (!b || b.length < 4) { depths.push(0); return; }
    const w = b[2] - b[0], h = b[3] - b[1];
    if (w <= 0 || h <= 0) { depths.push(0); return; }
    while (stack.length) {
      const t = nodes[stack[stack.length - 1]].b;
      const inside = b[0] >= t[0] - 1 && b[1] >= t[1] - 1
                  && b[2] <= t[2] + 1 && b[3] <= t[3] + 1;
      if (inside) break;
      stack.pop();
    }
    depths.push(stack.length);
    if (stack.length) hasChild[stack[stack.length - 1]] = true;
    stack.push(index);
  });
  return {depths: depths, hasChild: hasChild};
}

function openEditor(n) {
  editing = n;
  editorEl.style.display = 'flex';
  edRefEl.textContent = 'ref=' + (n.r || '(无)');
  edInput.value = n.t || '';
  edInput.focus();
  edInput.select();
  setMsg('编辑 ' + (n.r || '?') + '，回车提交');
}

function closeEditor() {
  editing = null;
  editorEl.style.display = 'none';
}

function submitEditor() {
  if (!editing) return;
  const ref = editing.r;
  if (!ref) {
    setMsg('该节点没有 ref，无法写入文本', true);
    return;
  }
  wsSend({type: 'set_text', ref: ref, text: edInput.value});
  setMsg('set_text → ' + ref);
  closeEditor();
}

function render(data) {
  DATA = data;
  W = 0; H = 0;
  dragState = null;
  dragLine = null;
  const nodes = (data && Array.isArray(data.nodes)) ? data.nodes : [];
  for (const n of nodes) {
    const b = n && n.b;
    if (!b || b.length < 4) continue;
    W = Math.max(W, b[2]);
    H = Math.max(H, b[3]);
  }
  W = W || 1080;
  H = H || 1920;
  while (svg.firstChild) svg.removeChild(svg.firstChild);
  svg.setAttribute('viewBox', '0 0 ' + W + ' ' + H);
  svg.setAttribute('width', W);
  svg.setAttribute('height', H);
  svg.appendChild(el('rect', {x: 0, y: 0, width: W, height: H, fill: '#0f1620'}));

  if (data && data.pkg) document.title = 'xime 元素视图 · ' + data.pkg;
  metaEl.textContent = summary(data);
  if (!data || !data.ok) {
    setMsg(summary(data), true);
    return;
  }
  if (!nodes.length) {
    const tip = el('text', {x: W / 2, y: H / 2, 'text-anchor': 'middle',
      'font-size': Math.max(18, Math.round(H / 30)), fill: '#8ea3b7',
      'font-family': 'system-ui, sans-serif'});
    tip.textContent = '未取到节点（界面可能不可访问或已锁屏）';
    svg.appendChild(tip);
    return;
  }

  const tree = computeTree(nodes);

  // 第一遍只画矩形：标签必须最后统一画，否则会被后绘制的子节点矩形盖住
  nodes.forEach(function (n, index) {
    const b = n.b;
    if (!b || b.length < 4) return;
    const x = b[0], y = b[1], w = b[2] - b[0], h = b[3] - b[1];
    if (w <= 0 || h <= 0) return;
    const color = PALETTE[Math.min(tree.depths[index], PALETTE.length - 1)];
    const clickable = !!n.k, editable = !!n.e, scrollable = !!n.s;
    const attrs = {
      x: x, y: y, width: w, height: h, 'rx': 3,
      fill: editable ? '#ff8a65' : color,
      'fill-opacity': clickable ? '0.22' : (editable ? '0.20' : '0.07'),
      stroke: editable ? '#ffb59c' : color,
      'stroke-opacity': clickable ? '0.95' : '0.60',
      'stroke-width': clickable ? '2' : '1',
      'vector-effect': 'non-scaling-stroke'
    };
    if (scrollable && !clickable) attrs['stroke-dasharray'] = '4 3';
    const rect = el('rect', attrs);
    rect.addEventListener('mouseenter', function () {
      metaEl.textContent = describe(n);
    });
    rect.addEventListener('click', function (ev) {
      ev.stopPropagation();
      if (performance.now() < suppressClickUntil) return;
      if (editable) {
        if (n.r) openEditor(n);
        else setMsg('该节点不可编辑（缺少 ref）', true);
        return;
      }
      sendTap(ev);
    });
    svg.appendChild(rect);
  });

  // 完全相同的 bounds 只让最深层节点出标签，避免多层同名文字压在一起
  const boxOwner = new Map();
  nodes.forEach(function (n, index) {
    const b = n.b;
    if (!b || b.length < 4) return;
    if (b[2] - b[0] <= 0 || b[3] - b[1] <= 0) return;
    const key = b.join(',');
    const prev = boxOwner.get(key);
    if (prev === undefined || tree.depths[index] >= tree.depths[prev]) {
      boxOwner.set(key, index);
    }
  });

  const labeledBoxes = new Set();
  const placedLabels = [];
  let fallback = -1;

  function drawLabel(n, index) {
    const b = n.b;
    const x = b[0], y = b[1], w = b[2] - b[0], h = b[3] - b[1];
    const fontSize = Math.min(26, Math.max(10, h / 2.2));
    if (w < fontSize * 2 || h < fontSize + 4) return false;
    const boxKey = b.join(',');
    if (labeledBoxes.has(boxKey)) return false;
    const maxChars = Math.max(1, Math.floor(
      Math.min(w - 8, W * 0.5) / (fontSize * 0.85)));
    let text = labelOf(n);
    if (text.length > maxChars) text = text.slice(0, maxChars - 1) + '…';

    const label = el('text', {
      x: x + 3, y: 0, 'font-size': fontSize,
      fill: '#ffffff', 'fill-opacity': '0.96', 'pointer-events': 'none',
      'font-family': 'ui-monospace, Consolas, monospace'
    });
    label.textContent = text;
    svg.appendChild(label);

    function measure() {
      try { return label.getComputedTextLength(); } catch (e) { return 0; }
    }
    const maxWidth = Math.min(w - 8, W * 0.5);
    let textWidth = measure();
    // 估算截断后仍可能超宽（中文比等宽字符宽），按真实测量再二分截断到框内
    if (textWidth > maxWidth) {
      let lo = 1, hi = text.length, best = 1;
      while (lo <= hi) {
        const mid = Math.floor((lo + hi) / 2);
        label.textContent = text.slice(0, mid) + '…';
        textWidth = measure();
        if (textWidth <= maxWidth) { best = mid; lo = mid + 1; }
        else { hi = mid - 1; }
      }
      label.textContent = text.slice(0, best) + '…';
      textWidth = measure();
    }
    const chipW = Math.min(w - 2, textWidth + 6);
    const chipH = fontSize * 1.32;
    const lineHeight = fontSize * 1.42;

    // 与已经放好的标签逐行碰撞检测：往下挪，挪不出本节点就放弃这个标签
    let labelTop = y + 2;
    while (labelTop + chipH <= y + h) {
      let clash = false;
      for (const p of placedLabels) {
        if (x + 1 < p.x + p.w && x + 1 + chipW > p.x
            && labelTop < p.y + p.h && labelTop + chipH > p.y) {
          clash = true;
          break;
        }
      }
      if (!clash) break;
      labelTop += lineHeight;
    }
    if (labelTop + chipH > y + h) {
      label.remove();
      return false;
    }
    label.setAttribute('y', labelTop + fontSize);
    placedLabels.push({x: x + 1, y: labelTop, w: chipW, h: chipH});
    labeledBoxes.add(boxKey);

    const chip = el('rect', {
      x: x + 1, y: labelTop, width: chipW, height: chipH,
      'rx': 2, fill: '#0b0f14', 'fill-opacity': '0.78',
      'pointer-events': 'none'
    });
    svg.insertBefore(chip, label);
    return true;
  }

  nodes.forEach(function (n, index) {
    const b = n.b;
    if (!b || b.length < 4) return;
    if (b[2] - b[0] <= 0 || b[3] - b[1] <= 0) return;
    if (boxOwner.get(b.join(',')) !== index) return;
    if (fallback < 0 || tree.depths[index] > tree.depths[fallback]) {
      fallback = index;
    }
    const hasText = !!(n.t || n.d || n.id);
    // 纯容器（无文本但有子节点）不标类名，让真正的叶子节点出标签
    if (!hasText && tree.hasChild[index]) return;
    drawLabel(n, index);
  });

  // 整页都是纯容器、一个标签都没有时，至少标出最深层节点
  if (labeledBoxes.size === 0 && fallback >= 0) {
    drawLabel(nodes[fallback], fallback);
  }
}

function toDevice(ev) {
  const matrix = svg.getScreenCTM();
  if (!matrix) return null;
  const point = svg.createSVGPoint();
  point.x = ev.clientX;
  point.y = ev.clientY;
  const local = point.matrixTransform(matrix.inverse());
  return {x: Math.round(local.x), y: Math.round(local.y)};
}

function marker(x, y) {
  const radius = Math.max(9, Math.round(W / 70));
  const dot = el('circle', {
    cx: x, cy: y, r: radius, fill: '#ffd166', 'fill-opacity': '0.9',
    stroke: '#ffffff', 'stroke-width': '2',
    'vector-effect': 'non-scaling-stroke', 'pointer-events': 'none'
  });
  svg.appendChild(dot);
  setTimeout(function () { dot.remove(); }, 1400);
}

function sendTap(ev) {
  const point = toDevice(ev);
  if (!point) return;
  if (point.x < 0 || point.y < 0 || point.x > W || point.y > H) return;
  marker(point.x, point.y);
  wsSend({type: 'tap', x: point.x, y: point.y});
  setMsg('tap ' + point.x + ', ' + point.y);
}

svg.addEventListener('mouseleave', function () {
  metaEl.textContent = summary(DATA);
});

svg.addEventListener('click', function (ev) {
  if (performance.now() < suppressClickUntil) return;
  sendTap(ev);
});

// ---- 按住拖动 = 手机侧 swipe（列表滚动 / 翻页） ----
svg.addEventListener('pointerdown', function (ev) {
  if (ev.button !== undefined && ev.button !== 0) return;
  const point = toDevice(ev);
  if (!point) return;
  dragState = {id: ev.pointerId, sx: ev.clientX, sy: ev.clientY,
               x1: point.x, y1: point.y, x2: point.x, y2: point.y,
               startedAt: performance.now(), moved: false};
  try { svg.setPointerCapture(ev.pointerId); } catch (e) {}
});

svg.addEventListener('pointermove', function (ev) {
  if (!dragState || ev.pointerId !== dragState.id) return;
  const dx = ev.clientX - dragState.sx;
  const dy = ev.clientY - dragState.sy;
  if (!dragState.moved && Math.abs(dx) < 8 && Math.abs(dy) < 8) return;
  const point = toDevice(ev);
  if (!point) return;
  dragState.moved = true;
  dragState.x2 = Math.max(0, Math.min(W, point.x));
  dragState.y2 = Math.max(0, Math.min(H, point.y));
  if (ev.cancelable) ev.preventDefault();
  if (!dragLine) {
    dragLine = el('line', {
      stroke: '#ffd166', 'stroke-width': '3', 'stroke-dasharray': '6 6',
      'stroke-opacity': '0.95', 'pointer-events': 'none',
      'vector-effect': 'non-scaling-stroke'
    });
    svg.appendChild(dragLine);
  }
  dragLine.setAttribute('x1', dragState.x1);
  dragLine.setAttribute('y1', dragState.y1);
  dragLine.setAttribute('x2', dragState.x2);
  dragLine.setAttribute('y2', dragState.y2);
  setMsg('swipe ' + dragState.x1 + ',' + dragState.y1 + ' → '
         + dragState.x2 + ',' + dragState.y2);
});

function endDrag(ev) {
  if (!dragState) return;
  if (ev && ev.pointerId !== undefined && ev.pointerId !== dragState.id) return;
  const state = dragState;
  dragState = null;
  if (dragLine) { dragLine.remove(); dragLine = null; }
  if (ev && ev.pointerId !== undefined) {
    try { svg.releasePointerCapture(ev.pointerId); } catch (e) {}
  }
  if (!state.moved) return;   // 位移不足阈值：交给 click 走 tap
  suppressClickUntil = performance.now() + 500;
  const duration = Math.min(1500,
      Math.max(150, Math.round(performance.now() - state.startedAt)));
  wsSend({type: 'swipe', x1: state.x1, y1: state.y1,
          x2: state.x2, y2: state.y2, duration_ms: duration});
  setMsg('swipe → ' + duration + 'ms');
}

svg.addEventListener('pointerup', endDrag);
svg.addEventListener('pointercancel', endDrag);

function wsSend(obj) {
  if (!ws || ws.readyState !== 1) {
    setMsg('WebSocket 未连接', true);
    return false;
  }
  ws.send(JSON.stringify(obj));
  return true;
}

function setState(text, isError) {
  stateEl.textContent = text;
  stateEl.style.color = isError ? '#ff9a94' : '';
}

function handle(msg) {
  if (!msg || typeof msg !== 'object') return;
  switch (msg.type) {
    case 'hello':
    case 'state': {
      if (msg.state === 'ready') {
        setState('已连接 · sid=' + (msg.sid || '?'));
      } else if (msg.state === 'error') {
        setState('连接失败: ' + (msg.error || '?'), true);
        setMsg('连接失败: ' + (msg.error || '?'), true);
      } else if (msg.state === 'opening') {
        setState('正在打开会话…');
      } else {
        setState('空闲');
      }
      if (msg.mqtt_rtt_ms !== undefined && msg.mqtt_rtt_ms !== null) {
        mqttLatEl.textContent = msg.mqtt_rtt_ms + ' ms';
      }
      break;
    }
    case 'ws_pong':
      if (msg.t !== undefined) {
        wsLatEl.textContent = Math.round(performance.now() - msg.t) + ' ms';
      }
      break;
    case 'snapshot':
      render(msg);
      break;
    case 'latency':
      if (msg.mqtt_rtt_ms !== undefined && msg.mqtt_rtt_ms !== null) {
        mqttLatEl.textContent = msg.mqtt_rtt_ms + ' ms';
      }
      break;
    case 'action_result': {
      const text = (msg.action || '?') + ' → ' + (msg.ok ? 'ok' : (msg.error || 'fail'));
      setMsg(text + (msg.mqtt_rtt_ms !== undefined ? ' · MQTT ' + msg.mqtt_rtt_ms + 'ms' : ''),
             !msg.ok);
      if (msg.mqtt_rtt_ms !== undefined && msg.mqtt_rtt_ms !== null) {
        mqttLatEl.textContent = msg.mqtt_rtt_ms + ' ms';
      }
      break;
    }
    case 'bye':
      setState('会话已结束: ' + (msg.reason || '?'));
      setMsg('会话已结束: ' + (msg.reason || '?'));
      break;
    case 'error':
      setMsg((msg.op || 'error') + ': ' + (msg.error || '?'), true);
      break;
  }
}

function connect() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(proto + '//' + location.host + '/uiws');
  ws.onopen = function () {
    retry = 0;
    setState('WebSocket 已连接，等待会话…');
    ws.send(JSON.stringify({type: 'reconnect'}));
    if (heartbeat) clearInterval(heartbeat);
    heartbeat = setInterval(function () {
      if (ws && ws.readyState === 1) {
        ws.send(JSON.stringify({type: 'ws_ping', t: performance.now()}));
      }
    }, 2000);
  };
  ws.onmessage = function (ev) {
    try { handle(JSON.parse(ev.data)); } catch (e) {}
  };
  ws.onclose = function () {
    if (heartbeat) { clearInterval(heartbeat); heartbeat = null; }
    setState('WebSocket 已断开，重连中…', true);
    const delay = Math.min(8000, 500 * Math.pow(2, retry++));
    setTimeout(connect, delay);
  };
  ws.onerror = function () {};
}

document.getElementById('refresh').addEventListener('click', function () {
  wsSend({type: 'refresh'});
});
document.getElementById('wake').addEventListener('click', function () {
  setMsg('唤醒屏幕…');
  wsSend({type: 'wake'});
});
document.getElementById('back').addEventListener('click', function () {
  setMsg('返回键…');
  wsSend({type: 'back'});
});
document.getElementById('toside').addEventListener('click', function () {
  document.getElementById('side').scrollIntoView(
      {behavior: 'smooth', inline: 'end', block: 'nearest'});
});
document.getElementById('tostage').addEventListener('click', function () {
  document.getElementById('stage').scrollIntoView(
      {behavior: 'smooth', inline: 'start', block: 'nearest'});
});
document.getElementById('ed-submit').addEventListener('click', submitEditor);
document.getElementById('ed-cancel').addEventListener('click', closeEditor);
edInput.addEventListener('keydown', function (ev) {
  if (ev.key === 'Enter') {
    ev.preventDefault();
    submitEditor();
  } else if (ev.key === 'Escape') {
    ev.stopPropagation();
    closeEditor();
  }
});

// 全局 Esc = 手机返回键；编辑框内的 Esc 已被上面拦下，只关闭编辑
document.addEventListener('keydown', function (ev) {
  if (ev.key !== 'Escape') return;
  ev.preventDefault();
  setMsg('返回键…');
  wsSend({type: 'back'});
});

connect();
</script>
</body>
</html>"""
