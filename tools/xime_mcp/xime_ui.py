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
_ADD_BROKERS = None     # (entries) -> dict  控制面本地 net 追加 broker

_UI_MAX_NODES = 600
_SESSION_TTL = 60.0
_CAPTURE_MS = 20000
_POLL_MS = 300
_HEARTBEAT_SEC = 3.0


def configure(send_code=None, get_config_fn=None, log_event=None,
              add_brokers=None):
    """由 xime_mcp 在定义好 ``_send_device_code`` / ``get_config`` 后调用。"""
    global _SEND_CODE, _GET_CONFIG, _LOG_EVENT, _ADD_BROKERS
    if send_code is not None:
        _SEND_CODE = send_code
    if get_config_fn is not None:
        _GET_CONFIG = get_config_fn
    if log_event is not None:
        _LOG_EVENT = log_event
    if add_brokers is not None:
        _ADD_BROKERS = add_brokers


def _parse_broker_spec(spec):
    """broker_add 条目 -> (host, port)；非法返回 None（不抛异常）。"""
    if isinstance(spec, (list, tuple)):
        try:
            return (str(spec[0]), int(spec[1]))
        except (TypeError, ValueError, IndexError):
            return None
    try:
        from rpc_config import parse_broker_add
        return parse_broker_add(spec)
    except Exception:
        return None


def _broker_key_list(entries):
    """broker 列表 -> ['host:port', ...]，忽略认证字段。"""
    keys = []
    for entry in entries or []:
        if isinstance(entry, (list, tuple)) and len(entry) >= 2:
            try:
                keys.append("%s:%d" % (entry[0], int(entry[1])))
            except (TypeError, ValueError):
                continue
        elif isinstance(entry, str) and entry:
            keys.append(entry)
    return keys


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
        # 接口缺失或调用失败都折叠成通用错误，由页面直接显示，不中断采集。
        try:
            return _pj(getattr(_ft, _name)(*_args))
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

    def _apply_brokers(_list):
        # 只改内存 broker 列表（update_brokers），绝不写 rpc.json。
        _cur = list(getattr(_net, "brokers", []) or [])
        _have = set()
        for _b in _cur:
            if isinstance(_b, (list, tuple)) and len(_b) >= 2:
                try:
                    _have.add((str(_b[0]).casefold(), int(_b[1])))
                except Exception:
                    pass
        _add = []
        for _e in _list or []:
            try:
                _h, _p = str(_e[0]), int(_e[1])
            except Exception:
                continue
            if (_h.casefold(), _p) not in _have:
                _add.append((_h, _p))
                _have.add((_h.casefold(), _p))
        if _add:
            _upd = getattr(_net, "update_brokers", None)
            if not callable(_upd):
                return {"ok": False, "op": "broker_add",
                        "error": "服务端网络层不支持 update_brokers"}
            _upd(_cur + _add)
        return {"ok": True, "op": "broker_add",
                "added": [[h, p] for h, p in _add],
                "brokers": [[str(b[0]), int(b[1])] for b in _cur + _add
                            if isinstance(b, (list, tuple)) and len(b) >= 2]}

    def _cur_brokers():
        _out = []
        for _b in list(getattr(_net, "brokers", []) or []):
            if isinstance(_b, (list, tuple)) and len(_b) >= 2:
                try:
                    _out.append([str(_b[0]), int(_b[1])])
                except Exception:
                    pass
        return _out

    # 无障碍全局动作清单（页面按键名 -> AccessibilityService 常量名）。
    # 存在性完全由 getattr 决定：旧系统没有的常量自然跳过，不写死 SDK 判断。
    _GA_NAMES = {
        "back": "GLOBAL_ACTION_BACK",
        "home": "GLOBAL_ACTION_HOME",
        "recents": "GLOBAL_ACTION_RECENTS",
        "notifications": "GLOBAL_ACTION_NOTIFICATIONS",
        "quick_settings": "GLOBAL_ACTION_QUICK_SETTINGS",
        "power_dialog": "GLOBAL_ACTION_POWER_DIALOG",
        "toggle_split_screen": "GLOBAL_ACTION_TOGGLE_SPLIT_SCREEN",
        "lock_screen": "GLOBAL_ACTION_LOCK_SCREEN",
        "take_screenshot": "GLOBAL_ACTION_TAKE_SCREENSHOT",
        "keycode_headsethook": "GLOBAL_ACTION_KEYCODE_HEADSETHOOK",
        "accessibility_button": "GLOBAL_ACTION_ACCESSIBILITY_BUTTON",
        "accessibility_button_chooser": "GLOBAL_ACTION_ACCESSIBILITY_BUTTON_CHOOSER",
        "accessibility_shortcut": "GLOBAL_ACTION_ACCESSIBILITY_SHORTCUT",
        "accessibility_all_apps": "GLOBAL_ACTION_ACCESSIBILITY_ALL_APPS",
        "dismiss_notification_shade": "GLOBAL_ACTION_DISMISS_NOTIFICATION_SHADE",
        "dpad_up": "GLOBAL_ACTION_DPAD_UP",
        "dpad_down": "GLOBAL_ACTION_DPAD_DOWN",
        "dpad_left": "GLOBAL_ACTION_DPAD_LEFT",
        "dpad_right": "GLOBAL_ACTION_DPAD_RIGHT",
        "dpad_center": "GLOBAL_ACTION_DPAD_CENTER",
    }

    def _keys():
        # 设备端真实可用的无障碍全局动作（名称 -> 编码），供页面列出按键。
        _out = {"sdk": None, "keys": {}}
        try:
            from java import jclass as _jcl
            _A = _jcl("android.accessibilityservice.AccessibilityService")
            for _name, _const in _GA_NAMES.items():
                try:
                    _out["keys"][_name] = int(getattr(_A, _const))
                except Exception:
                    pass
        except Exception as _exc:
            _out["error"] = str(_exc)
        try:
            from java import jclass as _jcl
            try:
                _out["sdk"] = int(_jcl("android.os.Build$VERSION").SDK_INT)
            except Exception:
                _out["sdk"] = int(_jcl("android.os.Build").VERSION.SDK_INT)
        except Exception:
            pass
        return _out

    _open_brokers = []
    if _a.get("brokers"):
        try:
            _open_brokers = _apply_brokers(_a.get("brokers")).get("brokers") or []
        except Exception:
            pass

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
        _wl = (_w.get("windows") or []) if isinstance(_w, dict) else []
        _nodes = (_d.get("nodes") or []) if isinstance(_d, dict) else []
        # 侧栏 pkg 用活动窗口：多窗口时 dump 的第一个节点通常是 systemui 状态栏，
        # 直接用它会让页面显示成当前应用是 systemui。
        _act = {}
        for _win in _wl:
            if isinstance(_win, dict) and _win.get("active"):
                _act = _win
                break
        # 节点带窗口 id（新版 apk）时，标出「一个有效节点都没读到」的窗口，
        # 典型是应用把无障碍节点树屏蔽了（根节点 class 空、bounds 为 0）。
        _readable = set()
        _has_w = False
        for _n in _nodes:
            if not isinstance(_n, dict) or _n.get("w") is None:
                continue
            _has_w = True
            _b = _n.get("b") or []
            if len(_b) >= 4 and _b[2] > _b[0] and _b[3] > _b[1]:
                _readable.add(_n.get("w"))
        _unread = []
        if _has_w:
            for _win in _wl:
                if isinstance(_win, dict) and _win.get("id") not in _readable:
                    _unread.append(str(_win.get("pkg") or "?"))
        _push({"type": "snapshot",
               "pkg": str(_act.get("pkg") or (_d.get("pkg", "") if isinstance(_d, dict) else "")),
               "title": str(_act.get("title") or ""),
               "count": _d.get("count") if isinstance(_d, dict) else 0,
               "nodes": _nodes,
               "windows": _wl,
               "unreadable": _unread,
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
                if _type == "broker_add":
                    _res = _apply_brokers(_fr.get("brokers") or [])
                    if isinstance(_res, dict):
                        # 仅按钮触发的这一次回包里带完整列表，其余帧只带数量。
                        _res["brokers"] = _cur_brokers()
                elif _type == "keys":
                    _res = _keys()
                    _res["ok"] = True
                    _res["op"] = "keys"
                elif _type == "global_action":
                    _res = _ftcall("global_action", int(_fr.get("action", 1)))
                elif _type == "back":
                    # 1 = GLOBAL_ACTION_BACK，与 MCP 的 global_action(1) 同一条路径
                    _res = _ftcall("global_action", 1)
                elif _type == "wake":
                    _res = _pj(_wake())
                elif _type == "tap":
                    _res = _ftcall("tap", float(_fr.get("x", 0)),
                                   float(_fr.get("y", 0)),
                                   int(_fr.get("duration_ms", 80)))
                elif _type == "long_press":
                    _res = _ftcall("long_press", float(_fr.get("x", 0)),
                                   float(_fr.get("y", 0)),
                                   int(_fr.get("duration_ms", 600)))
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
                     "in_topic": _in, "out_topic": _out,
                     "brokers": _open_brokers or _cur_brokers(),
                     "keys": _keys()}, ensure_ascii=False)

r = _cmq_xime_ui_start()
'''


# ----------------------------------------------------------------------------
# 共享 transport（全进程一条 MQTT 连接）
# ----------------------------------------------------------------------------

_TRANSPORT = None
_TRANSPORT_LOCK = threading.Lock()

# 页面运行期追加的 broker：只进内存，绝不写 rpc.json。
_RUNTIME_BROKERS = []
_RUNTIME_LOCK = threading.Lock()


def _runtime_broker_specs():
    with _RUNTIME_LOCK:
        return ["%s:%d" % entry for entry in _RUNTIME_BROKERS]


def add_brokers(specs):
    """页面「加入 Broker」：数据面 + 控制面的本地 net 都内存追加，并同步手机端。

    远端同步由下一次控制面 RPC（_send_device_code 里的幂等补丁）完成；如果
    会话已打开，这里再经上行帧即时补一次，避免等下一次工具调用。
    """
    entries = []
    if isinstance(specs, str):
        specs = [specs]
    for spec in specs or []:
        entry = _parse_broker_spec(spec)
        if entry is not None and entry not in entries:
            entries.append(entry)
    if not entries:
        return {"ok": False, "error": "broker_add 无合法条目（格式 host:port）"}

    added = []
    with _RUNTIME_LOCK:
        for entry in entries:
            if entry not in _RUNTIME_BROKERS:
                _RUNTIME_BROKERS.append(entry)
                added.append(entry)

    result = {"ok": True, "added": ["%s:%d" % entry for entry in added],
              "local": [], "control": None}
    try:
        transport = _get_transport()
        net = getattr(getattr(transport, "node", None), "mqtt_net", None)
        if net is not None and callable(getattr(net, "update_brokers", None)):
            cur = list(getattr(net, "brokers", []) or [])
            have = set()
            for item in cur:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    try:
                        have.add((str(item[0]).casefold(), int(item[1])))
                    except Exception:
                        pass
            fresh = [e for e in entries if (e[0].casefold(), e[1]) not in have]
            if fresh:
                net.update_brokers(cur + fresh)
                result["local"] = ["%s:%d" % e for e in fresh]
    except Exception as exc:
        result["local_error"] = str(exc)

    if _ADD_BROKERS is not None:
        try:
            result["control"] = _ADD_BROKERS(["%s:%d" % e for e in entries])
        except Exception as exc:
            result["control_error"] = str(exc)

    _event("broker_add", added=result["added"])
    return result


def _get_transport():
    global _TRANSPORT
    # 先取运行期追加列表再进 transport 锁：两把锁永不嵌套，避免锁序问题。
    runtime = _runtime_broker_specs()
    with _TRANSPORT_LOCK:
        if _TRANSPORT is None:
            from client.cmd_client_mqtt import MqttTransport

            config = _GET_CONFIG() if _GET_CONFIG is not None else {}
            _TRANSPORT = MqttTransport(
                request_topic=config.get("request_topic"),
                reply_topic=config.get("reply_topic") or "sys/device/response",
                private_key=config.get("private_key"),
                allow_no_pub=bool(config.get("allow_no_server_pubkey_response", True)),
                # broker_add 只在这里用于建连（内存），绝不写 rpc.json。
                broker_add=list(config.get("broker_add") or []) + runtime,
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
        self.broker_add = []      # 已同步给手机端的 broker（host, port）
        self.remote_brokers = []  # 手机端最近一次上报的实际 broker 列表
        self.device_keys = {}     # 手机端上报的可支持按键（name -> code）
        self.device_sdk = None    # 手机端 Android SDK_INT
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

    def _actual_brokers(self):
        """三端实际 broker 的 host:port 集合：控制面 / 数据面 / 手机端。"""
        control = []
        try:
            import client_mqtt as _cm
            net = getattr(getattr(_cm, "_default_client", None), "mqtt_net", None)
            control = _broker_key_list(getattr(net, "brokers", None))
        except Exception:
            control = []
        local = []
        transport = _TRANSPORT
        if transport is not None:
            net = getattr(getattr(transport, "node", None), "mqtt_net", None)
            local = _broker_key_list(getattr(net, "brokers", None))
        with self.lock:
            remote = _broker_key_list(self.remote_brokers)
        return control, local, remote

    def _broker_counts(self):
        """三端 broker 数量（只读内存）：控制面 / 数据面 / 手机端。

        只随页面打开与 broker_add 同步一次，避免每帧携带大列表。
        """
        control, local, remote = self._actual_brokers()
        return {"control": len(control), "local": len(local),
                "remote": len(remote)}

    def broker_status(self, specs):
        """按三端实际列表判断每个 broker_add 条目是否真的生效。"""
        control, local, remote = self._actual_brokers()
        items = []
        for spec in specs or []:
            entry = _parse_broker_spec(spec)
            if entry is None:
                continue
            key = "%s:%d" % entry
            items.append({"spec": key,
                          "control": key in control,
                          "local": key in local,
                          "remote": key in remote})
        return items

    def _cfg_view(self):
        """侧栏回显：request_topic、broker_add 实际状态、三端数量、设备按键。"""
        topic = None
        if _GET_CONFIG is not None:
            try:
                topic = (_GET_CONFIG() or {}).get("request_topic")
            except Exception:
                topic = None
        with self.lock:
            brokers = ["%s:%d" % (h, p) for h, p in self.broker_add]
            keys = dict(self.device_keys)
            keys_sdk = self.device_sdk
        view = {"topic": topic, "brokers": brokers, "keys": keys,
                "keys_sdk": keys_sdk, "broker_counts": self._broker_counts(),
                "broker_status": self.broker_status(brokers)}
        return view

    def hello(self):
        view = self._cfg_view()
        with self.lock:
            sess = self.session
            opening = self.opening
        if sess is not None and sess.ready:
            body = {"type": "hello", "state": "ready", "sid": sess.sid,
                    "in_topic": sess.in_topic, "out_topic": sess.out_topic,
                    "mqtt_rtt_ms": self.last_mqtt_rtt}
        elif opening:
            body = {"type": "hello", "state": "opening"}
        else:
            body = {"type": "hello", "state": "idle"}
        body.update(view)
        return body

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
            # 配置/CLI 里的 broker_add 也一并下发，手机端开场即接上同一批 broker
            seeds = [_parse_broker_spec(s) for s in config.get("broker_add") or []]
            with self.lock:
                for entry in seeds:
                    if entry is not None and entry not in self.broker_add:
                        self.broker_add.append(entry)
                brokers_payload = [[h, p] for h, p in self.broker_add]
            payload = {"sid": sid, "in_topic": in_topic, "out_topic": out_topic,
                       "ttl": _SESSION_TTL, "poll_ms": _POLL_MS,
                       "max_nodes": _UI_MAX_NODES, "capture_ms": _CAPTURE_MS,
                       "brokers": brokers_payload}
            code = _UI_SESSION_TEMPLATE.replace(
                "__PAYLOAD__", repr(json.dumps(payload, ensure_ascii=False)))
            result = _SEND_CODE(code, "ui_open", config.get("timeout"), config)
            if not (isinstance(result, dict) and result.get("ok")):
                raise RuntimeError((result or {}).get("error") or "远端会话打开失败")
            sess.ready = True
            remote = result.get("brokers")
            with self.lock:
                if isinstance(remote, list):
                    self.remote_brokers = [[b[0], b[1]] for b in remote
                                           if isinstance(b, (list, tuple)) and len(b) >= 2]
                keys = result.get("keys")
                if isinstance(keys, dict) and isinstance(keys.get("keys"), dict):
                    self.device_keys = keys["keys"]
                    if keys.get("sdk") is not None:
                        self.device_sdk = keys["sdk"]
            _event("ui_open", sid=sid, in_topic=in_topic, out_topic=out_topic)
            state = {"type": "state", "state": "ready", "sid": sid,
                     "in_topic": in_topic, "out_topic": out_topic,
                     "elapsed_ms": result.get("elapsed_ms")}
            state.update(self._cfg_view())
            self._broadcast(state)
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
            # 手机端 broker 实况只在 broker_add 回包里更新一次，帧内只带数量。
            if data.get("type") == "action_result" \
                    and data.get("action") == "broker_add":
                result = data.get("result")
                changed = False
                if isinstance(result, dict):
                    if isinstance(result.get("brokers"), list):
                        with self.lock:
                            self.remote_brokers = [
                                [b[0], b[1]] for b in result["brokers"]
                                if isinstance(b, (list, tuple)) and len(b) >= 2]
                        changed = True
                        result.pop("brokers", None)
                if changed:
                    self._broadcast({"type": "broker_counts",
                                     "broker_counts": self._broker_counts()})
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
        if mtype == "broker_check":
            # 以三端实际 broker 列表为准，不看按钮有没有点过；
            # 只回报页面传入的条目，内建 broker 不铺到界面上
            specs = msg.get("brokers")
            if isinstance(specs, str):
                specs = [specs]
            specs = [s for s in (specs or []) if str(s).strip()]
            slot.send({"type": "broker_status",
                       "items": self.broker_status(specs)})
            return
        if mtype == "broker_add":
            specs = msg.get("brokers")
            if isinstance(specs, str):
                specs = [specs]
            if not specs:
                specs = [msg["broker"]] if msg.get("broker") else []
            result = add_brokers(specs)
            entries = [e for e in (_parse_broker_spec(s) for s in specs) if e]
            with self.lock:
                for entry in entries:
                    if entry not in self.broker_add:
                        self.broker_add.append(entry)
                sess = self.session
            # 会话已打开时立刻经上行帧同步手机端（失败也不影响本地已生效）
            if entries and sess is not None and sess.ready:
                try:
                    self._send_uplink(sess, {"type": "broker_add",
                                             "brokers": [[h, p] for h, p in entries]})
                    result["remote_sent"] = True
                except Exception as exc:
                    result["remote_error"] = str(exc)
            payload = {"type": "action_result", "action": "broker_add",
                       "ok": bool(result.get("ok"))}
            payload.update({k: v for k, v in result.items() if k != "ok"})
            payload.update(self._cfg_view())
            slot.send(payload)
            return
        with self.lock:
            sess = self.session
        if sess is None or not sess.ready:
            slot.send({"type": "state", "state": "opening"})
            return
        if mtype in ("tap", "long_press", "swipe", "set_text", "perform_action",
                     "refresh", "wake", "back", "global_action"):
            body = {"type": mtype}
            if mtype == "global_action":
                body["action"] = int(msg.get("action", 1))
            if mtype == "swipe":
                body.update({"x1": msg.get("x1"), "y1": msg.get("y1"),
                             "x2": msg.get("x2"), "y2": msg.get("y2"),
                             "duration_ms": msg.get("duration_ms", 300)})
            elif mtype in ("tap", "long_press"):
                body.update({"x": msg.get("x"), "y": msg.get("y"),
                             "duration_ms": msg.get("duration_ms",
                                                    80 if mtype == "tap" else 600)})
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
<title>xime 辅助功能元素视图</title>
<style>
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; overflow: hidden; background: #0b0f14;
  color: #d7e0ea; font: 13px/1.4 system-ui, -apple-system, "Segoe UI", sans-serif;
  user-select: none; -webkit-user-select: none; }
body { display: flex; justify-content: center; }
#side { flex: 0 0 280px; position: relative; display: flex; flex-direction: column;
  gap: 10px; padding: 14px 12px; border-left: 1px solid #1e2a36;
  background: #0e141b; overflow: auto;
  user-select: text; -webkit-user-select: text; }
#side .name { font-weight: 600; color: #7ee787; white-space: nowrap; }
#conn { font-size: 12px; line-height: 1.5; color: #8ea3b7; word-break: break-all; }
#cfg { font-size: 12px; line-height: 1.5; color: #8ea3b7; word-break: break-all;
  white-space: pre-line; }
#meta { font-size: 12px; line-height: 1.6; color: #8ea3b7; word-break: break-all; }
#lat { display: flex; align-items: baseline; gap: 10px; font-size: 12px;
  color: #8ea3b7; white-space: nowrap; overflow: hidden; }
#lat span { display: inline-flex; align-items: baseline; gap: 4px; }
#lat b { color: #d7e0ea; font-size: 13px; }
button { background: #1b2733; color: #d7e0ea; border: 1px solid #2b3a4a;
  border-radius: 4px; padding: 6px 12px; cursor: pointer; font: inherit; width: 100%; }
button:hover { background: #243444; }
#editor { display: none; flex-direction: column; gap: 6px; padding: 8px;
  border: 1px solid #2b3a4a; border-radius: 4px; background: #131b24; }
#editor .ref { font-size: 11px; color: #8ea3b7; word-break: break-all; }
#editor input { width: 100%; background: #0b0f14; color: #d7e0ea;
  border: 1px solid #2b3a4a; border-radius: 4px; padding: 6px 8px; font: inherit; }
#editor .row { display: flex; gap: 6px; }
#broker { display: flex; flex-direction: column; gap: 6px; }
#broker input { width: 100%; background: #0b0f14; color: #d7e0ea;
  border: 1px solid #2b3a4a; border-radius: 4px; padding: 6px 8px; font: inherit; }
#labelcfg { display: flex; flex-direction: column; gap: 6px; }
#labelcfg .row { display: flex; align-items: center; justify-content: space-between;
  font-size: 12px; color: #8ea3b7; }
#labelcfg input[type=range] { width: 100%; accent-color: #4ea1ff; }
#labelcfg .chk { display: flex; align-items: center; gap: 6px; font-size: 12px;
  color: #8ea3b7; }
#labelcfg .chk input { width: auto; }
#keys { display: flex; flex-direction: column; gap: 6px; }
#keys .row { display: flex; align-items: center; justify-content: space-between;
  font-size: 12px; color: #8ea3b7; }
#keys-list { display: flex; flex-wrap: wrap; gap: 4px; font-size: 11px;
  color: #8ea3b7; }
#keys-list button { width: auto; padding: 3px 8px; font-size: 11px; }
#keys-list button.on { background: #1d3b52; border-color: #2f6f9f; }
#stage { flex: 0 1 auto; min-width: 0; display: flex; align-items: center;
  justify-content: center; padding: 8px; user-select: none;
  -webkit-user-select: none; }
#ui { max-height: 100%; max-width: 100%; height: auto; width: auto;
  background: #0f1620; border: 1px solid #22303e; border-radius: 2px;
  cursor: crosshair; touch-action: none; }
#ui rect:hover { stroke: #ffffff !important; stroke-width: 3px !important; }
#msg { padding: 6px 9px; border-radius: 4px; background: #131b24;
  border: 1px solid #2b3a4a; color: #d7e0ea; font-size: 12px; line-height: 1.5;
  word-break: break-all; }
#msg:empty { display: none; }
#msg.err { border-color: #a3342f; color: #ff9a94; }
#legend { display: flex; flex-direction: row; flex-wrap: wrap; gap: 6px 12px;
  padding-top: 10px; border-top: 1px solid #1e2a36; font-size: 11px;
  color: #8ea3b7; }
#side > * { flex: none; }
#foot { margin-top: auto; display: flex; flex-direction: column; gap: 6px;
  padding-top: 10px; border-top: 1px solid #1e2a36; }
#legend span { display: flex; align-items: center; }
#legend i { display: inline-block; width: 11px; height: 11px; border-radius: 2px;
  margin-right: 4px; vertical-align: -1px; flex: none; }
#tostage { display: none; }
#toside { display: none; }
/* 极窄（手机长屏）：元素视图与侧栏排不下才整屏滑动查看 */
@media (max-width: 480px) {
  html { height: 100%; overflow-x: auto; overflow-y: hidden; }
  body { display: flex; justify-content: flex-start; height: 100vh; height: 100dvh;
    width: max-content; min-width: 100%; overflow: visible; }
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
  <div id="lat">
    <span>延迟</span>
    <span>WS<b id="ws-lat">—</b></span>
    <span>MQTT<b id="mqtt-lat">—</b></span>
  </div>
  <button id="tostage">← 元素视图</button>
  <button id="refresh">刷新</button>
  <button id="back">返回键</button>
  <button id="wake">唤醒屏幕</button>
  <div id="broker">
    <input id="broker-input" type="text" autocomplete="off"
      placeholder="broker_add，如 192.168.1.20:1883">
    <button id="broker-add">加入 Broker</button>
  </div>
  <div id="labelcfg">
    <div class="row"><span>文本标签字号</span><b id="label-size-val">14</b></div>
    <input id="label-size" type="range" min="8" max="26" step="1" value="14">
    <div class="chk">
      <input id="label-show" type="checkbox" checked>
      <label for="label-show">显示文本标签</label>
    </div>
  </div>
  <div id="keys">
    <div class="row"><span>支持的按键</span><b id="keys-sdk">—</b></div>
    <div id="keys-list">连接后读取</div>
  </div>
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
  <div id="foot">
    <span id="conn">未连接</span>
    <span id="cfg">topic —</span>
    <span id="meta">连接中…</span>
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
const cfgEl = document.getElementById('cfg');
const wsLatEl = document.getElementById('ws-lat');
const mqttLatEl = document.getElementById('mqtt-lat');
const editorEl = document.getElementById('editor');
const edRefEl = document.getElementById('ed-ref');
const edInput = document.getElementById('ed-input');
const brokerInput = document.getElementById('broker-input');
// 浏览器本地记住已提交的 broker_add（不改变后端状态，仅回填输入框）。
const BROKER_KEY = 'xime.broker_add';
let savedBrokers = [];
try {
  const stored = JSON.parse(localStorage.getItem(BROKER_KEY) || '[]');
  if (Array.isArray(stored)) savedBrokers = stored.filter(Boolean);
} catch (e) { savedBrokers = []; }
brokerInput.value = savedBrokers.join(' ');
// 文本标签设置：字号下限与是否显示，浏览器本地记住。
const LABEL_KEY = 'xime.label';
const labelSizeEl = document.getElementById('label-size');
const labelSizeValEl = document.getElementById('label-size-val');
const labelShowEl = document.getElementById('label-show');
let labelMinSize = 14;
let labelVisible = true;
try {
  const cfg = JSON.parse(localStorage.getItem(LABEL_KEY) || 'null');
  if (cfg && typeof cfg === 'object') {
    if (cfg.size !== undefined) {
      labelMinSize = Math.max(8, Math.min(26, Number(cfg.size) || 14));
    }
    if (cfg.show !== undefined) labelVisible = !!cfg.show;
  }
} catch (e) {}
labelSizeEl.value = labelMinSize;
labelSizeValEl.textContent = labelMinSize;
labelShowEl.checked = labelVisible;
let labelTimer = null;
function saveLabelCfg() {
  try {
    localStorage.setItem(LABEL_KEY, JSON.stringify(
        {size: labelMinSize, show: labelVisible}));
  } catch (e) {}
}
function relabel() {
  if (!DATA) return;
  if (labelTimer) clearTimeout(labelTimer);
  labelTimer = setTimeout(function () { render(DATA); }, 150);
}
labelSizeEl.addEventListener('input', function () {
  labelMinSize = Number(labelSizeEl.value) || 14;
  labelSizeValEl.textContent = labelMinSize;
  saveLabelCfg();
  relabel();
});
labelShowEl.addEventListener('change', function () {
  labelVisible = !!labelShowEl.checked;
  saveLabelCfg();
  relabel();
});
window.addEventListener('resize', relabel);
let cfgTopic = '';
let cfgBrokers = [];
let cfgCounts = null;   // 三端 broker 数量：进页面 / broker_add 时同步一次
let cfgStatus = [];     // 各 broker 在三端的实际存在情况
// 侧栏：设备支持的按键（连上会话后由手机端反射上报）
const keysListEl = document.getElementById('keys-list');
const keysSdkEl = document.getElementById('keys-sdk');
let deviceKeys = {};

function renderKeys(payload) {
  if (!payload || typeof payload !== 'object') return;
  const names = (payload.keys && typeof payload.keys === 'object')
      ? payload.keys : payload;
  const sdk = payload.sdk !== undefined ? payload.sdk : payload.keys_sdk;
  if (sdk !== undefined && sdk !== null) keysSdkEl.textContent = 'SDK ' + sdk;
  deviceKeys = {};
  const entries = Object.keys(names).map(function (name) {
    return [name, Number(names[name])];
  }).filter(function (item) {
    return isFinite(item[1]);
  }).sort(function (a, b) {
    return a[1] - b[1];
  });
  entries.forEach(function (item) { deviceKeys[item[0]] = item[1]; });
  if (!entries.length) {
    keysListEl.textContent = '未上报（需先连上会话）';
    return;
  }
  keysListEl.textContent = '';
  entries.forEach(function (item) {
    const chip = document.createElement('button');
    chip.type = 'button';
    chip.textContent = item[0];
    chip.title = 'global_action(' + item[1] + ')';
    if (item[0].indexOf('dpad_') === 0) chip.className = 'on';
    chip.addEventListener('click', function () {
      setMsg('按键 ' + item[0] + ' → ' + item[1]);
      wsSend({type: 'global_action', action: item[1]});
    });
    keysListEl.appendChild(chip);
  });
}

let DATA = null;
let W = 0, H = 0;
let editing = null;
let ws = null, retry = 0, heartbeat = null;
let dragState = null;      // 按住拖动的起点/终点（设备坐标）
let dragLine = null;       // 拖动时的虚线反馈
let overlay = null;        // 持久覆盖层：快照刷新不销毁拖动指示线
let pressTimer = null;     // 长按判定定时器
let pressRing = null;      // 长按中的本地提示圈
let suppressClickUntil = 0;  // 拖动/长按结束后短暂屏蔽 click，避免误发 tap
const LONG_PRESS_MS = 450;   // 与 Android 长按阈值一致的判定时长

function raiseOverlay() {
  if (!overlay) overlay = el('g', {'pointer-events': 'none'});
  svg.appendChild(overlay);   // 已存在时自动从旧父节点移出，挂回最上层
  return overlay;
}

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
  const parts = ['pkg=' + (data.pkg || '(未知)')];
  if (data.title) parts.push('窗口=' + data.title);
  parts.push('节点=' + (data.count !== undefined ? data.count : (data.nodes || []).length));
  parts.push('窗口数=' + ((data.windows || []).length));
  if (data.version !== undefined) parts.push('版本=' + data.version);
  if (data.unreadable && data.unreadable.length) {
    parts.push('无节点=' + data.unreadable.join(','));
  }
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
  // 快照刷新只重绘显示层；拖动指示线放在覆盖层，最后再挂回最上层
  svg.setAttribute('viewBox', '0 0 ' + W + ' ' + H);
  svg.setAttribute('width', W);
  svg.setAttribute('height', H);
  // SVG 按 viewBox 等比缩放显示，字号必须按「屏幕像素」换算回 viewBox 单位，
  // 否则滑块调大在屏幕上几乎看不出变化（旧实现的 26 上限就是缩放后的小字）。
  const boxW = svg.clientWidth || 0, boxH = svg.clientHeight || 0;
  const labelScale = (boxW > 0 && boxH > 0)
      ? Math.max(0.05, Math.min(1, Math.min(boxW / W, boxH / H))) : 1;
  svg.appendChild(el('rect', {x: 0, y: 0, width: W, height: H, fill: '#0f1620'}));

  if (data && data.pkg) document.title = 'xime 元素视图 · ' + data.pkg;
  metaEl.textContent = summary(data);
  if (!data || !data.ok) {
    setMsg(summary(data), true);
    raiseOverlay();
    return;
  }
  if (!nodes.length) {
    const tip = el('text', {x: W / 2, y: H / 2, 'text-anchor': 'middle',
      'font-size': Math.max(18, Math.round(H / 30)), fill: '#8ea3b7',
      'font-family': 'system-ui, sans-serif'});
    tip.textContent = '未取到节点（界面可能不可访问或已锁屏）';
    svg.appendChild(tip);
    raiseOverlay();
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
    if (!labelVisible) return false;
    const b = n.b;
    const x = b[0], y = b[1], w = b[2] - b[0], h = b[3] - b[1];
    // 滑块值即屏幕上的字号（px）；节点太矮才按比例收缩，最小 9px。
    const nodeWpx = w * labelScale, nodeHpx = h * labelScale;
    const fontPx = Math.min(labelMinSize, Math.max(9, nodeHpx - 2.5));
    const fontSize = fontPx / labelScale;
    if (nodeWpx < fontPx * 2 || nodeHpx < fontPx + 2) return false;
    const boxKey = b.join(',');
    if (labeledBoxes.has(boxKey)) return false;
    const maxWidth = Math.max(fontSize, Math.min(w - 8, W * 0.5));
    const lineHeight = fontSize * 1.28;
    const maxLines = Math.max(1, Math.floor((h - 4) / lineHeight));
    const raw = String(labelOf(n));

    const label = el('text', {
      x: x + 3, y: 0, 'font-size': fontSize,
      fill: '#ffffff', 'fill-opacity': '0.96', 'pointer-events': 'none',
      'font-family': 'ui-monospace, Consolas, monospace'
    });
    svg.appendChild(label);

    function measure(str) {
      label.textContent = str;
      try { return label.getComputedTextLength(); } catch (e) { return 0; }
    }

    // 先在元素宽度内折行（二分找每行能放下的字符数），只有末行仍放不下才省略
    const lines = [];
    let pos = 0;
    while (pos < raw.length) {
      let fit = 0, lo = 1, hi = raw.length - pos;
      while (lo <= hi) {
        const mid = Math.floor((lo + hi) / 2);
        if (measure(raw.substr(pos, mid)) <= maxWidth) {
          fit = mid; lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      if (fit <= 0) fit = 1;           // 单个字符都放不下：强制放一个，避免死循环
      if (lines.length + 1 === maxLines) {
        // 最后一行：把剩余文字全部塞进来，塞不下再截断加省略号
        let rest = raw.substr(pos);
        if (measure(rest) > maxWidth) {
          let best = 0, l = 0, h = rest.length;
          while (l <= h) {
            const mid = Math.floor((l + h) / 2);
            if (measure(rest.slice(0, mid) + '…') <= maxWidth) {
              best = mid; l = mid + 1;
            } else {
              h = mid - 1;
            }
          }
          rest = rest.slice(0, best) + '…';
        }
        lines.push(rest);
        break;
      }
      lines.push(raw.substr(pos, fit));
      pos += fit;
    }

    const lineWidths = lines.map(function (line) { return measure(line); });
    const textWidth = Math.max.apply(null, lineWidths.concat([0]));
    label.textContent = '';
    lines.forEach(function (line, i) {
      const span = el('tspan', {x: x + 3});
      if (i > 0) span.setAttribute('dy', lineHeight);
      span.textContent = line;
      label.appendChild(span);
    });

    const chipW = Math.min(w - 2, textWidth + 6);
    const chipH = lines.length * lineHeight;

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
  raiseOverlay();
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
  raiseOverlay().appendChild(dot);
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

// ---- 按住拖动 = 手机侧 swipe；按住不动 = 手机侧 long_press ----
function clearPress() {
  if (pressTimer) { clearTimeout(pressTimer); pressTimer = null; }
  if (pressRing) { pressRing.remove(); pressRing = null; }
}

svg.addEventListener('pointerdown', function (ev) {
  if (ev.button !== undefined && ev.button !== 0) return;
  const point = toDevice(ev);
  if (!point) return;
  clearPress();
  dragState = {id: ev.pointerId, sx: ev.clientX, sy: ev.clientY,
               x1: point.x, y1: point.y, x2: point.x, y2: point.y,
               startedAt: performance.now(), moved: false, longPressed: false};
  try { svg.setPointerCapture(ev.pointerId); } catch (e) {}
  // 手指/鼠标按住不动到阈值，立刻发 long_press，让手机端出真实长按菜单
  pressTimer = setTimeout(function () {
    pressTimer = null;
    if (!dragState || dragState.moved) return;
    dragState.longPressed = true;
    const radius = Math.max(14, Math.round(W / 45));
    pressRing = el('circle', {
      cx: dragState.x1, cy: dragState.y1, r: radius,
      fill: 'none', stroke: '#ffd166', 'stroke-width': '3',
      'stroke-opacity': '0.95', 'pointer-events': 'none',
      'vector-effect': 'non-scaling-stroke'
    });
    raiseOverlay().appendChild(pressRing);
    wsSend({type: 'long_press', x: dragState.x1, y: dragState.y1,
            duration_ms: 600});
    setMsg('long_press ' + dragState.x1 + ', ' + dragState.y1);
  }, LONG_PRESS_MS);
});

svg.addEventListener('pointermove', function (ev) {
  if (!dragState || ev.pointerId !== dragState.id) return;
  const dx = ev.clientX - dragState.sx;
  const dy = ev.clientY - dragState.sy;
  if (!dragState.moved && Math.abs(dx) < 8 && Math.abs(dy) < 8) return;
  dragState.moved = true;
  clearPress();               // 移动了就不再是长按
  const point = toDevice(ev);
  if (!point) return;
  dragState.x2 = Math.max(0, Math.min(W, point.x));
  dragState.y2 = Math.max(0, Math.min(H, point.y));
  if (ev.cancelable) ev.preventDefault();
  if (!dragLine) {
    dragLine = el('line', {
      stroke: '#ffd166', 'stroke-width': '3', 'stroke-dasharray': '6 6',
      'stroke-opacity': '0.95', 'pointer-events': 'none',
      'vector-effect': 'non-scaling-stroke'
    });
    raiseOverlay().appendChild(dragLine);
  }
  dragLine.setAttribute('x1', dragState.x1);
  dragLine.setAttribute('y1', dragState.y1);
  dragLine.setAttribute('x2', dragState.x2);
  dragLine.setAttribute('y2', dragState.y2);
  setMsg('swipe ' + dragState.x1 + ',' + dragState.y1 + ' → '
         + dragState.x2 + ',' + dragState.y2);
});

function endDrag(ev) {
  clearPress();
  if (!dragState) return;
  if (ev && ev.pointerId !== undefined && ev.pointerId !== dragState.id) return;
  const state = dragState;
  dragState = null;
  if (dragLine) { dragLine.remove(); dragLine = null; }
  if (ev && ev.pointerId !== undefined) {
    try { svg.releasePointerCapture(ev.pointerId); } catch (e) {}
  }
  if (state.longPressed) {
    // 长按已在手机端执行，抬手不要再补一次 tap
    suppressClickUntil = performance.now() + 800;
    return;
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

function renderCfg(msg) {
  msg = msg || {};
  if (msg.topic !== undefined) cfgTopic = msg.topic || '';
  if (msg.brokers !== undefined) {
    cfgBrokers = Array.isArray(msg.brokers) ? msg.brokers : [];
  }
  if (msg.broker_status) cfgStatus = msg.broker_status;
  if (msg.broker_counts) cfgCounts = msg.broker_counts;
  let text = 'topic ' + (cfgTopic || '—');
  if (cfgCounts) {
    text += ' · broker 控制' + (cfgCounts.control || 0)
         + ' 数据' + (cfgCounts.local || 0)
         + ' 手机' + (cfgCounts.remote || 0);
  }
  // 只回显用户自己填过的 broker，三端实际状态压成一行 控制✓/数据✓/手机✓，
  // 内建 broker 列表不再铺开，保持界面紧凑
  (cfgStatus || []).forEach(function (it) {
    text += ' · ' + it.spec + ' ' + (it.control ? '控制✓' : '控制✗')
         + (it.local ? ' 数据✓' : ' 数据✗')
         + (it.remote ? ' 手机✓' : ' 手机✗');
  });
  cfgEl.textContent = text;
}

function checkBrokers() {
  // 只复核用户填过的 broker；没填就不查，避免把内建 broker 列表铺到界面上
  const list = savedBrokers.slice();
  if (list.length) wsSend({type: 'broker_check', brokers: list});
}

function handle(msg) {
  if (!msg || typeof msg !== 'object') return;
  switch (msg.type) {
    case 'hello':
    case 'state': {
      renderCfg(msg);
      renderKeys(msg);
      if (msg.state === 'ready') {
        setState('已连接 · sid=' + (msg.sid || '?'));
        checkBrokers();
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
    case 'broker_counts':
      if (msg.broker_counts) renderCfg(msg);
      // 手机端 broker_add 回包后立即按三端实际列表复核
      checkBrokers();
      break;
    case 'broker_status':
      cfgStatus = Array.isArray(msg.items) ? msg.items : [];
      renderCfg({});
      break;
    case 'action_result': {
      let text = (msg.action || '?') + ' → ' + (msg.ok ? 'ok' : (msg.error || 'fail'));
      if (msg.keys !== undefined) renderKeys(msg);
      if (msg.broker_counts) renderCfg(msg);
      if (msg.action === 'broker_add') {
        renderCfg(msg);
        const parts = [];
        if (msg.added && msg.added.length) parts.push('新增 ' + msg.added.join(','));
        if (msg.local && msg.local.length) parts.push('本地 ' + msg.local.join(','));
        if (msg.remote_sent) parts.push('已同步手机端');
        if (msg.remote_error) parts.push('手机端失败 ' + msg.remote_error);
        if (parts.length) text += ' · ' + parts.join(' · ');
      }
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
function submitBroker() {
  const raw = brokerInput.value.trim();
  if (!raw) return;
  const list = raw.split(/[\s,;]+/).filter(Boolean);
  wsSend({type: 'broker_add', brokers: list});
  setMsg('broker_add → ' + list.join(' '));
  list.forEach(function (item) {
    if (savedBrokers.indexOf(item) < 0) savedBrokers.push(item);
  });
  try { localStorage.setItem(BROKER_KEY, JSON.stringify(savedBrokers)); } catch (e) {}
  brokerInput.value = savedBrokers.join(' ');
}
document.getElementById('broker-add').addEventListener('click', submitBroker);
brokerInput.addEventListener('keydown', function (ev) {
  if (ev.key === 'Enter') {
    ev.preventDefault();
    submitBroker();
  } else if (ev.key === 'Escape') {
    ev.stopPropagation();
  }
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

// 全局按键：Esc = 手机返回键，方向键 = 无障碍 DPAD 全局动作（与 Esc 同样透传）。
// 焦点在输入框时不劫持，避免影响文本输入。
const DPAD_KEYS = {ArrowUp: 'dpad_up', ArrowDown: 'dpad_down',
                   ArrowLeft: 'dpad_left', ArrowRight: 'dpad_right'};
const DPAD_CODES = {ArrowUp: 16, ArrowDown: 17, ArrowLeft: 18, ArrowRight: 19};
document.addEventListener('keydown', function (ev) {
  const tag = ev.target && ev.target.tagName;
  if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
  if (ev.key === 'Escape') {
    ev.preventDefault();
    setMsg('返回键…');
    wsSend({type: 'back'});
    return;
  }
  const name = DPAD_KEYS[ev.key];
  if (!name) return;
  ev.preventDefault();
  const action = deviceKeys[name] !== undefined ? deviceKeys[name] : DPAD_CODES[ev.key];
  setMsg('按键 ' + name + ' → ' + action);
  wsSend({type: 'global_action', action: action});
});

connect();
</script>
</body>
</html>"""
