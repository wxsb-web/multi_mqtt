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
import os
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

    def _volume(_set, _level, _stream):
        # 音量直接走 Java AudioManager，不依赖 feature_touch 是否暴露音量接口，
        # 与 feature_audio.build_volume_code 同一套调用（ActivityThread 取
        # Application → getSystemService(AUDIO_SERVICE)）。
        # 注意：部分 ROM（实测 ColorOS 的媒体流）会静默忽略 setStreamVolume——不报
        # 错、索引也不变，页面读回旧值（常是 0）就成了“滑条回弹 0”。这类系统只吃
        # 相对调节：回读确认绝对设置没生效后，退回 adjustStreamVolume（与音量键
        # 同一条路径）按真实步长逐格逼近目标，最后仍回读真实值。step 回传实际步长，
        # 供页面把滑条粒度对齐，避免 1 格拖动只走 1/10 步。
        try:
            from java import jclass as _jc
            _AT = _jc("android.app.ActivityThread")
            _Ctx = _jc("android.content.Context")
            _AM = _jc("android.media.AudioManager")
            _am = _AT.currentApplication().getSystemService(_Ctx.AUDIO_SERVICE)
            _max = int(_am.getStreamMaxVolume(_stream))
            _before = int(_am.getStreamVolume(_stream))
            _after = _before
            _step = None
            _target = _before
            if _set:
                _target = max(0, min(int(_level), _max))
                if _target != _before:
                    _am.setStreamVolume(_stream, _target, 0)
                    _after = int(_am.getStreamVolume(_stream))
                    if _after == _before:
                        # 绝对设置被系统吞掉：改用相对调节逐格逼近。
                        _dir = 1 if _target > _after else -1
                        for _ in range(200):
                            _am.adjustStreamVolume(_stream, _dir, 0)
                            _now = int(_am.getStreamVolume(_stream))
                            if _now == _after:      # 到底/到顶或系统不响应
                                break
                            if _step is None:
                                _step = abs(_now - _after)
                            _after = _now
                            if (_dir > 0 and _after >= _target) \
                                    or (_dir < 0 and _after <= _target):
                                break
            return {"ok": True, "op": "volume", "stream": _stream,
                    "level": _after, "max": _max, "before": _before,
                    "target": _target, "step": _step}
        except Exception as _exc:
            return {"ok": False, "op": "volume", "error": str(_exc)}

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
              "last_version": None, "stopped": None, "superseded": False}

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
            # 被新会话顶替时不要关共享的事件采集，否则新会话收不到事件。
            if not _state["superseded"]:
                _ftcall("event_capture", False)
            with _lock:
                _sessions.pop(_sid, None)

    def _snapshot():
        # 先记版本、再 dump、最后再记一次。dump 期间界面若还在变（切页/滑动
        # 动画），把“更新后的版本”当成已消费会让页面永久停在中间态：事件停
        # 了以后 version 不再变化，poll 就再也不会拍帧。两次版本不同则置
        # dirty，下一轮继续追平到稳定帧。
        _es0 = _ftcall("event_state")
        _v0 = _es0.get("version") if isinstance(_es0, dict) else None
        _state["dirty"] = False
        _w = _ftcall("windows")
        _d = _ftcall("dump_ui", _max_nodes, True)
        _es = _ftcall("event_state")
        _v1 = _es.get("version") if isinstance(_es, dict) else None
        _state["last_version"] = _v1 if _v0 is None else _v0
        if _v0 is not None and _v1 is not None and _v0 != _v1:
            _state["dirty"] = True
        _err = None
        for _part in (_d, _w, _es0, _es):
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
        _settle_at = 0.0
        _settle_delay = 0.6
        _catchup = 0
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
                _need = _state["dirty"] or (_v is not None and _v != _state["last_version"])
                _settle = False
                if not _need and _settle_at and _t.time() >= _settle_at:
                    _settle = True
                    _settle_at = 0.0
                    _need = True
                if _need:
                    _snapshot()
                    if _state["dirty"]:
                        # dump 期间界面还在变：继续追平；连续追平过多次就退让
                        # 2s，避免持续动画（视频列表/惯性滚动）一直 dump。
                        _catchup += 1
                        _settle_at = 0.0
                        if _catchup >= 8:
                            _catchup = 0
                            _settle_at = _t.time() + 2.0
                    elif not _settle:
                        # 切页/滑动结束后通常不再产生事件，补拍一帧静置后的
                        # 布局；只补一次，静止时不再 dump。
                        _catchup = 0
                        _settle_at = _t.time() + _settle_delay
                    else:
                        _catchup = 0
                        _settle_at = 0.0
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
                elif _type in ("volume", "volume_get"):
                    # volume=设置（拖动松手才发），volume_get=只读，用于页面初始化。
                    # 注意不能用键名 stream：上行帧的 stream 字段是会话 sid。
                    _res = _volume(_type == "volume",
                                   int(_fr.get("level", 0) or 0),
                                   int(_fr.get("audio_stream", 3) or 3))
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
        if not _state["superseded"]:
            _ftcall("event_capture", False)

    _threads = [_th.Thread(target=_poll_loop,
                           name="xime-ui-poll-%s" % _sid, daemon=True),
                _th.Thread(target=_in_loop,
                           name="xime-ui-in-%s" % _sid, daemon=True)]
    # PC 重启/刷新会带新 sid 开新会话，旧会话在 PC 那边已经没人用了，只靠
    # 60s TTL 回收。这段时间里多个孤儿会话会同时 300ms 轮询/dump，把手机端
    # 的无障碍调用拖住，连控制面 RPC 都排不上队（表现为下发会话代码超时）。
    # 新会话建立时直接把旧会话顶掉：停线程、取消订阅，不关共享事件采集。
    _stale = []
    with _lock:
        for _k, _v in list(_sessions.items()):
            if _k != _sid:
                _stale.append(_v)
                _sessions.pop(_k, None)
        _sessions[_sid] = {"end": _end, "threads": _threads,
                           "topics": [_in], "state": _state}
    for _v in _stale:
        _st = _v.get("state") if isinstance(_v, dict) else None
        if isinstance(_st, dict):
            _st["superseded"] = True
        try:
            _v["end"].set()
        except Exception:
            pass
        for _tp in _v.get("topics") or []:
            try:
                _net._cmq_ui_router.pop(_tp, None)
            except Exception:
                pass
            try:
                _un2 = getattr(_net, "unsubscribe", None)
                if callable(_un2):
                    _un2(_tp)
            except Exception:
                pass
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
                     "refresh", "wake", "back", "global_action",
                     "volume", "volume_get"):
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
            elif mtype in ("volume", "volume_get"):
                body.update({"level": int(msg.get("level", 0) or 0),
                             "audio_stream": int(msg.get("audio_stream", 3) or 3)})
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


_UI_PAGE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "ui.html")


def ui_html(p):
    """元素视图页面（HTTP RPC：``/ui_html(p)``，根路径自动跳转）。"""
    p.set_header("Content-Type", "text/html; charset=utf-8")
    p.set_header("Cache-Control", "no-store")
    with open(_UI_PAGE_FILE, "r", encoding="utf-8") as fp:
        p.set_data(fp.read())
ui=ui_html