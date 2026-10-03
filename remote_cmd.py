#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""remote_cmd —— 与网络层无关的「远端 Shell / 文件编辑」命令封装层。

设计原则
========
1. **本模块只负责把各种操作包装成自包含 Python 代码（str），并解析回包。**
   网络怎么走由注入的 :class:`Transport` 决定（MQTT / HTTP / TCP / WebSocket …），
   换网络层时本文件一行都不用改——只需新写一个实现 ``request(code, timeout)``
   一问一答接口的 Transport。

2. **远端零改动**：远端只需要能执行 Python 字符串并返回
   ``{"ok": bool, "r": str, "stdout": str, "error"?: str}`` 形态的回包。

3. **报文是 JSON 文本协议，不是字节协议**（实测）：
   - 文本（含中文）直接放在 JSON 字符串里，无需任何编码；
   - 任意二进制用 latin-1 映射成 U+0000..U+00FF 字符承载（1 字节 ↔ 1 字符，
     远端 ``.encode('latin-1')`` 无损还原）；
   - 不能往报文里塞 Python bytes（json.dumps 直接 TypeError），也不能发绕过
     JSON 的原始 MQTT 二进制帧（对端 decode('utf-8') 会丢弃）。

4. **单报文 ≤ 1 MiB（MQTT broker 实际上限，实测 1000KiB 可过、1200KiB 被丢，
   且 512KiB 已需 4 秒）**。文件传输默认禁止超过 ``MAX_TRANSFER``（1 MiB），
   大文件必须在远端就地处理（grep/sed/编辑/下载/解压都在远端跑），见
   :class:`RemoteShell` 文档与 mqtt-remote-shell skill。
"""
from __future__ import annotations

import json
import os
import posixpath
import threading
import time
from abc import ABC, abstractmethod

# ---- 传输尺寸约束（均按实测的公共 broker 行为设定） --------------------------
MAX_TRANSFER = 1 << 20          # 1 MiB：单文件经本报文通道传输的默认硬上限
WIRE_BUDGET = 1 << 17          # 128 KiB：单条报文 JSON 转义后在线尺寸目标（实测 0.3s）
INLINE_MAX = 8192              # 命令输出小于该字节数时直接随回包返回
DEFAULT_TIMEOUT = 60           # 单次问答默认等待秒数
DEFAULT_STREAM_TOPIC = "sys/device/stream"  # 远端周期汇报的默认 topic


# ============================ 网络层抽象（更换协议时实现它） ============================

class Transport(ABC):
    """一问一答的代码执行通道。换网络层只需实现这两个方法。

    输入  : Python 代码字符串、超时秒数
    输出  : 原始回包 dict，至少形如
            ``{"ok": bool, "r": str|None, "stdout": str, "error"?: str}``
           （网络层自己的元信息如 latency/server_from 可以保留，本层不看）
    超时/通道故障：返回 ``None`` 或抛异常均可（本层把 None 映射为 RemoteTimeout）。
    """

    @abstractmethod
    def request(self, code: str, timeout: float = DEFAULT_TIMEOUT) -> dict | None:
        ...

    def close(self):
        """释放底层连接等资源；无资源可省略。"""

    # ---- 可选能力：服务端推送（周期汇报）。不实现则 RemoteShell.stream() 不可用 ----
    # 约定两个方法，签名：
    #   stream_subscribe(topic: str, handler: Callable[[dict], None]) -> None
    #   stream_unsubscribe(topic: str, handler) -> None
    # handler 收到的是推送消息解出的 dict；RemoteShell 按帧里的 stream/seq 自行
    # 过滤 sid 与多 broker 重复帧，transport 只负责"订阅 + 原样分发"。

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# ============================ 远端执行模板（自包含，禁止改名） ============================
# 远端持久命名空间里只占一个固定函数名 _cmq_dispatch；函数体内不得 print；
# 最后一条裸表达式调用，REPL 语义把返回的 JSON 字符串放进 r。
# 二进制一律 latin-1 字符承载，文本就是文本。
_REMOTE_TEMPLATE = r'''
def _cmq_dispatch():
    import json as _j, os as _o, sys as _s, time as _t, shutil as _sh
    import stat as _st, hashlib as _hl, tempfile as _tf, traceback as _tb
    import subprocess as _sp, errno as _en
    _a = _j.loads(__PAYLOAD__)
    _res = {}

    def _S(_x):
        # bytes -> 可入 JSON 的字符串：latin-1 对 0..255 一一映射，绝不损失
        return _x.decode("latin-1")

    def _B(_s):
        # JSON 字符串 -> bytes
        return _s.encode("latin-1")

    def _xp(_p):
        return _o.path.abspath(_o.path.expanduser(str(_p)))

    def _stash_dir():
        _d = _o.path.join(_tf.gettempdir(), "cmq_stash")
        _o.makedirs(_d, exist_ok=True)
        try:
            _now = _t.time()
            for _f in _o.listdir(_d):
                _fp = _o.path.join(_d, _f)
                try:
                    if _now - _o.path.getmtime(_fp) > 3600:
                        _o.remove(_fp)
                except Exception:
                    pass
        except Exception:
            pass
        return _d

    def _stash(_data):
        _fd, _fp = _tf.mkstemp(prefix="s", dir=_stash_dir())
        with _o.fdopen(_fd, "wb") as _fh:
            _fh.write(_data)
        return _o.path.basename(_fp)

    def _pack(_tag, _data, _thr):
        _res[_tag + "_size"] = len(_data)
        if len(_data) <= _thr:
            _res[_tag] = _S(_data)
        else:
            _res[_tag] = None
            _res[_tag + "_id"] = _stash(_data)

    def _finfo(_p, _st2):
        _m = _st2.st_mode
        _d = {"path": _p, "size": _st2.st_size,
              "mode": oct(_st.S_IMODE(_m)),
              "mtime": _st2.st_mtime, "uid": _st2.st_uid, "gid": _st2.st_gid,
              "isdir": _st.S_ISDIR(_m), "isfile": _st.S_ISREG(_m),
              "islink": _st.S_ISLNK(_m)}
        if _st.S_ISLNK(_m):
            try:
                _d["linkto"] = _o.readlink(_p)
            except Exception:
                _d["linkto"] = None
        return _d

    def _atomic_write(_dst, _data, _mode, _backup, _enc):
        _d2 = _o.path.dirname(_dst) or "."
        _old_mode = None
        if _o.path.exists(_dst):
            try:
                _old_mode = _st.S_IMODE(_o.lstat(_dst).st_mode)
            except Exception:
                pass
            if _backup:
                _sh.copy2(_dst, _dst + ".bak")
        _fd, _tmp = _tf.mkstemp(prefix=".cmq.", suffix=".part", dir=_d2)
        try:
            with _o.fdopen(_fd, "wb") as _fh:
                _fh.write(_data)
                _fh.flush()
                _o.fsync(_fh.fileno())
            try:
                _o.replace(_tmp, _dst)
            except OSError as _e:
                if _e.errno == _en.EXDEV:
                    _sh.move(_tmp, _dst)
                elif _e.errno == getattr(_en, "EBUSY", 16):
                    # Docker 绑定挂载文件（/etc/hostname|hosts|resolv.conf 等）
                    # 禁止 rename 替换：原地截断写、保留 inode（sed -i 同款坑）
                    with open(_dst, "wb") as _fh:
                        _fh.write(_data)
                        _fh.flush()
                        _o.fsync(_fh.fileno())
                    try:
                        _o.remove(_tmp)
                    except OSError:
                        pass
                else:
                    raise
            if _mode is not None:
                _mm = int(_mode, 8) if isinstance(_mode, str) else int(_mode)
                _o.chmod(_dst, _mm)
            elif _old_mode is not None:
                _o.chmod(_dst, _old_mode)
            elif _o.name == "posix":
                _o.chmod(_dst, 0o644)
        except Exception:
            try:
                _o.remove(_tmp)
            except OSError:
                pass
            raise
        _st3 = _o.stat(_dst)
        return _st3

    def _find_mqtt_net():
        # 复用服务端进程里现成的 MQTT 网络层（server_mqtt.py 的 gms.mqtt_net），
        # 不新建任何 broker 连接；找不到则不支持 stream。
        for _v in list(globals().values()):
            _mn = getattr(_v, "mqtt_net", None)
            if _mn is not None and hasattr(_mn, "publish_broadcast"):
                return _mn
        return None

    def _stats_once():
        # 纯标准库读 /proc 采一帧：整机 CPU%/内存/load/top8 进程。
        # 容器内 /proc 常是宿主视图，口径以远端内核为准。每帧 <1KB，适合秒级推送。
        import glob as _gb
        _page = _o.sysconf("SC_PAGE_SIZE") if hasattr(_o, "sysconf") else 4096

        def _cpu_total():
            with open("/proc/stat") as _fh:
                _v = [int(_x) for _x in _fh.readline().split()[1:]]
            return sum(_v), _v[3] + _v[4]

        def _snap():
            _out = []
            for _sd in _gb.glob("/proc/[0-9]*"):
                try:
                    _stt = open(_sd + "/stat", "rb").read().decode("ascii", "replace").split()
                    _pid = int(_stt[0])
                    _comm = _stt[1].strip("()")
                    _jif = int(_stt[13]) + int(_stt[14])
                    _rss = 0
                    try:
                        _rss = int(open(_sd + "/statm").read().split()[1]) * _page
                    except Exception:
                        pass
                    _out.append((_pid, _comm, _jif, _rss))
                except Exception:
                    pass
            return _out

        _t1, _i1 = _cpu_total()
        _p1 = _snap()
        _t.sleep(0.15)
        _t2, _i2 = _cpu_total()
        _p2 = _snap()
        _dt = max(_t2 - _t1, 1)
        _cpu_pct = round(100.0 * (_dt - (_i2 - _i1)) / _dt, 1)
        _mi = {}
        with open("/proc/meminfo") as _fh:
            for _ln in _fh:
                _k, _v = _ln.split(":", 1)
                _mi[_k] = int(_v.split()[0])
        _mt = _mi.get("MemTotal", 0)
        _ma = _mi.get("MemAvailable", 0)
        _load = open("/proc/loadavg").read().split()[:3]
        _old = {_x[0]: _x for _x in _p1}
        _rows = []
        for _pid, _comm, _j2, _rss2 in _p2:
            _q = _old.get(_pid)
            if _q is None:
                continue
            _pc = max(0.0, 100.0 * (_j2 - _q[2]) / _dt)
            _rows.append((round(_pc, 1), _pid, _comm, _rss2))
        _rows.sort(key=lambda _z: _z[0], reverse=True)
        return {"cpu_pct": _cpu_pct,
                "mem_total_kb": _mt, "mem_avail_kb": _ma,
                "mem_used_pct": round(100.0 * (_mt - _ma) / max(_mt, 1), 1),
                "load": [float(_x) for _x in _load],
                "procs": [{"pid": _pid, "comm": _comm, "cpu": _pc, "rss_kb": _rss}
                          for _pc, _pid, _comm, _rss in _rows[:8]]}

    def _stream_worker(_sid, _cfg, _net):
        # 后台守护线程：周期执行命令或采 /proc，每帧直接 publish 到汇报 topic。
        # 帧无 code 字段 → 网络层不签名，也不会被任何服务端当新命令回环执行。
        _ev = globals().get("_cmq_streams", {}).get(_sid, {}).get("event")
        _seq = 0
        _deadline = _t.time() + float(_cfg["ttl"])
        _reason = "count"
        try:
            while _ev is not None and not _ev.is_set():
                if _t.time() > _deadline:
                    _reason = "ttl"
                    break
                if int(_cfg["count"]) and _seq >= int(_cfg["count"]):
                    _reason = "count"
                    break
                _t0 = _t.time()
                _frame = {"stream": _sid, "seq": _seq, "ts": int(_t0 * 1000)}
                try:
                    if _cfg["mode"] == "stats":
                        _frame["stats"] = _stats_once()
                    else:
                        _gp2 = _sp.run(
                            ["/bin/sh", "-c", str(_cfg.get("cmd") or "top -b -n 1")],
                            stdout=_sp.PIPE, stderr=_sp.PIPE,
                            cwd=_xp(_cfg["cwd"]) if _cfg.get("cwd") else None,
                            timeout=float(_cfg["frame_timeout"]))
                        _frame["rc"] = _gp2.returncode
                        _frame["out"] = (_gp2.stdout or b"").decode("latin-1")
                        _frame["err"] = (_gp2.stderr or b"").decode("latin-1")
                except Exception as _e:
                    _frame["frame_error"] = repr(_e)
                try:
                    _net.publish_broadcast(_cfg["topic"], _frame)
                except Exception:
                    pass
                _seq += 1
                _rest = float(_cfg["interval"]) - (_t.time() - _t0)
                if _ev.wait(max(0.0, _rest)):
                    _reason = "stopped"
                    break
        except Exception:
            _reason = "error"
        finally:
            try:
                _net.publish_broadcast(_cfg["topic"],
                                       {"stream": _sid, "end": True,
                                        "reason": _reason, "frames": _seq})
            except Exception:
                pass
            globals().get("_cmq_streams", {}).pop(_sid, None)

    try:
        _op = _a.get("op")

        if _op == "info":
            import platform as _pf, getpass as _gp
            _u = _o.uname() if hasattr(_o, "uname") else None
            _res = {"ok": True,
                    "system": _pf.system(), "node": _pf.node(),
                    "machine": _pf.machine(), "release": _pf.release(),
                    "uname": (" ".join([_u.sysname, _u.nodename, _u.release,
                                        _u.version, _u.machine])) if _u else "",
                    "python": _s.version.split()[0],
                    "cwd": _o.getcwd(), "home": _o.path.expanduser("~"),
                    "uid": _o.getuid() if hasattr(_o, "getuid") else 0,
                    "gid": _o.getegid() if hasattr(_o, "getegid") else 0,
                    "user": _gp.getuser(),
                    "shell": _o.environ.get("SHELL", ""),
                    "env_PATH": _o.environ.get("PATH", "")}

        elif _op == "run":
            _thr = int(_a.get("inline_max", 8192))
            _cwd = _xp(_a["cwd"]) if _a.get("cwd") else None
            _env = None
            if isinstance(_a.get("env"), dict) and _a["env"]:
                _env = dict(_o.environ)
                for _k, _v in _a["env"].items():
                    if _v is None:
                        _env.pop(str(_k), None)
                    else:
                        _env[str(_k)] = str(_v)
            if _a.get("args") is not None:
                _cmd = [str(_x) for _x in _a["args"]]
            else:
                _exe = _a.get("shell") or ("/bin/sh" if _o.name == "posix" else "cmd.exe")
                _flag = "-c" if _o.name == "posix" else "/c"
                _cmd = [_exe, _flag, str(_a.get("cmd", ""))]
            _kw = {"stdout": _sp.PIPE, "stderr": _sp.PIPE, "cwd": _cwd}
            if _env is not None:
                _kw["env"] = _env
            _t0 = _t.time()
            if _o.name == "posix":
                _p = _sp.Popen(_cmd, preexec_fn=_o.setsid, **_kw)
            else:
                _p = _sp.Popen(_cmd, **_kw)
            try:
                _out, _err = _p.communicate(timeout=_a.get("timeout"))
                _res["timed_out"] = False
            except _sp.TimeoutExpired:
                try:
                    if _o.name == "posix":
                        _o.killpg(_p.pid, 9)
                    else:
                        _p.kill()
                except Exception:
                    _p.kill()
                _out, _err = _p.communicate()
                _res["timed_out"] = True
            _res["ok"] = True
            _res["rc"] = _p.returncode
            _res["dur"] = round(_t.time() - _t0, 3)
            _pack("out", _out or b"", _thr)
            _pack("err", _err or b"", _thr)

        elif _op == "read":
            _p = _xp(_a["path"])
            _off = int(_a.get("off", 0))
            _n = int(_a.get("n", 32768))
            with open(_p, "rb") as _fh:
                _fh.seek(0, 2)
                _total = _fh.tell()
                _fh.seek(_off)
                _chunk = _fh.read(_n)
            _res = {"ok": True, "path": _p, "size": _total,
                    "data": _S(_chunk), "n": len(_chunk),
                    "eof": _off + len(_chunk) >= _total}

        elif _op == "hash":
            _p = _xp(_a["path"])
            _algo = str(_a.get("algo", "sha256"))
            _h = _hl.new(_algo)
            with open(_p, "rb") as _fh:
                while True:
                    _c1 = _fh.read(1 << 20)
                    if not _c1:
                        break
                    _h.update(_c1)
            _res = {"ok": True, "path": _p, "algo": _algo,
                    "digest": _h.hexdigest(), "size": _o.path.getsize(_p)}

        elif _op == "push_init":
            _dst = _xp(_a["path"])
            _d2 = _o.path.dirname(_dst)
            if _a.get("parents", True):
                _o.makedirs(_d2 or ".", exist_ok=True)
            _fd, _part = _tf.mkstemp(prefix=".cmq.", suffix=".part", dir=_d2 or ".")
            _o.close(_fd)
            _res = {"ok": True, "part": _part}

        elif _op == "push_data":
            _part = str(_a["part"])
            _off = int(_a.get("off", 0))
            _data = _B(_a.get("data") or "")
            with open(_part, "rb+") as _fh:
                _cur = _fh.seek(0, 2)
                if _cur != _off:
                    raise RuntimeError("bad chunk offset: expect %d, got %d" % (_cur, _off))
                _fh.seek(_off)
                _fh.write(_data)
                _fh.flush()
                _o.fsync(_fh.fileno())
            if not _a.get("last"):
                _res = {"ok": True, "size": _off + len(_data)}
            else:
                _dst = _xp(_a["path"])
                _st3 = _atomic_write(
                    _dst, open(_part, "rb").read(), _a.get("mode"),
                    bool(_a.get("backup")), _a.get("encoding", "utf-8"))
                try:
                    _o.remove(_part)
                except OSError:
                    pass
                _h = _hl.sha256()
                with open(_dst, "rb") as _fh:
                    while True:
                        _c2 = _fh.read(1 << 20)
                        if not _c2:
                            break
                        _h.update(_c2)
                _res = {"ok": True, "path": _dst, "bytes": _st3.st_size,
                        "sha256": _h.hexdigest(), "mode": oct(_st.S_IMODE(_st3.st_mode))}

        elif _op == "push_abort":
            try:
                _o.remove(str(_a["part"]))
            except FileNotFoundError:
                pass
            _res = {"ok": True}

        elif _op == "fetch":
            _sid = _o.path.basename(str(_a["id"]))
            _fp = _o.path.join(_stash_dir(), _sid)
            _off = int(_a.get("off", 0))
            _n = int(_a.get("n", 32768))
            with open(_fp, "rb") as _fh:
                _fh.seek(0, 2)
                _total = _fh.tell()
                _fh.seek(_off)
                _chunk = _fh.read(_n)
            _res = {"ok": True, "data": _S(_chunk), "n": len(_chunk),
                    "size": _total, "eof": _off + len(_chunk) >= _total}

        elif _op == "drop":
            _sid = _o.path.basename(str(_a["id"]))
            try:
                _o.remove(_o.path.join(_stash_dir(), _sid))
            except FileNotFoundError:
                pass
            _res = {"ok": True}

        elif _op == "ls":
            _p = _xp(_a.get("path", "."))
            _items = []
            for _n2 in sorted(_o.listdir(_p)):
                _fp2 = _o.path.join(_p, _n2)
                _it = {"name": _n2}
                _it.update(_finfo(_fp2, _o.lstat(_fp2)))
                _items.append(_it)
            _res = {"ok": True, "path": _p, "items": _items}

        elif _op == "stat":
            _p = _xp(_a["path"])
            _res = {"ok": True}
            _res.update(_finfo(_p, _o.lstat(_p)))

        elif _op == "exists":
            _p = _xp(_a["path"])
            _res = {"ok": True, "path": _p, "exists": _o.path.exists(_p),
                    "lexists": _o.path.lexists(_p)}

        elif _op == "mkdir":
            _p = _xp(_a["path"])
            _o.makedirs(_p, exist_ok=bool(_a.get("exist_ok", True)))
            if _o.name == "posix" and _a.get("mode") is not None:
                _o.chmod(_p, int(_a["mode"], 8) if isinstance(_a["mode"], str)
                         else int(_a["mode"]))
            _res = {"ok": True, "path": _p}

        elif _op == "rm":
            _p = _xp(_a["path"])
            _rec = bool(_a.get("recursive", False))
            if not _o.path.lexists(_p):
                if not _a.get("missing_ok", True):
                    raise FileNotFoundError(_p)
            elif _st.S_ISDIR(_o.lstat(_p).st_mode) and not _st.S_ISLNK(_o.lstat(_p).st_mode):
                if _rec:
                    _sh.rmtree(_p)
                else:
                    raise IsADirectoryError(_p)
            else:
                _o.remove(_p)
            _res = {"ok": True, "path": _p}

        elif _op == "mv":
            _s2, _d3 = _xp(_a["src"]), _xp(_a["dst"])
            _sh.move(_s2, _d3)
            _res = {"ok": True, "src": _s2, "dst": _d3}

        elif _op == "cp":
            _s2, _d3 = _xp(_a["src"]), _xp(_a["dst"])
            if _o.path.isdir(_s2):
                _sh.copytree(_s2, _d3, dirs_exist_ok=True)
            else:
                _sh.copy2(_s2, _d3)
            _res = {"ok": True, "src": _s2, "dst": _d3}

        elif _op == "chmod":
            _p = _xp(_a["path"])
            _mode = int(_a["mode"], 8) if isinstance(_a["mode"], str) else int(_a["mode"])
            if _a.get("recursive") and _o.path.isdir(_p):
                for _root, _dirs, _files in _o.walk(_p):
                    try:
                        _o.chmod(_root, _mode)
                    except Exception:
                        pass
                    for _f3 in _files:
                        try:
                            _o.chmod(_o.path.join(_root, _f3), _mode)
                        except Exception:
                            pass
            else:
                _o.chmod(_p, _mode)
            _res = {"ok": True, "path": _p, "mode": oct(_st.S_IMODE(_o.stat(_p).st_mode))}

        elif _op == "grep":
            # 结构化远端 grep：-nH 输出 path:lineno:text，由客户端解析；
            # 搜索全程在远端完成，不回传文件内容。
            _gx = _sh.which("grep") or "grep"
            _ga = [_gx, "-nH", "--binary-files=without-match",
                   "-F" if _a.get("fixed") else "-E"]
            if _a.get("ignore_case"):
                _ga.append("-i")
            if _a.get("recursive", True):
                _ga.append("-r")
            for _inc in _a.get("include") or []:
                _ga += ["--include", str(_inc)]
            for _exc in _a.get("exclude") or []:
                _ga += ["--exclude", str(_exc)]
            _ga += ["-e", str(_a["pattern"])]
            _ga += [str(x) for x in (_a.get("paths") or ["."])]
            _gp = _sp.run(_ga, stdout=_sp.PIPE, stderr=_sp.PIPE,
                          cwd=_xp(_a["cwd"]) if _a.get("cwd") else None)
            _res = {"ok": True, "rc": _gp.returncode,
                    "out": _S(_gp.stdout or b""),
                    "err": _S(_gp.stderr or b"")}

        elif _op == "edit":
            # 远端就地文本编辑（精确字符串替换/追加），文件内容不离开远端。
            _p = _xp(_a["path"])
            _enc = str(_a.get("encoding", "utf-8"))
            _old = str(_a.get("old", ""))
            _new = str(_a.get("new", ""))
            with open(_p, "rb") as _fh:
                _raw = _fh.read()
            _txt = _raw.decode(_enc)
            if _a.get("append"):
                _txt2, _hit, _cnt = _txt + _new, 0, 0
            else:
                _cnt = _txt.count(_old)
                if _cnt == 0:
                    raise RuntimeError("pattern not found in file: %s" % _p)
                _lim0 = int(_a.get("count", 0))
                _lim = _lim0 if _lim0 > 0 else _cnt
                _txt2 = _txt.replace(_old, _new, _lim)
                _hit = min(_lim, _cnt)
            _data = _txt2.encode(_enc)
            _st3 = _atomic_write(_p, _data, _a.get("mode"),
                                 bool(_a.get("backup")), _enc)
            _res = {"ok": True, "path": _p, "matches": _cnt if not _a.get("append") else 0,
                    "replaced": _hit, "bytes": _st3.st_size}

        elif _op == "stream_start":
            # 远端后台周期汇报：daemon 线程复用 gms.mqtt_net 推到独立 topic。
            import threading as _th
            _ss2 = globals().setdefault("_cmq_streams", {})
            _net = _find_mqtt_net()
            if _net is None:
                raise RuntimeError(
                    "remote has no reusable MQTT manager (expected gms.mqtt_net)")
            _sid = str(_a.get("sid") or ("cmq-%d" % int(_t.time() * 1000)))
            if _sid in _ss2:
                raise RuntimeError("stream sid already active: %s" % _sid)
            _cfg = {"topic": str(_a.get("topic") or "sys/device/stream"),
                    "interval": max(0.2, float(_a.get("interval", 1.0))),
                    "count": max(0, int(_a.get("count", 0))),
                    "ttl": min(max(5.0, float(_a.get("ttl", 60.0))), 1800.0),
                    "mode": "stats" if _a.get("mode") == "stats" else "cmd",
                    "cmd": _a.get("cmd"),
                    "cwd": _a.get("cwd"),
                    "frame_timeout": float(_a.get("frame_timeout", 20.0))}
            _ev = _th.Event()
            _ss2[_sid] = {"event": _ev, "cfg": _cfg, "started": _t.time()}
            _th.Thread(target=_stream_worker, args=(_sid, _cfg, _net),
                       daemon=True).start()
            _res = {"ok": True, "sid": _sid, "topic": _cfg["topic"],
                    "interval": _cfg["interval"], "ttl": _cfg["ttl"],
                    "mode": _cfg["mode"]}

        elif _op == "stream_stop":
            _sid = str(_a["sid"])
            _ctx = globals().get("_cmq_streams", {}).get(_sid)
            if _ctx is not None:
                _ctx["event"].set()
            _res = {"ok": True, "sid": _sid, "was_active": _ctx is not None}

        elif _op == "stream_list":
            _ss2 = globals().get("_cmq_streams", {})
            _res = {"ok": True, "streams": [
                {"sid": _k, "mode": _v["cfg"]["mode"], "topic": _v["cfg"]["topic"],
                 "interval": _v["cfg"]["interval"],
                 "age": round(_t.time() - _v["started"], 1)}
                for _k, _v in list(_ss2.items())]}

        else:
            _res = {"ok": False, "error": "unknown op: %r" % (_op,)}

    except Exception:
        _res = {"ok": False, "error": _tb.format_exc()}
    return _j.dumps(_res, ensure_ascii=False)
_cmq_dispatch()
'''


def build_code(payload: dict) -> str:
    """把操作信封编译成远端可直接执行的 Python 代码。"""
    # json.dumps(str) 产出合法 Python 字面量；ensure_ascii=True 保证代码本体 ASCII。
    lit = json.dumps(json.dumps(payload, ensure_ascii=False))
    return _REMOTE_TEMPLATE.replace("__PAYLOAD__", lit)


def bytes_to_wire(b: bytes) -> str:
    """客户端字节 -> 报文字符串（latin-1 无损映射）。"""
    return b.decode("latin-1")


def wire_to_bytes(s) -> bytes:
    return (s or "").encode("latin-1")


def _wire_len(s: str) -> int:
    """预估该字符串经网络层 json.dumps(ensure_ascii=True) 转义后的在线字节数。

    可打印 ASCII（除引号/反斜杠）1 字节；控制字符与非 ASCII 都是 \\uXXXX 6 字节，
    引号/反斜杠转义后 2 字节。
    """
    total = 0
    for ch in s:
        o = ord(ch)
        if ch == '"' or ch == "\\":
            total += 2
        elif 0x20 <= o <= 0x7E:
            total += 1
        else:
            total += 6
    return total


def _iter_wire_chunks(s: str, budget: int = WIRE_BUDGET):
    """按在线尺寸预算切字符串，产出 (字节偏移, 字节偏移, 子串)。

    在线长度随 j 单调，用二分找最大可行 j（O(log n) 次长度计算）；
    绝不能逐字符递减全量重算——那是 O(n^2)，200KB 随机二进制会卡数十分钟。
    对 latin-1 映射串字符偏移 == 字节偏移（1:1）。
    """
    n = len(s)
    i = 0
    while i < n:
        lo, hi, best = i + 1, min(i + budget, n), i
        while lo <= hi:
            mid = (lo + hi) // 2
            if _wire_len(s[i:mid]) <= budget:
                best = mid
                lo = mid + 1
            else:
                hi = mid - 1
        yield i, best, s[i:best]
        i = best


# ============================ 异常 / 结果对象 ============================

class RemoteError(RuntimeError):
    """所有远端客户端异常的基类。"""


class RemoteTimeout(RemoteError):
    """请求超时（通道无回包）。"""


class RemoteRpcError(RemoteError):
    """远端 Python 执行失败（语法/未捕获异常）。"""

    def __init__(self, message, response=None):
        super().__init__(message)
        self.response = response


class RemoteOpError(RemoteError):
    """远端 op 逻辑返回失败。"""

    def __init__(self, message, env=None, response=None):
        super().__init__(message)
        self.env = env
        self.response = response


class TransferTooLarge(RemoteError):
    """试图经报文通道传输超过 MAX_TRANSFER 的文件/数据。"""


class CmdResult:
    """shell 命令结果。stdout/stderr 始终是 bytes。"""

    __slots__ = ("rc", "stdout", "stderr", "duration", "timed_out")

    def __init__(self, rc, stdout, stderr, duration, timed_out):
        self.rc = rc
        self.stdout = stdout
        self.stderr = stderr
        self.duration = duration
        self.timed_out = timed_out

    @property
    def ok(self) -> bool:
        return self.rc == 0 and not self.timed_out

    @property
    def text(self) -> str:
        return self.stdout.decode("utf-8", "replace")

    @property
    def err_text(self) -> str:
        return self.stderr.decode("utf-8", "replace")

    def check(self) -> "CmdResult":
        if not self.ok:
            raise RemoteError(
                f"remote command failed (rc={self.rc}, timed_out={self.timed_out}):\n"
                f"{self.err_text}")
        return self

    def __repr__(self):
        return (f"CmdResult(rc={self.rc}, {len(self.stdout)}B out, "
                f"{len(self.stderr)}B err, {self.duration}s, timed_out={self.timed_out})")


# ============================ 与传输无关的远端操作封装 ============================

class RemoteShell:
    """远端 Shell / 文件操作客户端。网络层由 ``transport`` 注入。

    Parameters
    ----------
    transport : Transport
        一问一答代码通道（MQTT 版见 cmd_client_mqtt.MqttTransport）。
    timeout : 默认问答等待秒数
    max_transfer : 经报文通道传输单文件/数据的硬上限（默认 1 MiB，实测 broker 上限）
    wire_budget : 单条报文在线尺寸目标（默认 128 KiB）

    大文件原则
    ----------
    超过 ``max_transfer`` 的读写/上传下载默认直接抛 :class:`TransferTooLarge`。
    请改为在远端就地处理：:meth:`grep` / :meth:`edit_replace` /
    ``run("sed -i ...")`` / ``run("curl -O ..." )`` / ``run("tar ...")`` /
    :meth:`apt_install` / :meth:`pip_install`，或让远端之间直接传文件。
    """

    def __init__(self, transport: Transport, timeout: float = DEFAULT_TIMEOUT,
                 max_transfer: int = MAX_TRANSFER, wire_budget: int = WIRE_BUDGET):
        if not isinstance(transport, Transport):
            raise TypeError("transport 必须是 remote_cmd.Transport 实例")
        self.tr = transport
        self.timeout = timeout
        self.max_transfer = max_transfer
        self.wire_budget = wire_budget
        self._info = None
        self._cwd = None
        self._lock = threading.RLock()

    # ---------- 底层问答 ----------

    def _call(self, op, req_timeout=None, **args) -> dict:
        payload = {"op": op}
        for k, v in args.items():
            if v is not None:
                payload[k] = v
        resp = self.tr.request(build_code(payload), req_timeout or self.timeout)
        if not resp:
            raise RemoteTimeout(f"请求超时无回包: op={op}")
        if not resp.get("ok"):
            raise RemoteRpcError(resp.get("error", "remote rpc failed"), resp)
        try:
            env = json.loads(resp.get("r") or "")
        except (ValueError, TypeError) as e:
            raise RemoteRpcError(f"无法解析远端回包 r={resp.get('r')!r}: {e}", resp)
        if not env.get("ok"):
            raise RemoteOpError(env.get("error", "remote op failed"), env, resp)
        return env

    def _guard(self, name, size, allow_large):
        if size > self.max_transfer and not allow_large:
            raise TransferTooLarge(
                f"{name} 大小 {size} 字节超过报文通道上限 {self.max_transfer} 字节 "
                f"(1MiB)：MQTT broker 实测 1.2MiB 即丢包。请改为远端就地处理——"
                f"grep/edit_replace/sed/curl/tar/apt 等都在远端执行；"
                f"确需经本通道传输时显式传 allow_large=True 自行承担风险。")

    def close(self):
        self.tr.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---------- 环境 ----------

    def info(self, refresh=False) -> dict:
        if self._info is None or refresh:
            with self._lock:
                self._info = self._call("info")
                self._cwd = self._info["cwd"]
        return self._info

    @property
    def cwd(self) -> str:
        if self._cwd is None:
            self.info()
        return self._cwd

    def cd(self, path) -> str:
        st = self._call("stat", path=str(path))
        if not st.get("isdir"):
            raise RemoteError(f"not a directory: {st.get('path')}")
        self._cwd = st["path"]
        return self._cwd

    # ---------- 命令（像 SSH 一样用） ----------

    def run(self, cmd, cwd=None, timeout=None, env=None, shell=True, check=False) -> CmdResult:
        """执行远端命令（默认 /bin/sh -c）。

        timeout 为远端命令超时秒数，到时 kill 整个进程组；本地问答等待自动 +15s 余量。
        """
        args = {"cwd": cwd or self._cwd, "env": env, "inline_max": INLINE_MAX}
        req_to = self.timeout
        if timeout is not None:
            args["timeout"] = timeout
            req_to = max(self.timeout, float(timeout) + 15)
        if shell:
            args["cmd"] = str(cmd)
        else:
            args["args"] = list(cmd)
        env_resp = self._call("run", req_to, **args)
        result = CmdResult(
            rc=env_resp.get("rc", -1),
            stdout=self._collect(env_resp, "out"),
            stderr=self._collect(env_resp, "err"),
            duration=env_resp.get("dur", 0.0),
            timed_out=env_resp.get("timed_out", False),
        )
        if check:
            result.check()
        return result

    def _collect(self, env, tag) -> bytes:
        sid = env.get(tag + "_id")
        if sid:
            return self._fetch_stash(sid)
        return wire_to_bytes(env.get(tag))

    def _fetch_stash(self, sid) -> bytes:
        parts = []
        off = 0
        try:
            while True:
                r = self._call("fetch", id=sid, off=off, n=self.wire_budget)
                parts.append(wire_to_bytes(r["data"]))
                off += r["n"]
                if r["eof"]:
                    break
        finally:
            try:
                self._call("drop", id=sid)
            except Exception:
                pass
        return b"".join(parts)

    def py(self, code, timeout=None) -> dict:
        """逃生舱：直接发任意 Python 代码，返回原始回包 dict。"""
        resp = self.tr.request(code, timeout or self.timeout)
        if not resp:
            raise RemoteTimeout("python 请求超时无回包")
        return resp

    # ---------- 远端就地操作（首选，文件内容不离开远端） ----------

    def grep(self, pattern, paths=(".",), recursive=True, ignore_case=False,
             fixed=False, include=None, exclude=None, cwd=None,
             max_matches=0) -> list[dict]:
        """在远端 grep，返回 ``[{path, lineno, text}, ...]``，不拉文件。

        默认 ERE；fixed=True 按字面量（grep -F）。rc=1（无匹配）正常返回空列表。
        max_matches>0 时只保留前 N 条（客户端截断）。
        """
        r = self._call("grep", pattern=str(pattern),
                       paths=[str(p) for p in (paths if isinstance(paths, (list, tuple))
                                               else (paths,))],
                       recursive=recursive, ignore_case=ignore_case, fixed=fixed,
                       include=include, exclude=exclude, cwd=cwd or self._cwd)
        if r.get("rc", 0) > 1 and r.get("err"):
            raise RemoteOpError(r["err"], r)
        matches = []
        for line in wire_to_bytes(r.get("out")).decode("utf-8", "replace").splitlines():
            # -nH 形如 path:lineno:text（文件名含冒号是已知边界情况）
            first = line.find(":")
            second = line.find(":", first + 1) if first >= 0 else -1
            if first < 0 or second < 0:
                continue
            path = line[:first]
            num_txt = line[first + 1:second]
            if not num_txt.isdigit():
                continue
            matches.append({"path": path, "lineno": int(num_txt), "text": line[second + 1:]})
            if max_matches and len(matches) >= max_matches:
                break
        return matches

    def edit_replace(self, path, old, new, count=1, backup=True,
                     encoding="utf-8", cwd=None) -> dict:
        """远端就地精确字符串替换（sed 的安全替代，无需引号地狱）。

        old 必须存在，否则报错；count=0 替换全部；backup=True 先留 .bak。
        返回 {path, matches, replaced, bytes}。
        """
        return self._call("edit", path=str(path), old=old, new=new, count=count,
                          backup=backup, encoding=encoding, append=False, cwd=cwd)

    def append_text(self, path, text, backup=False, encoding="utf-8") -> dict:
        """远端就地追加文本（文件内容不回传）。"""
        return self._call("edit", path=str(path), old="", new=text,
                          backup=backup, encoding=encoding, append=True)

    def apt_install(self, packages, update=True, timeout=600) -> CmdResult:
        """远端 apt-get 非交互安装（需要 root；设备是 Docker root）。"""
        if isinstance(packages, str):
            packages = [packages]
        pkgs = " ".join(str(p) for p in packages)
        script = "set -e; export DEBIAN_FRONTEND=noninteractive; "
        if update:
            script += "apt-get update -y; "
        script += f"apt-get install -y --no-install-recommends {pkgs}"
        return self.run(script, timeout=timeout, check=True)

    def pip_install(self, packages, args=("--break-system-packages",), timeout=600) -> CmdResult:
        """远端 pip 安装（默认带 --break-system-packages，Debian12 容器需要）。"""
        if isinstance(packages, str):
            packages = [packages]
        opts = " ".join(args) if args else ""
        return self.run(
            "python3 -m pip install --no-cache-dir " + opts + " "
            + " ".join(str(p) for p in packages),
            timeout=timeout, check=True)

    def which(self, name) -> str | None:
        r = self.run(f"command -v {name} || true")
        p = r.text.strip()
        return p or None

    # ---------- 周期汇报 / 持续输出（像 ssh 里跑 top，但是订阅推送模型） ----------

    def stream(self, cmd=None, interval=1.0, count=0,
               topic=DEFAULT_STREAM_TOPIC, ttl=60.0, stats=False,
               cwd=None, frame_timeout=20.0, on_frame=None,
               stop_event=None, req_timeout=30.0) -> int:
        """让远端后台线程周期执行/采集并推送到 topic，本地订阅实时收帧。

        - ``stats=True``：远端纯标准库读 /proc 采 CPU%/内存/load/top8 进程，
          每帧约 1KB，**容器里没装 top 也能用**；payload 为 stats dict。
        - ``stats=False``：周期 shell 执行 ``cmd``（如 ``'top -b -n 1'``），
          payload 为 ``{"rc", "out": bytes, "err": bytes, "frame_error"}``。
        - ``count=0`` 持续到 ``ttl`` 或 KeyboardInterrupt；退出时自动 stream_stop。
        - ``on_frame(seq:int, payload, raw:dict)``：每帧（已按 sid/seq 去重）。
        - ``stop_event``：外部 threading.Event，set 即优雅停止。
        返回实际收到的帧数。要求 transport 实现 stream_subscribe/unsubscribe。
        """
        if not callable(getattr(self.tr, "stream_subscribe", None)):
            raise RemoteError("当前 transport 不支持推送订阅（缺 stream_subscribe）")
        sid = "cmq-%d-%s" % (int(time.time() * 1000), os.urandom(2).hex())
        state = {"n": 0, "end": None}
        seen = set()
        wake = threading.Event()
        lock = threading.Lock()

        def _handler(data):
            if not isinstance(data, dict) or data.get("stream") != sid:
                return
            if data.get("end"):
                state["end"] = data.get("reason")
                wake.set()
                return
            key = ("f", data.get("seq", 0))
            with lock:
                if key in seen:
                    return  # 同一帧被多个 broker 重复送达
                seen.add(key)
                state["n"] += 1
                n = state["n"]
            if stats or "stats" in data:
                payload = data.get("stats") or {}
            else:
                payload = {"rc": data.get("rc"),
                           "out": wire_to_bytes(data.get("out")),
                           "err": wire_to_bytes(data.get("err")),
                           "frame_error": data.get("frame_error")}
            if on_frame is not None:
                try:
                    on_frame(n, payload, data)
                except Exception:
                    pass
            wake.set()

        self.tr.stream_subscribe(topic, _handler)
        try:
            r = self._call("stream_start", req_timeout=req_timeout, sid=sid,
                           topic=topic, interval=interval, count=count, ttl=ttl,
                           mode=("stats" if stats else "cmd"), cmd=cmd,
                           cwd=cwd or self._cwd, frame_timeout=frame_timeout)
            deadline = time.time() + min(float(r.get("ttl", ttl)) + 15.0, 1830.0)
            while True:
                if state["end"] is not None or (count and state["n"] >= count):
                    break
                if stop_event is not None and stop_event.is_set():
                    break
                if time.time() > deadline:
                    break
                wake.wait(0.5)
                wake.clear()
        except KeyboardInterrupt:
            pass
        finally:
            try:
                self._call("stream_stop", req_timeout=10.0, sid=sid)
            except Exception:
                pass
            self.tr.stream_unsubscribe(topic, _handler)
        return state["n"]

    def monitor(self, on_frame, interval=1.0, count=0, ttl=60.0, **ka) -> int:
        """stats 模式便捷封装：``on_frame(seq, stats:dict, raw)``，返回帧数。"""
        return self.stream(stats=True, interval=interval, count=count,
                           ttl=ttl, on_frame=on_frame, **ka)

    def stream_list(self, req_timeout=20.0) -> list:
        """列出远端仍在运行的汇报流（诊断用）。"""
        return self._call("stream_list", req_timeout=req_timeout).get("streams", [])

    # ---------- 文件传输（仅用于 ≤1MiB 的小文件！） ----------

    def read(self, path, allow_large=False) -> bytes:
        """读远端文件全文（自动分块）。超过 1MiB 默认拒绝——大文件请在远端 grep/tail。"""
        parts = []
        total = None
        off = 0
        while True:
            r = self._call("read", path=str(path), off=off, n=self.wire_budget)
            total = r["size"]
            if off == 0:
                self._guard(f"读取 {path}", total, allow_large)
            parts.append(wire_to_bytes(r["data"]))
            off += r["n"]
            if r["eof"]:
                break
        return b"".join(parts)

    def read_text(self, path, encoding="utf-8", allow_large=False) -> str:
        return self.read(path, allow_large=allow_large).decode(encoding)

    def write(self, path, data, mode=None, backup=False, allow_large=False) -> dict:
        """把 bytes/str 原子写入远端文件（临时文件 + replace；默认保留原权限）。"""
        if isinstance(data, str):
            data = data.encode("utf-8")
        data = bytes(data)
        self._guard(f"写入 {path}", len(data), allow_large)
        part = self._call("push_init", path=str(path))["part"]
        try:
            r = None
            if data:
                mapped = bytes_to_wire(data)
                for off, _j, chunk in _iter_wire_chunks(mapped, self.wire_budget):
                    last = off + len(chunk) >= len(mapped)
                    ka = {"part": part, "off": off, "data": chunk, "last": last}
                    if last:
                        ka.update(path=str(path), mode=mode, backup=backup)
                    r = self._call("push_data", **ka)
            else:
                r = self._call("push_data", part=part, off=0, data="",
                               last=True, path=str(path), mode=mode, backup=backup)
            return r
        except Exception:
            try:
                self._call("push_abort", part=part)
            except Exception:
                pass
            raise

    def write_text(self, path, text, encoding="utf-8", mode=None, backup=False,
                   allow_large=False) -> dict:
        return self.write(path, text.encode(encoding), mode=mode, backup=backup,
                          allow_large=allow_large)

    def download(self, remote, local, verify=True, allow_large=False, progress=None) -> dict:
        """远端小文件下载到本地（分块 + 临时文件替换 + sha256 校验）。"""
        remote = str(remote)
        local = os.path.abspath(str(local))
        if os.path.isdir(local):
            local = os.path.join(local, posixpath.basename(remote.rstrip("/")) or "download")
        st = self._call("stat", path=remote)
        self._guard(f"下载 {remote}", st["size"], allow_large)
        os.makedirs(os.path.dirname(local) or ".", exist_ok=True)
        tmp = local + ".cmqpart"
        got = 0
        with open(tmp, "wb") as fh:
            off = 0
            while True:
                r = self._call("read", path=remote, off=off, n=self.wire_budget)
                fh.write(wire_to_bytes(r["data"]))
                off += r["n"]
                got = off
                if progress:
                    progress(got, r["size"])
                if r["eof"]:
                    total = r["size"]
                    break
        os.replace(tmp, local)
        digest = None
        if verify:
            import hashlib
            digest = self._call("hash", path=remote)["digest"]
            hh = hashlib.sha256()
            with open(local, "rb") as fh:
                for blk in iter(lambda: fh.read(1 << 20), b""):
                    hh.update(blk)
            if hh.hexdigest() != digest:
                raise RemoteError(f"下载校验失败: {remote} sha256 不一致")
        return {"local": local, "bytes": got, "remote_size": total, "sha256": digest}

    def upload(self, local, remote, mode=None, backup=False, allow_large=False,
               progress=None) -> dict:
        """本地小文件分块上传到远端（sha256 校验）。"""
        local = os.path.abspath(str(local))
        remote = str(remote)
        size = os.path.getsize(local)
        self._guard(f"上传 {local}", size, allow_large)
        part = self._call("push_init", path=remote)["part"]
        r = None
        try:
            with open(local, "rb") as fh:
                data = fh.read()
            if not data:
                r = self._call("push_data", part=part, off=0, data="",
                               last=True, path=remote, mode=mode, backup=backup)
            else:
                mapped = bytes_to_wire(data)
                off = 0
                for off, _j, chunk in _iter_wire_chunks(mapped, self.wire_budget):
                    last = off + len(chunk) >= len(mapped)
                    ka = {"part": part, "off": off, "data": chunk, "last": last}
                    if last:
                        ka.update(path=remote, mode=mode, backup=backup)
                    r = self._call("push_data", **ka)
                    if progress:
                        progress(min(off + len(chunk), size), size)
        except Exception:
            try:
                self._call("push_abort", part=part)
            except Exception:
                pass
            raise
        if r.get("bytes") != size:
            raise RemoteError(f"上传大小不一致: local={size} remote={r.get('bytes')}")
        return r

    # ---------- 目录 / 元信息 ----------

    def ls(self, path=".") -> list:
        return self._call("ls", path=str(path))["items"]

    def stat(self, path) -> dict:
        return self._call("stat", path=str(path))

    def exists(self, path) -> bool:
        return self._call("exists", path=str(path))["exists"]

    def mkdir(self, path, parents=True, mode=None) -> dict:
        return self._call("mkdir", path=str(path), parents=parents, mode=mode)

    def rm(self, path, recursive=False, missing_ok=True) -> dict:
        return self._call("rm", path=str(path), recursive=recursive, missing_ok=missing_ok)

    def mv(self, src, dst) -> dict:
        return self._call("mv", src=str(src), dst=str(dst))

    def cp(self, src, dst) -> dict:
        return self._call("cp", src=str(src), dst=str(dst))

    def chmod(self, path, mode, recursive=False) -> dict:
        return self._call("chmod", path=str(path), mode=mode, recursive=recursive)

    def hash(self, path, algo="sha256") -> dict:
        return self._call("hash", path=str(path), algo=algo)
