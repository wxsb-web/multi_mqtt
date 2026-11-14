#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""runmqtt 公共 broker 凭据自动同步工具（只依赖标准库 + 仓库内 client_mqtt）。

流程：
  1. 抓取 https://runmqtt.com/zh-Hans/public-broker 的「连接参数」
     （主机 / 用户名 / 密码）；
  2. 通过可插拔的执行通道读取远端 `gms.rpc_config_get()`；
  3. 比对 public.runmqtt.com 当前条目的凭据；
  4. 有变化时调用 `gms.rpc_config(broker_list_add=[...])`，只热更新这一条
     broker（其它 broker 连接不受影响），并持久化到远端 rpc.json；
  5. 再次 `rpc_config_get()` 回读校验。

执行通道：
  * HTTP  —— 把代码放进 URL 路径：http://<ip>:<port>/<urlencode(code)>
  * MQTT  —— client_mqtt.rpc(code, request_topic=..., k=..., mb=...)
             `--topic` 可重复，表示同一份更新要下发到多个目标 topic。

示例：
  python tools/runmqtt_password_sync.py --dry-run
 
  python tools/runmqtt_password_sync.py --base-url http://192.168.1.100:1122 \
      --topic aaa --key  --mb 6
  python tools/runmqtt_password_sync.py --watch 600
  python tools/runmqtt_password_sync.py --http-auth user:pass
"""

import argparse
import ast
import base64
import gzip
import html as html_lib
import http.client
import json
import logging
import re
import socket
import ssl
import sys
import time
import urllib.parse
import zlib
from pathlib import Path

PAGE_URL = "https://runmqtt.com/zh-Hans/public-broker"
DEFAULT_BASE_URL = "http://192.168.1.100:1122"
DEFAULT_BROKER_HOST = "public.runmqtt.com"
DEFAULT_BROKER_PORT = 1883
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0 Safari/537.36"
)
REPO_ROOT = Path(__file__).resolve().parent.parent

logger = logging.getLogger("runmqtt-sync")


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def _addr_text(sockaddr):
    host, port = sockaddr[0], sockaddr[1]
    if ":" in host:
        return "[%s]:%s" % (host, port)
    return "%s:%s" % (host, port)


def _resolve(host, port):
    """解析主机名并按去重后的地址顺序返回，dns 耗时一并打印。"""
    started = time.perf_counter()
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as error:
        raise RuntimeError("DNS 解析失败 %s: %s" % (host, error)) from error
    cost = (time.perf_counter() - started) * 1000
    seen, addrs = set(), []
    for family, stype, proto, _, sockaddr in infos:
        key = (family, sockaddr[0])
        if key in seen:
            continue
        seen.add(key)
        addrs.append((family, stype, proto, sockaddr))
    if not addrs:
        raise RuntimeError("DNS 解析 %s 没有得到可用地址" % host)
    logger.info("DNS 解析 %s -> %s（%.0fms）", host,
                ", ".join(_addr_text(sa) for *_, sa in addrs), cost)
    return addrs


def _open_socket(host, port, timeout, use_tls):
    """按解析结果逐个地址 TCP 连接（必要时 TLS 握手），每一步都打时延。"""
    last_error = None
    for family, stype, proto, sockaddr in _resolve(host, port):
        sock = socket.socket(family, stype, proto)
        sock.settimeout(timeout)
        started = time.perf_counter()
        try:
            sock.connect(sockaddr)
        except OSError as error:
            last_error = error
            sock.close()
            logger.warning("TCP 连接 %s 失败: %s", _addr_text(sockaddr), error)
            continue
        logger.info("TCP 已连接 %s（%.0fms）", _addr_text(sockaddr),
                    (time.perf_counter() - started) * 1000)
        if not use_tls:
            return sock
        started = time.perf_counter()
        try:
            tls = ssl.create_default_context().wrap_socket(
                sock, server_hostname=host)
        except (OSError, ssl.SSLError) as error:
            last_error = error
            sock.close()
            logger.warning("TLS 握手 %s 失败: %s", _addr_text(sockaddr), error)
            continue
        alpn = tls.selected_alpn_protocol() or "-"
        cipher = tls.cipher()[0] if tls.cipher() else "-"
        logger.info("TLS 握手完成 %s（%.0fms，%s，ALPN=%s）",
                    _addr_text(sockaddr), (time.perf_counter() - started) * 1000,
                    cipher, alpn)
        return tls
    raise RuntimeError("无法连接 %s:%s: %s" % (host, port, last_error))


def _http_get_once(url, headers, timeout):
    parts = urllib.parse.urlsplit(url)
    host = parts.hostname
    if not host:
        raise RuntimeError("URL 缺少主机名: %s" % url)
    use_tls = parts.scheme.lower() == "https"
    port = parts.port or (443 if use_tls else 80)
    path = parts.path or "/"
    if parts.query:
        path += "?" + parts.query

    sock = _open_socket(host, port, timeout, use_tls)
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    conn.sock = sock
    started = time.perf_counter()
    try:
        conn.request("GET", path, headers=headers)
        response = conn.getresponse()
        raw = response.read()
        cost = (time.perf_counter() - started) * 1000
        encoding = (response.getheader("Content-Encoding") or "").lower()
        if encoding == "gzip":
            body = gzip.decompress(raw)
        elif encoding == "deflate":
            body = zlib.decompress(raw)
        else:
            body = raw
        logger.info("HTTP GET http%s://%s%s -> %s %s（%.0fms，%d 字节%s）",
                    "s" if use_tls else "", _addr_text((host, port)), path,
                    response.status, response.reason, cost, len(body),
                    "，压缩传输 %d" % len(raw) if len(raw) != len(body) else "")
        return response.status, response.getheader("Location"), body
    finally:
        conn.close()


def http_text(url, *, timeout=15.0, auth=None, browser=False):
    """GET 一个 URL 并返回文本；DNS/TCP/TLS/HTTP 每步都打印连接详情。"""
    headers = {"Accept": "*/*", "Accept-Encoding": "gzip, deflate"}
    if browser:
        headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "zh-CN,zh;q=0.9",
        })
    else:
        headers["User-Agent"] = "runmqtt-password-sync/1.0"
    if auth:
        token = base64.b64encode(auth.encode("utf-8")).decode("ascii")
        headers["Authorization"] = "Basic " + token

    current = url
    for _ in range(5):
        status, location, body = _http_get_once(current, headers, timeout)
        if status in (301, 302, 303, 307, 308) and location:
            current = urllib.parse.urljoin(current, location)
            logger.info("重定向到 %s", current)
            continue
        text = body.decode("utf-8", "replace")
        if status >= 400:
            raise RuntimeError(
                "HTTP %s %s: %s" % (status, current, text[:800].strip()))
        return text
    raise RuntimeError("重定向次数过多: %s" % url)


# ---------------------------------------------------------------------------
# 页面解析
# ---------------------------------------------------------------------------
def _html_field(page, label):
    """取「连接参数」里 <dt>label</dt><dd><code>value</code> 的值。"""
    pattern = re.compile(
        r">\s*" + re.escape(label) + r"\s*</dt>\s*<dd[^>]*>\s*<code[^>]*>(.*?)</code>",
        re.S | re.I,
    )
    match = pattern.search(page)
    if match is None:
        return None
    return html_lib.unescape(match.group(1)).strip()


def parse_page_credentials(page):
    """从公共 broker 页面解析 host / port / username / password。"""
    username = _html_field(page, "用户名")
    password = _html_field(page, "密码")
    host = _html_field(page, "主机")
    port_text = _html_field(page, "TCP")

    # 兜底：页面结构微调时用示例代码里的 export 行
    if not username:
        match = re.search(r'MQTT_USERNAME\s*=\s*"([^"]*)"', page)
        username = match.group(1) if match else None
    if not password:
        match = re.search(r'MQTT_PASSWORD\s*=\s*"([^"]*)"', page)
        password = match.group(1) if match else None
    if not host:
        match = re.search(r'mosquitto_sub\s*\\?\s*-h\s+([^\s\\]+)', page)
        host = match.group(1) if match else None

    missing = [
        name for name, value in (
            ("主机", host), ("TCP 端口", port_text),
            ("用户名", username), ("密码", password),
        ) if not value
    ]
    if missing:
        raise RuntimeError("页面里没有解析到：%s（页面结构可能已改）" % "、".join(missing))

    try:
        port = int(port_text)
    except ValueError as error:
        raise RuntimeError("页面 TCP 端口不是整数：%r" % port_text) from error

    return {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
    }


# ---------------------------------------------------------------------------
# RPC 返回解码
# ---------------------------------------------------------------------------
def decode_payload(text):
    """解码执行结果：Python 字面量（IPython pretty）或 JSON，都不是就原样返回。"""
    if not isinstance(text, str):
        return text
    stripped = text.strip()
    if not stripped:
        return text
    try:
        return ast.literal_eval(stripped)
    except (ValueError, SyntaxError):
        pass
    try:
        return json.loads(stripped)
    except ValueError:
        return text


# ---------------------------------------------------------------------------
# 执行通道（可插拔）
# ---------------------------------------------------------------------------
class HttpTarget:
    """HTTP RPC 目标：代码放在 URL 路径里执行。"""

    kind = "http"

    def __init__(self, base_url, auth="", timeout=15.0):
        self.base_url = base_url
        self.auth = auth
        self.timeout = timeout

    @property
    def label(self):
        return self.base_url

    def execute(self, code):
        url = self.base_url.rstrip("/") + "/" + urllib.parse.quote(code, safe="")
        return decode_payload(http_text(url, timeout=self.timeout, auth=self.auth))


class MqttTarget:
    """MQTT RPC 目标：client_mqtt.rpc 把代码广播到指定 request_topic。"""

    kind = "mqtt"

    def __init__(self, topic, key=None, min_broker_count=0, timeout=30.0,
                 allow_unsigned=True):
        self.topic = topic
        self.key = key
        self.min_broker_count = min_broker_count
        self.timeout = timeout
        # 默认接受未回签的响应：老版本服务端不会按协议回签 req_id，开启私钥后
        # client 会把这些合法回包丢弃，表现为"topic 明明在线却一直超时"。
        self.allow_unsigned = allow_unsigned

    @property
    def label(self):
        return "mqtt:" + self.topic

    @staticmethod
    def _manager(client_mqtt):
        client = getattr(client_mqtt, "_default_client", None)
        return getattr(client, "mqtt_net", None)

    @staticmethod
    def _broker_rows(manager):
        """取 (host, 是否在线) 列表；任何异常都吞掉，只用于展示。"""
        rows = []
        try:
            with manager.lock:
                clients = list(manager.clients.items())
        except Exception:
            return rows
        for host, client in clients:
            try:
                online = bool(client.is_connected())
            except Exception:
                online = False
            rows.append((host, online))
        return rows

    def _log_brokers(self, manager, level=logging.INFO):
        rows = self._broker_rows(manager)
        if not rows:
            logger.log(level, "[%s] broker 连接详情: 不可用（客户端未启动）",
                       self.label)
            return
        online = sum(1 for _, ok in rows if ok)
        logger.log(level, "[%s] broker 连接详情: %d/%d 在线",
                   self.label, online, len(rows))
        for host, ok in rows:
            logger.log(level, "[%s]   %s %s", self.label,
                       "在线" if ok else "离线", host)

    def execute(self, code):
        if str(REPO_ROOT) not in sys.path:
            sys.path.insert(0, str(REPO_ROOT))
        from client import client_mqtt

        started = time.perf_counter()
        response = client_mqtt.rpc(
            code,
            request_topic=self.topic,
            timeout=self.timeout,
            client_private_key_bytes=self.key,
            min_broker_count=self.min_broker_count,
            allow_no_server_pubkey_response=self.allow_unsigned,
        )
        cost_ms = (time.perf_counter() - started) * 1000
        manager = self._manager(client_mqtt)

        if not isinstance(response, dict) or not response:
            self._log_brokers(manager)
            raise RuntimeError(
                "MQTT RPC 无响应（topic=%s，等待 %.1fs，可能超时或没有在线 "
                "broker 订阅该 topic）" % (self.topic, cost_ms / 1000))
        if not response.get("ok"):
            self._log_brokers(manager)
            raise RuntimeError(
                "MQTT RPC 执行失败 topic=%s: %s"
                % (self.topic, str(response.get("error"))[:800].strip())
            )
        rows = self._broker_rows(manager)
        online = sum(1 for _, ok in rows if ok)
        logger.info(
            "[%s] 回包: client_from=%s server_from=%s 往返=%sms broker 在线=%d/%d "
            "req_id=%s",
            self.label,
            response.get("client_from"),
            response.get("server_from"),
            response.get("latency_ms", round(cost_ms, 2)),
            online, len(rows),
            response.get("req_id"),
        )
        return decode_payload(response.get("r"))


# ---------------------------------------------------------------------------
# 凭据比对 / 更新
# ---------------------------------------------------------------------------
def _entry_identity(entry):
    if not isinstance(entry, (list, tuple)) or len(entry) < 2:
        return None
    try:
        return str(entry[0]).strip().casefold(), int(entry[1])
    except (TypeError, ValueError):
        return None


def find_entry(config, host, port):
    identity = (host.casefold(), int(port))
    for entry in config.get("broker_list") or []:
        if _entry_identity(entry) == identity:
            return entry
    return None


def entry_credentials(entry):
    """取条目里的 (username, password)；无认证返回 None。"""
    if not isinstance(entry, (list, tuple)) or len(entry) < 3:
        return None
    auth = entry[2]
    if isinstance(auth, (list, tuple)) and len(auth) >= 2:
        return str(auth[0]), str(auth[1])
    if isinstance(auth, str):
        return auth, ""
    return None


def merge_add_entries(config, new_entry, host, port):
    """保留其它追加条目，同 host:port 的旧条目用新凭据原位替换。"""
    identity = (host.casefold(), int(port))
    merged = []
    for entry in config.get("broker_list_add") or []:
        if _entry_identity(entry) == identity:
            continue
        if isinstance(entry, (list, tuple)) and len(entry) >= 2:
            merged.append(list(entry))
    merged.append(list(new_entry))
    return merged


def read_config(target):
    config = target.execute("r=gms.rpc_config_get()")
    if not isinstance(config, dict):
        raise RuntimeError("rpc_config_get 返回的不是 dict：%r" % (type(config),))
    return config


def sync_target(target, fresh, args):
    """对一个执行目标做 读取→比对→热更新→回读校验。"""
    config = read_config(target)
    current = find_entry(config, args.host, args.port)
    if current is None:
        logger.info("[%s] 远端没有 %s:%s 条目，将新增",
                    target.label, args.host, args.port)
    else:
        logger.info("[%s] 远端当前条目: %r", target.label, current)

    credentials = entry_credentials(current) if current is not None else None
    fresh_credentials = (fresh["username"], fresh["password"])
    if credentials == fresh_credentials:
        logger.info("[%s] 凭据一致，无需更新", target.label)
        return "unchanged"

    if credentials is not None:
        logger.info("[%s] 凭据变化: %s / %s -> %s / %s",
                    target.label, credentials[0], credentials[1],
                    fresh_credentials[0], fresh_credentials[1])

    new_entry = [fresh["host"], args.port,
                 [fresh["username"], fresh["password"]]]
    additions = merge_add_entries(config, new_entry, args.host, args.port)

    if args.dry_run:
        logger.info("[%s] [dry-run] 将提交 broker_list_add=%r",
                    target.label, additions)
        return "dry-run"

    target.execute("r=gms.rpc_config(broker_list_add=%r)" % (additions,))
    logger.info("[%s] 已提交热更新: %s:%s", target.label, args.host, args.port)

    verify = read_config(target)
    verified = find_entry(verify, args.host, args.port)
    if entry_credentials(verified) != fresh_credentials:
        raise RuntimeError("回读校验失败，当前条目为 %r" % (verified,))
    logger.info("[%s] 回读校验通过: %r", target.label, verified)
    return "updated"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_key(text):
    """--key 接受整数（含 0x）或纯数值算式。"""
    if not text:
        return None
    text = text.strip()
    try:
        return int(text, 0)
    except ValueError:
        pass
    try:
        return ast.literal_eval(text)
    except (ValueError, SyntaxError):
        pass
    if not re.fullmatch(r"[0-9eE_+\-*()\s]+", text):
        raise argparse.ArgumentTypeError("--key 只接受整数或数值算式: %r" % text)
    try:
        return eval(text, {"__builtins__": {}}, {})
    except Exception as error:
        raise argparse.ArgumentTypeError("--key 无法解析: %r" % text) from error


def build_parser():
    parser = argparse.ArgumentParser(
        description="从 runmqtt 公共 broker 页面抓取最新凭据，"
                    "经 HTTP / MQTT 通道热更新远端 gms.rpc_config")
    parser.add_argument("--base-url", action="append", metavar="URL",
                        help="HTTP RPC 基址，可重复；不给且没有 --topic 时默认 %s"
                             % DEFAULT_BASE_URL)
    parser.add_argument("--topic", "-t", action="append", metavar="TOPIC",
                        help="MQTT request_topic，可重复（多个目标设备）")
    parser.add_argument("--key", "-k", type=_parse_key, default=None,
                        help="MQTT 签名私钥，整数或数值算式")
    parser.add_argument("--mb", type=int, default=0,
                        help="MQTT 最少在线 broker 数（默认 %(default)s）")
    parser.add_argument("--page-url", default=PAGE_URL,
                        help="凭据来源页面（默认 %(default)s）")
    parser.add_argument("--page-timeout", type=float, default=20.0,
                        help="抓取凭据页面的超时秒数（默认 %(default)s）")
    parser.add_argument("--page-retry", type=int, default=3,
                        help="凭据页面失败重试次数（默认 %(default)s）")
    parser.add_argument("--host", default=DEFAULT_BROKER_HOST,
                        help="要更新的 broker 主机（默认 %(default)s）")
    parser.add_argument("--port", type=int, default=DEFAULT_BROKER_PORT,
                        help="要更新的 broker 端口（默认 %(default)s）")
    parser.add_argument("--timeout", type=float, default=30.0,
                        help="单次 RPC/HTTP 超时秒数（默认 %(default)s）")
    parser.add_argument("--watch", type=float, default=0.0, metavar="SECONDS",
                        help="常驻轮询间隔秒数；0 表示只跑一次")
    parser.add_argument("--dry-run", action="store_true",
                        help="只抓取和比对，不提交更新")
    parser.add_argument("--http-auth", default="",
                        help="HTTP RPC 的 Basic 认证，格式 user:password")
    return parser


def build_targets(args):
    targets = []
    for base_url in args.base_url or []:
        targets.append(HttpTarget(
            base_url, auth=args.http_auth, timeout=args.timeout))
    for topic in args.topic or []:
        targets.append(MqttTarget(
            topic, key=args.key,
            min_broker_count=args.mb, timeout=args.timeout))
    if not targets:
        targets.append(HttpTarget(
            DEFAULT_BASE_URL, auth=args.http_auth, timeout=args.timeout))
    return targets


def fetch_fresh_credentials(args):
    """抓页面凭据；单次 SSL/网络抖动重试，全部失败才抛错交给调用方降级。"""
    last_error = None
    for attempt in range(1, max(1, args.page_retry) + 1):
        try:
            page = http_text(args.page_url, timeout=args.page_timeout,
                             browser=True)
            fresh = parse_page_credentials(page)
            if attempt > 1:
                logger.info("页面抓取第 %d 次成功", attempt)
            return fresh
        except Exception as error:
            last_error = error
            logger.warning("页面抓取失败（第 %d/%d 次）: %s",
                           attempt, args.page_retry, error)
            if attempt < args.page_retry:
                time.sleep(min(2.0 * attempt, 5.0))
    raise RuntimeError("页面抓取连续 %d 次失败: %s"
                       % (args.page_retry, last_error))


def run_round(targets, args):
    """跑一轮：先取最新凭据，再逐个目标同步。

    任何单点失败都只降级，不影响其它目标：页面抓不到就整轮跳过并返回失败数，
    某个 topic 超时/报错只记在该目标上，其余 topic 继续。
    """
    try:
        fresh = fetch_fresh_credentials(args)
    except Exception as error:
        logger.error("本轮跳过：%s", error)
        return len(targets)
    logger.info("页面凭据: %s:%s 用户名=%s 密码=%s",
                fresh["host"], fresh["port"], fresh["username"], fresh["password"])

    failed = 0
    for target in targets:
        try:
            sync_target(target, fresh, args)
        except RuntimeError as error:
            # 超时/无响应等预期失败：只打一行，不刷 traceback
            failed += 1
            logger.error("[%s] 同步失败（不影响其它目标）: %s",
                         target.label, error)
        except Exception:
            failed += 1
            logger.exception("[%s] 同步失败（不影响其它目标）", target.label)
    return failed


def main(argv=None):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    args = build_parser().parse_args(argv)
    targets = build_targets(args)
    logger.info("同步目标: %s", ", ".join(target.label for target in targets))

    if args.watch <= 0:
        try:
            failed = run_round(targets, args)
        except Exception:
            logger.exception("同步失败")
            return 1
        if failed >= len(targets):
            logger.error("本轮所有目标均失败（%d/%d）", failed, len(targets))
            return 1
        if failed:
            logger.warning("本轮 %d/%d 个目标失败，其余已完成",
                           failed, len(targets))
        return 0

    logger.info("常驻模式：每 %s 秒检查一次（Ctrl+C 退出）", args.watch)
    while True:
        try:
            run_round(targets, args)
        except KeyboardInterrupt:
            raise
        except Exception:
            logger.exception("本轮同步失败，%s 秒后重试", args.watch)
        time.sleep(args.watch)


if __name__ == "__main__":
    sys.exit(main())
