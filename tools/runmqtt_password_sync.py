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
import html as html_lib
import json
import logging
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
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
def http_text(url, *, timeout=15.0, auth=None, browser=False):
    """GET 一个 URL 并返回文本；HTTP 错误带上响应体抛 RuntimeError。"""
    headers = {"Accept": "*/*"}
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
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", "replace")
        raise RuntimeError(
            "HTTP %s %s: %s" % (error.code, url, body[:800].strip())
        ) from error
    except urllib.error.URLError as error:
        raise RuntimeError("无法访问 %s: %s" % (url, error.reason)) from error


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

    def __init__(self, topic, key=None, min_broker_count=0, timeout=30.0):
        self.topic = topic
        self.key = key
        self.min_broker_count = min_broker_count
        self.timeout = timeout

    @property
    def label(self):
        return "mqtt:" + self.topic

    def execute(self, code):
        if str(REPO_ROOT) not in sys.path:
            sys.path.insert(0, str(REPO_ROOT))
        from client import client_mqtt

        response = client_mqtt.rpc(
            code,
            request_topic=self.topic,
            timeout=self.timeout,
            client_private_key_bytes=self.key,
            min_broker_count=self.min_broker_count,
        )
        if not isinstance(response, dict) or not response:
            raise RuntimeError("MQTT RPC 无响应（topic=%s，可能超时）" % self.topic)
        if not response.get("ok"):
            raise RuntimeError(
                "MQTT RPC 执行失败 topic=%s: %s"
                % (self.topic, str(response.get("error"))[:800].strip())
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
    """--key 接受整数（含 0x）或纯数值算式，如 2**222。"""
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
                        help="MQTT 签名私钥，整数或数值算式，如 \"2**222\"")
    parser.add_argument("--mb", type=int, default=0,
                        help="MQTT 最少在线 broker 数（默认 %(default)s）")
    parser.add_argument("--page-url", default=PAGE_URL,
                        help="凭据来源页面（默认 %(default)s）")
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


def run_round(targets, args):
    page = http_text(args.page_url, timeout=args.timeout, browser=True)
    fresh = parse_page_credentials(page)
    logger.info("页面凭据: %s:%s 用户名=%s 密码=%s",
                fresh["host"], fresh["port"], fresh["username"], fresh["password"])

    failed = 0
    for target in targets:
        try:
            sync_target(target, fresh, args)
        except Exception:
            failed += 1
            logger.exception("[%s] 同步失败", target.label)
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
        return 1 if run_round(targets, args) else 0

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
