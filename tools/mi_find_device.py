#!/usr/bin/env python3
"""小米云服务「查找设备」：登录并读取手机在线状态 / 电量 / GPS 位置。

两条登录通道：

1. 纯 HTTP 登录（requests）——直接 POST 小米账号登录接口。账号触发风控时
   会要求二次验证，脚本会把验证地址打印出来，用户可以点开完成验证后重跑。
2. 浏览器登录（playwright）——复用本机 Edge/Chrome 走真实的网页登录流程，
   成功后把 Cookie 缓存下来，后续直接调接口。这条通道最稳，也是默认兜底。

用法：
    python tools/mi_find_device.py                       # 自动登录并查询
    python tools/mi_find_device.py --json                # JSON 输出
    python tools/mi_find_device.py --device Redmi        # 只看名字含 Redmi 的设备
    python tools/mi_find_device.py --cookie "serviceToken=...; userId=..."   # 复用浏览器 Cookie
    python tools/mi_find_device.py --login-only http     # 只走 HTTP 登录（不拉浏览器）

账号密码可用环境变量：MI_ACCOUNT / MI_PASSWORD；也可 --account / --password。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

FIND_URL = (
    "https://i.mi.com/find/fe/afde28eb879a2bfce1f279bb23121958ed4b0bb97f4b/"
    "phone~a9d22fe8879a2ffbe2a60c991db150468a03420aec1ad8935f11cab40dc67e5a48e164fa8b7591a5829f29dc42e3628c0599036d6f68ccf4c07d82a6b359115919b2c71c5edd598ddccd24ac23f02b97231dee74f1bcbfa115fe415e29587dc053fd1631c0cd3ebda2e035a2075fda31aad2ee35c0"
    "/0/detail"
)
API_STATUS_LIST = "https://i.mi.com/find/v3/device/status/list"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
CACHE = Path(os.environ.get("MI_FIND_CACHE", Path.home() / ".mi_find_device" / "cookies.json"))


class VerificationRequired(RuntimeError):
    """密码正确，但账号被要求二次验证（短信/通知确认）。"""

    def __init__(self, url: str):
        super().__init__("需要二次验证")
        self.url = url


class AuthExpired(RuntimeError):
    pass


def log(msg: str) -> None:
    print(msg, file=sys.stderr)


# ---------------------------------------------------------------- cookie 工具
def parse_cookie_text(text: str) -> dict[str, str]:
    """支持 'serviceToken=..; userId=..' 或 'serviceToken=..&userId=..' 两种写法。"""
    text = text.strip()
    if not text:
        return {}
    parts = re.split(r"[;&]", text)
    out = {}
    for part in parts:
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, value = part.split("=", 1)
        out[name.strip()] = value.strip()
    return out


def save_cookie_cache(cookies: dict[str, str]) -> None:
    try:
        CACHE.parent.mkdir(parents=True, exist_ok=True)
        CACHE.write_text(json.dumps(cookies, ensure_ascii=False), encoding="utf-8")
        try:
            CACHE.chmod(0o600)
        except OSError:
            pass
    except OSError as exc:  # 缓存失败不影响主流程
        log(f"[warn] 写 Cookie 缓存失败：{exc}")


def load_cookie_cache() -> dict[str, str]:
    try:
        return json.loads(CACHE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


# ---------------------------------------------------------------- HTTP 登录
def _login_page_state(session: requests.Session) -> dict[str, str]:
    resp = session.get(
        "https://account.xiaomi.com/pass/serviceLogin",
        params={"sid": "i.mi.com", "_json": "true"},
        headers={"User-Agent": UA},
        timeout=20,
    )
    body = resp.text.split("&&&START&&&", 1)[-1]
    data = json.loads(body)
    for key in ("_sign", "qs", "callback"):
        if not data.get(key):
            raise RuntimeError(f"登录页异常，缺少 {key}：{body[:200]}")
    return data


def login_http(account: str, password: str) -> dict[str, str]:
    """小米账号密码登录，成功返回 Cookie。被风控拦下则抛 VerificationRequired。"""
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    state = _login_page_state(session)
    password_hash = hashlib.md5(password.encode("utf-8")).hexdigest().upper()

    resp = session.post(
        "https://account.xiaomi.com/pass/serviceLoginAuth2",
        params={"_json": "true"},
        data={
            "user": account,
            "hash": password_hash,
            "sid": "i.mi.com",
            "callback": state["callback"],
            "qs": state["qs"],
            "_sign": state["_sign"],
            "_json": "true",
        },
        headers={"User-Agent": UA, "Referer": "https://account.xiaomi.com/"},
        timeout=20,
    )
    data = json.loads(resp.text.split("&&&START&&&", 1)[-1])

    if data.get("notificationUrl"):
        raise VerificationRequired(data["notificationUrl"])
    if data.get("result") != "ok" or int(data.get("code", -1)) != 0:
        desc = data.get("description") or data.get("desc") or resp.text[:200]
        raise RuntimeError(f"登录失败：{desc}")
    if not data.get("location"):
        raise RuntimeError(f"登录未返回跳转地址：{json.dumps(data, ensure_ascii=False)[:300]}")

    session.get(data["location"], headers={"User-Agent": UA}, timeout=20)
    session.get("https://i.mi.com/", headers={"User-Agent": UA}, timeout=20)

    cookies = {c.name: c.value for c in session.cookies if c.name in REQUIRED_COOKIES or c.domain.endswith("mi.com")}
    cookies = {k: v for k, v in cookies.items() if k in REQUIRED_COOKIES}
    if "serviceToken" not in cookies or "userId" not in cookies:
        raise RuntimeError("登录流程未拿到 serviceToken/userId，Cookie：" + ",".join(cookies))
    return cookies


# ---------------------------------------------------------------- 浏览器登录
REQUIRED_COOKIES = {
    "serviceToken",
    "userId",
    "i.mi.com_slh",
    "i.mi.com_ph",
    "i.mi.com_isvalid_servicetoken",
    "i.mi.com_istrudev",
}


def find_browser() -> str | None:
    env = os.environ.get("MI_BROWSER")
    if env and Path(env).exists():
        return env
    candidates = [
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium",
    ]
    for path in candidates:
        if Path(path).exists():
            return path
    return None


def login_browser(account: str, password: str, headless: bool = False) -> dict[str, str]:
    """用 playwright 驱动本机浏览器完成网页登录，返回 Cookie。"""
    from playwright.sync_api import sync_playwright

    exe = find_browser()
    profile = Path(os.environ.get("MI_PROFILE_DIR", Path.home() / ".mi_find_device" / "profile"))
    profile.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        kwargs = {
            "user_data_dir": str(profile),
            "headless": headless,
            "args": ["--no-first-run", "--no-default-browser-check"],
        }
        if exe:
            kwargs["executable_path"] = exe
        ctx = p.chromium.launch_persistent_context(**kwargs)
        try:
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto(FIND_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2500)

            if "account.xiaomi.com" in page.url:
                page.fill("input[name=account]", account)
                page.fill("input[name=password]", password)
                page.click("button[type=submit]")
                page.wait_for_timeout(2500)
                # 协议确认弹窗：同意并继续
                for _ in range(2):
                    btn = page.get_by_role("button", name="同意并继续")
                    if btn.count() and btn.first.is_visible():
                        btn.first.click()
                        page.wait_for_timeout(3000)
                    else:
                        break
                try:
                    page.wait_for_url(re.compile(r"^https://i\.mi\.com/"), timeout=30000)
                except Exception:
                    log("[warn] 未自动跳回 i.mi.com，当前地址：" + page.url[:120])

            page.wait_for_timeout(2000)
            cookies = {c["name"]: c["value"] for c in ctx.cookies()}
            cookies = {k: v for k, v in cookies.items() if k in REQUIRED_COOKIES}
            if "serviceToken" not in cookies or "userId" not in cookies:
                raise RuntimeError(
                    "浏览器登录未拿到 serviceToken/userId（可能仍需短信验证），当前地址：" + page.url[:160]
                )
            return cookies
        finally:
            ctx.close()


# ---------------------------------------------------------------- 接口
def build_session(cookies: dict[str, str]) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Referer": "https://i.mi.com/find/"})
    for name, value in cookies.items():
        session.cookies.set(name, value, domain=".mi.com")
    return session


def fetch_devices(session: requests.Session) -> list[dict]:
    resp = session.get(
        API_STATUS_LIST,
        params={"ts": int(time.time() * 1000)},
        timeout=20,
    )
    if resp.status_code == 401:
        raise AuthExpired(resp.text[:200])
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("result") != "ok":
        raise RuntimeError("接口返回异常：" + json.dumps(payload, ensure_ascii=False)[:300])
    return payload["data"]["deviceList"]


# ---------------------------------------------------------------- 解析输出
def _model_name(device: dict) -> str:
    try:
        raw = device["componentModelInfo"]["0"]["modelName"]["zhCN"]
        return json.loads(raw).get("modelName") or raw
    except (KeyError, TypeError, ValueError):
        return device.get("deviceType", "unknown")


def _fmt_time(ms: int | None) -> str:
    if not ms:
        return "-"
    return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")


def summarize(device: dict) -> dict:
    comp = (device.get("componentList") or [{}])[0]
    battery = comp.get("batteryInfo") or {}
    locations = (comp.get("locationList") or [{}])[0]
    points = locations.get("gpsInfoTransformed") or []
    by_type = {p.get("coordinateType"): p for p in points}
    main = by_type.get("autonavi") or (points[0] if points else {})

    return {
        "fid": device.get("fid"),
        "model": _model_name(device),
        "online": bool(comp.get("online")),
        "battery": battery.get("level"),
        "battery_time": _fmt_time(battery.get("clientUpdateTime")),
        "address": main.get("address") or locations.get("address"),
        "accuracy_m": round(main["accuracy"], 1) if main.get("accuracy") else None,
        "location_time": _fmt_time(main.get("clientUpdateTime")),
        "coords": {
            k: {"lat": v.get("latitude"), "lng": v.get("longitude")}
            for k, v in by_type.items()
        },
        "lost_mode": (device.get("lostModeInfo") or {}).get("status"),
    }


def print_device(d: dict) -> None:
    print(f"设备：{d['model']}")
    print(f"  在线：{'是' if d['online'] else '否'}")
    print(f"  电量：{d['battery'] if d['battery'] is not None else '-'}%  （{d['battery_time']}）")
    if d["address"]:
        print(f"  地址：{d['address']}")
        print(f"  定位：{d['location_time']}  精度约 {d['accuracy_m']} 米")
        gcj = d["coords"].get("autonavi")
        wgs = d["coords"].get("google")
        bd = d["coords"].get("baidu")
        if gcj:
            print(f"  GCJ-02（高德/腾讯）：{gcj['lat']:.6f}, {gcj['lng']:.6f}")
        if wgs:
            print(f"  WGS-84（GPS 原始）：{wgs['lat']:.6f}, {wgs['lng']:.6f}")
        if bd:
            print(f"  BD-09（百度）：{bd['lat']:.6f}, {bd['lng']:.6f}")
    else:
        print("  地址：暂无定位（设备可能离线或未上报位置）")


# ---------------------------------------------------------------- 主流程
def get_cookies(args) -> dict[str, str]:
    if args.cookie:
        cookies = parse_cookie_text(args.cookie)
        if "serviceToken" not in cookies:
            raise SystemExit("--cookie 里必须包含 serviceToken")
        return cookies

    cached = load_cookie_cache()
    if cached and not args.relogin:
        return cached

    account = args.account or os.environ.get("MI_ACCOUNT") or "17373200083"
    password = args.password or os.environ.get("MI_PASSWORD") or "1234qwer"

    if args.login_only == "http":
        return login_http(account, password)

    try:
        cookies = login_http(account, password)
        log("[info] HTTP 登录成功")
        return cookies
    except VerificationRequired as exc:
        log("[warn] 账号触发二次验证，改用浏览器登录")
        log("[info] 如浏览器也不通，可手动打开该地址完成验证后重跑：" + exc.url)
        if args.login_only == "http":
            raise SystemExit(1)
    except Exception as exc:  # 边缘失败降级到浏览器，不打断主流程
        log(f"[warn] HTTP 登录失败（{exc}），改用浏览器登录")

    return login_browser(account, password, headless=args.headless)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="小米查找设备：查询手机状态与 GPS")
    parser.add_argument("--account", help="小米账号（手机号/邮箱/ID）")
    parser.add_argument("--password", help="密码")
    parser.add_argument("--cookie", help="已有的 Cookie，如 'serviceToken=..; userId=..'")
    parser.add_argument("--relogin", action="store_true", help="忽略本地 Cookie 缓存，重新登录")
    parser.add_argument("--login-only", choices=["auto", "http", "browser"], default="auto",
                        help="登录方式，默认先 HTTP 再浏览器兜底")
    parser.add_argument("--headless", action="store_true", help="浏览器登录使用无头模式")
    parser.add_argument("--device", help="只输出型号/地址含该关键字的设备")
    parser.add_argument("--json", action="store_true", help="以 JSON 输出")
    args = parser.parse_args(argv)

    cookies = get_cookies(args)
    session = build_session(cookies)
    try:
        devices = fetch_devices(session)
    except AuthExpired:
        log("[warn] Cookie 已失效，重新登录")
        cookies = get_cookies(argparse.Namespace(**{**vars(args), "relogin": True, "cookie": None}))
        session = build_session(cookies)
        devices = fetch_devices(session)

    save_cookie_cache(cookies)

    rows = [summarize(d) for d in devices]
    if args.device:
        key = args.device.lower()
        rows = [r for r in rows if key in (r["model"] or "").lower() or key in (r["address"] or "").lower()]
        if not rows:
            log(f"[warn] 没有匹配 '{args.device}' 的设备")
            return 2

    payload = {"count": len(rows), "updated_at": _fmt_time(int(time.time() * 1000)), "devices": rows}
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        for i, row in enumerate(rows):
            if i:
                print()
            print_device(row)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
