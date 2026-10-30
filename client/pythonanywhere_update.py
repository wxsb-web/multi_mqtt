#!/usr/bin/env python3
"""把本地 multi_mqtt 当前代码同步到 .pythonanywhere.com 并热重载。

用法:
    python client/pythonanywhere_update.py https://suishou.pythonanywhere.com/ \
        /home/suishou/multi_mqtt --key -                         # 同步整仓并热重载
    python client/pythonanywhere_update.py --check --key -       # 只对比不上传
    python client/pythonanywhere_update.py --local-root client \
        https://ip:port/ /other/dir                              # 只推 client 子树

地址形态（server_http 的 key，本脚本用 --key 传入）:
    无 key:   https://<host>/1+1
    key='-':  https://<host>/-1+1；已注册别名还可走 https://<host>/add
    上传发生在重载之前，线上可能还是旧 key（典型是首次推 '-' 时线上仍为
    空 key），所以脚本会先探测线上实际生效的 key，再决定同步用哪种 URL；
    SIGHUP 之后则改用 --key 指定的新 key 轮询与验证。

流程（全部走 HTTP RPC 通道，复用 client/client_http.py）:
    1. 收集本地源码文件（按扩展名/黑名单过滤，跳过 .git、__pycache__、*.db 等）
    2. 与远程逐文件 sha256 对比，只上传有差异的文件（分块 + 上传后 sha 校验）
    3. 上传后先 py_compile 全量语法检查，然后向 uWSGI master（容器 pid 1）
       发 SIGHUP 触发整站优雅重载——等价于控制台 Reload 按钮，master 才会
       重新从磁盘 import 全部模块（REPL 命名空间与别名注册表被清空，属预期）。
       两个无效/有害的替代方案（均实测过）：
       * 杀 worker：worker 由 master fork，respawn 出的副本仍继承 master
         启动时已 import 的旧模块，磁盘新代码不会生效；
       * exec 热替换运行中模块：uWSGI 持有的旧 application 闭包体按旧
         签名调用全局（如 _WSGIHandlerShim(environ) 单参数），与新类
         签名不兼容会导致所有 RPC 请求 500。
    4. 验证 1+1 / ALIAS_KEYWORDS / alias 开关 / #alias 全链路。

WSGI 入口 /var/www/{u}_pythonanywhere_com_wsgi.py 无需改动:
    from server_http_wsgi import application
"""
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
# 仓库根：本脚本在 client/ 下，而线上 /home/<u>/multi_mqtt 是整仓部署
# （根目录有 server_http.py，client/ 是子目录），默认同步仓库根；只想推
# client 子树时用 --local-root client，或把远程目录指到 .../multi_mqtt/client。
REPO_ROOT = os.path.dirname(_HERE)
for _p in (_HERE, REPO_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    import urllib3
    urllib3.disable_warnings()
except Exception:
    pass

# 复用 client_http 的分块上传 / sha 校验 / RPC 调用
from client_http import (
    _rpc_call, _rpc_json, _rpc_list_dir,
    _rpc_sha256_remote, _rpc_delete_file, _upload_one_file,
)

DEFAULT_USER=''
DEFAULT_BASE =f'https://{DEFAULT_USER}.pythonanywhere.com/'
DEFAULT_REMOTE_DIR=f'/home/{DEFAULT_USER}/multi_mqtt'

EXCLUDE_DIRS = {'.git', '.trae', '__pycache__', '.pytest_cache', '.idea',
                '.vscode', '.stfolder'}
EXCLUDE_FILES = {
    'rpc.json', 'client_history.db', 'cmd_client_history.db',
    'realtime_editor.txt',    # .gitignore 里的本地运行产物
    'glm5.3=全库审计报告.md',  # 本地审计报告，不属于项目代码
}
INCLUDE_EXTS = {'.py', '.sh', '.md', '.txt', '.bat'}

PROXIES = {}  # 直连，不走环境代理


def collect_local(local_root):
    """返回 {相对路径posix: (绝对路径, 大小, sha256)}，只含源码类文件。"""
    import hashlib
    files = {}
    for dp, dns, fns in os.walk(local_root):
        dns[:] = [d for d in dns if d not in EXCLUDE_DIRS]
        for fn in fns:
            if fn in EXCLUDE_FILES or fn.endswith('.db'):
                continue
            ext = os.path.splitext(fn)[1].lower()
            if fn not in ('.gitattributes', '.gitignore') and ext not in INCLUDE_EXTS:
                continue
            abs_p = os.path.join(dp, fn)
            rel = os.path.relpath(abs_p, local_root).replace(os.sep, '/')
            with open(abs_p, 'rb') as f:
                body = f.read()
            files[rel] = (abs_p, len(body), hashlib.sha256(body).hexdigest())
    return files


def sync_files(base, local_root, remote_dir, timeout, check_only=False, key=''):
    """对比并上传差异文件，返回 (已上传列表, 跳过列表, 失败列表)。"""
    local_files = collect_local(local_root)
    remote_files = _rpc_list_dir(base, remote_dir, proxies=PROXIES,
                                 timeout=timeout, key=key)

    to_upload, unchanged, failed = [], [], []
    for rel in sorted(local_files):
        abs_p, size, sha = local_files[rel]
        remote_path = remote_dir.rstrip('/') + '/' + rel
        if rel in remote_files:
            if remote_files[rel] == size:
                rsha = _rpc_sha256_remote(base, remote_path,
                                          proxies=PROXIES, timeout=timeout,
                                          key=key)
                if rsha == sha:
                    unchanged.append(rel)
                    continue
        to_upload.append((rel, abs_p, size, remote_path))

    total = sum(s for _r, _a, s, _p in to_upload)
    print(f"[sync] local={len(local_files)} remote={len(remote_files)} "
          f"upload={len(to_upload)} ({total} B) unchanged={len(unchanged)}")
    for rel, _a, _s, _p in to_upload:
        print(f"  [upload] {rel}")
    if check_only:
        return [], unchanged, failed

    for i, (rel, abs_p, size, remote_path) in enumerate(to_upload, 1):
        try:
            _upload_one_file(base, abs_p, remote_path, size,
                             chunk_size=1024 * 1024, show_speed=0,
                             speed_threshold=1024 * 1024, proxies=PROXIES,
                             timeout=timeout, verify_file=1, key=key)
            print(f"  [{i}/{len(to_upload)}] done: {rel}")
        except Exception as e:
            print(f"  [{i}/{len(to_upload)}] FAILED: {rel}: {e}")
            failed.append((rel, e))
    return [r for r, _a, _s, _p in to_upload], unchanged, failed


def _probe_key(base, timeout, key):
    """探测线上当前真正生效的 URL 前缀，返回 '' 或 key。

    同步发生在重载之前：首次把 key 推上线时线上还是旧的无前缀地址，之后
    再跑线上已是新 key。两种状态都要能上传，所以按线上实际响应探测而不是
    写死。探测只发 1+1 这个无副作用表达式：走错形态只会拿到 403 或语法
    错误（-1+1），不会产生任何副作用。
    """
    for candidate in ([key, ''] if key else ['']):
        try:
            resp = _rpc_call(base, '1+1', proxies=PROXIES, timeout=timeout,
                             key=candidate)
        except Exception:
            continue
        if resp.status_code == 200 and resp.text.strip() == '2':
            return candidate
    raise RuntimeError(
        f"无法探测线上 key（{base}）：试过 {[key, ''] if key else ['']} 都失败；"
        "线上已启用 key 时请用 --key 指定，例如 --key -")


def syntax_check(base, remote_dir, timeout, key=''):
    """远程 py_compile 全量检查上传后的 .py，坏文件直接抛错不进入重载。"""
    code = (
        "import py_compile\n"
        f"__root = {remote_dir!r}\n"
        "import os\n"
        "__bad = []\n"
        "for __dp, __dn, __fn in os.walk(__root):\n"
        "    if '.git' in __dp or '__pycache__' in __dp:\n"
        "        continue\n"
        "    for __f in __fn:\n"
        "        if not __f.endswith('.py'):\n"
        "            continue\n"
        "        __p = os.path.join(__dp, __f)\n"
        "        try:\n"
        "            py_compile.compile(__p, doraise=True)\n"
        "        except Exception as __e:\n"
        "            __bad.append((__p, str(__e)))\n"
        "__result = __bad\n"
    )
    from client_http import _server_code
    bad = _rpc_json(base, _server_code(code), proxies=PROXIES, timeout=timeout,
                    key=key)
    if bad:
        raise RuntimeError(f"remote py_compile failed: {bad}")
    print("[check] remote py_compile all OK")


def activate(base, timeout, live_key='', post_key=''):
    """向 uWSGI master 发 SIGHUP 触发整站优雅重载（等价控制台 Reload 按钮）。

    live_key: 重载前线上生效的 key（发 SIGHUP 的请求本身还走旧地址）；
    post_key: 重载后新代码的 key（轮询恢复必须用新地址，否则会一直 403/500）。

    pythonanywhere 的 worker 由 master fork：杀 worker 只会 fork 出继承旧
    模块的副本，磁盘新代码不会生效；必须重载 master。master 即容器 pid 1，
    先校验父进程 cmdline 含 uWSGI master 再发信号，避免误伤其它 supervisor。
    """
    code = (
        "import os, signal\n"
        "ppid = os.getppid()\n"
        "name = open(f'/proc/{ppid}/cmdline','rb').read()\n"
        "if b'uWSGI master' not in name:\n"
        "    raise RuntimeError(f'parent {ppid} is not uWSGI master: {name!r}')\n"
        "os.kill(ppid, signal.SIGHUP)\n"
        "__result = f'SIGHUP sent to uWSGI master pid {ppid}'\n"
    )
    from client_http import _server_code
    try:
        print("[reload]", _rpc_json(base, _server_code(code),
                                    proxies=PROXIES, timeout=timeout,
                                    key=live_key))
    except Exception as e:
        # master 重载会切断在途请求，连接中断属预期
        print(f"[reload] reload 请求中断（预期）: {type(e).__name__}")
    for i in range(60):
        time.sleep(2)
        try:
            # 新代码可能换过 key，必须按新地址探测；'1+1' 而不是 '1'，
            # 这样 key='-' 时 /-1+1 才执行 1+1 而不是把 -1 当表达式。
            resp = _rpc_call(base, "1+1", proxies=PROXIES, timeout=15,
                             key=post_key)
            if resp.status_code == 200 and resp.text.strip() == "2":
                print(f"[reload] web app back after {(i + 1) * 2}s")
                return
        except Exception:
            continue
    raise RuntimeError("reload 后 120s 内未恢复；请到 pythonanywhere 控制台手动点 Reload")


def verify(base, timeout, key=''):
    """验证新代码在线上生效: r 回退 / 关键字 list / alias 全链路。"""
    def get(code):
        resp = _rpc_call(base, code, proxies=PROXIES, timeout=timeout, key=key)
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
        return resp.text.strip()

    checks = [
        ("expr r-fallback", "1+1", "2"),
        ("keyword list live", "server_http.rpc_executor.ALIAS_KEYWORDS", "'alias'"),
        # 激活哨兵：旧版 executor 没有该属性——防止“master 未真正重载、
        # 仍在跑旧内存代码”的情况蒙混过关（worker fork 不重新 import 磁盘）
        ("alias switch live", "RPCRequestHandler.executor.alias", "True"),
    ]
    for name, code, expect in checks:
        got = get(code)
        if expect not in got:
            raise RuntimeError(f"verify[{name}] expect {expect!r} got {got!r}")
        print(f"[verify] {name}: {got[:80]}")

    # alias 全链路: 定义 → 使用 → 列表 → 删除（不在生产留测试别名）
    got = get("#alias update_selfcheck\n40+2")
    if "42" not in got:
        raise RuntimeError(f"verify[alias define] got {got!r}")
    got = get("update_selfcheck")
    if "42" not in got:
        raise RuntimeError(f"verify[alias use] got {got!r}")
    got = get("#alias")
    if "update_selfcheck" not in got:
        raise RuntimeError(f"verify[alias list] got {got!r}")
    get("#unalias update_selfcheck")
    print("[verify] alias define/use/list/unalias 全链路 OK")


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description='同步本地 multi_mqtt 到 pythonanywhere 并通过 SIGHUP 热重载',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('base', nargs='?', default=DEFAULT_BASE,
                        help='站点根地址，如 https://suishou.pythonanywhere.com/')
    parser.add_argument('remote_dir', nargs='?', default=DEFAULT_REMOTE_DIR,
                        help='远程项目目录')
    parser.add_argument('--key', '-key', '-k', default='',
                        help="线上 URL 前缀（server_http 的 key），如 -")
    parser.add_argument('--local-root', '-local-root', default=REPO_ROOT,
                        help="本地待同步根目录（默认仓库根；只推 client 用 client）")
    parser.add_argument('--check', action='store_true', help='只对比不上传')
    parser.add_argument('--force-reload', action='store_true',
                        help='即使根目录 .py 无变化也强制重载')
    args = parser.parse_args()
    base, remote_dir = args.base, args.remote_dir
    new_key = args.key
    check_only = args.check
    force_reload = args.force_reload
    local_root = os.path.abspath(args.local_root)
    timeout = 60

    print(f"{__file__} {local_root} -> {base}  remote_dir={remote_dir}"
          f"  key={new_key!r}{'  CHECK-ONLY' if check_only else ''}")
    # 扫清 1+1 这类无副作用请求，判断线上此刻认哪种地址
    live_key = _probe_key(base, timeout, new_key)
    print(f"[sync] live key={live_key!r}  (上传后新 key={new_key!r})")
    uploaded, unchanged, failed = sync_files(base, local_root, remote_dir,
                                             timeout, check_only=check_only,
                                             key=live_key)
    if check_only:
        return
    if failed:
        raise SystemExit(f"{__file__} {len(failed)} 个文件上传失败，中止")

    # 清理本脚本此前版本可能留下的探测文件
    try:
        _rpc_delete_file(base, remote_dir.rstrip('/') + '/.upload_test',
                         proxies=PROXIES, timeout=timeout, key=live_key)
    except Exception:
        pass

    # 根目录任意 .py 变化都可能影响 app imports（不只三个核心模块），
    # 需要 respawn；tests/、client/ 等子目录不影响运行中的 web app。
    root_py_changed = force_reload or any(
        u.endswith('.py') and '/' not in u for u in uploaded)
    if not root_py_changed:
        print("[reload] 运行时代码无变化，跳过 worker 重启")
        verify(base, timeout, key=live_key)
        print(f"{__file__} done")
        return

    syntax_check(base, remote_dir, timeout, key=live_key)
    activate(base, timeout, live_key=live_key, post_key=new_key)
    time.sleep(1)
    verify(base, timeout, key=new_key)
    print(f"{__file__} done (uWSGI master reloaded)")


if __name__ == '__main__':
    main()
