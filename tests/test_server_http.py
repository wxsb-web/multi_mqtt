import base64
import json
import subprocess
import socket
import sys
import tempfile
import unittest
import urllib.parse
import urllib.request
from pathlib import Path

import server_http


class ServerHttpTests(unittest.TestCase):
    def test_module_exports_rpc_and_executor_interfaces(self):
        # 本仓库的 HTTP RPC 只有一个实现模块 server_http.py；旧用例还要求与
        # 外部项目的 rpc/rpc.py 包导出同名符号，该包从不属于本仓库，故移除
        # 对等断言，只校验本模块自身的公共接口。
        for name in (
            "start_rpc_server",
            "RPCRequestHandler",
            "ThreadedHTTPServer",
            "WebSocket",
            "get_bmp_bytes",
            "qpsu",
            "stime",
        ):
            self.assertTrue(hasattr(server_http, name), name)
        self.assertTrue(hasattr(server_http, "rpc_executor"))
        self.assertEqual(server_http.rpc_executor.PythonExecutor().execute("1")["r"], 1)

    def test_http_response_wrapper_matches_rpc_contract(self):
        server = server_http.start_rpc_server(port=0, ip="127.0.0.1")
        try:
            code = "response.set_status(201); response.set_header('X-Test', 'ok'); response.set_data('body')"
            url = f"http://127.0.0.1:{server.server_port}/{urllib.parse.quote(code)}"
            with urllib.request.urlopen(url) as response:
                self.assertEqual(response.status, 201)
                self.assertEqual(response.headers["X-Test"], "ok")
                self.assertEqual(response.read().decode(), "body")
        finally:
            server.shutdown()
            server.server_close()
            # thread.join(timeout=2)

    def test_websocket_upgrade_is_supported(self):
        server = server_http.start_rpc_server(
            port=0,
            ip="127.0.0.1",
            websocket_handler=lambda _handler, websocket, request: None,
        )
        try:
            with socket.create_connection(("127.0.0.1", server.server_port)) as client:
                client.sendall(
                    b"GET /ws HTTP/1.1\r\n"
                    b"Host: localhost\r\n"
                    b"Upgrade: websocket\r\n"
                    b"Connection: Upgrade\r\n"
                    b"Sec-WebSocket-Version: 13\r\n"
                    b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
                )
                response = client.recv(1024).split(b"\r\n\r\n", 1)[0].decode("ascii")
            self.assertIn("HTTP/1.1 101 Switching Protocols", response)
            self.assertIn("Sec-WebSocket-Accept:", response)
        finally:
            server.shutdown()
            server.server_close()
            # thread.join(timeout=2)

    def test_module_can_be_imported_from_its_directory(self):
        result = subprocess.run(
            [sys.executable, "-c", "import server_http; print(server_http.start_rpc_server.__name__)"],
            cwd=Path(server_http.__file__).parent,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "start_rpc_server")

    def test_http_execution_and_root_redirect(self):
        server = server_http.start_rpc_server(
            port=0,
            ip="127.0.0.1",
            globals={},
            locals={},
            redirect_root="/r=3",
        )
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{server.server_port}/r%3D3"
            ) as response:
                self.assertEqual(response.status, 200)
                self.assertEqual(response.read().decode(), "3")

            class NoRedirect(urllib.request.HTTPRedirectHandler):
                def redirect_request(self, request, response, code, msg, headers, new_url):
                    return None

            opener = urllib.request.build_opener(NoRedirect())
            with self.assertRaises(urllib.error.HTTPError) as context:
                opener.open(f"http://127.0.0.1:{server.server_port}/")
            self.assertEqual(context.exception.code, 302)
            self.assertEqual(context.exception.headers["Location"], "/r=3")
        finally:
            server.shutdown()
            server.server_close()
            # thread.join(timeout=2)


class KeyPrefixAndAliasUrlTests(unittest.TestCase):
    """key 作为真正的 URL 前缀，以及 alias_url 开关（不改 executor）。"""

    def _start(self, **kwargs):
        # 本组只验证 URL 语义；持久化关闭，避免往仓库 rpc.json 写测试别名
        kwargs.setdefault('persist_aliases', False)
        server = server_http.start_rpc_server(
            port=0, ip="127.0.0.1", globals={}, locals={}, **kwargs
        )
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server.server_port

    @staticmethod
    def _get(port, path):
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as response:
            return response.read().decode()

    def _assert_403(self, port, path):
        with self.assertRaises(urllib.error.HTTPError) as context:
            urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10)
        self.assertEqual(context.exception.code, 403)

    def test_key_is_true_url_prefix_and_legacy_form_still_works(self):
        port = self._start(key="-")
        # key='-' → /-r=1+1；旧地址 /-/r=1+1 也保留
        self.assertEqual(self._get(port, "/-1%2B1"), "2")
        self.assertEqual(self._get(port, "/-/1%2B1"), "2")
        self._assert_403(port, "/1%2B1")

    def test_key_strips_surrounding_slashes(self):
        port = self._start(key="/abc/")
        self.assertEqual(self._get(port, "/abc/1%2B1"), "2")
        self.assertEqual(self._get(port, "/abc1%2B1"), "2")
        self._assert_403(port, "/1%2B1")

    def test_alias_url_keyed_requires_key_prefix(self):
        port = self._start(key="-")
        # key 处理后的代码是 add，执行器自己会展开别名
        self.assertEqual(self._get(port, "/-%23alias%20add%0Ar%3D1%2B1"), "2")
        self.assertEqual(self._get(port, "/-add"), "2")
        self._assert_403(port, "/add")

    def test_alias_url_raw_allows_bare_alias_name(self):
        port = self._start(key="-", alias_url="raw")
        self.assertEqual(self._get(port, "/-%23alias%20add%0Ar%3D1%2B1"), "2")
        # /add 免 key 前缀，等价 /-r=1+1；未注册路径仍 403
        self.assertEqual(self._get(port, "/add"), "2")
        self.assertEqual(self._get(port, "/-add"), "2")
        self._assert_403(port, "/not_registered_alias")

    def test_alias_url_rejects_unknown_value(self):
        with self.assertRaises(ValueError):
            self._start(alias_url="definitely-not-valid")


class CliArgsTests(unittest.TestCase):
    """命令行参数拼写（--basic_auth / -basic_auth / -basic-auth 同义）。"""

    def test_basic_auth_flag_spellings(self):
        for flag in ("--basic_auth", "-basic_auth", "-basic-auth"):
            args = server_http.build_arg_parser().parse_args([flag, "admin:pw"])
            self.assertEqual(args.basic_auth, "admin:pw", flag)

    def test_defaults_keep_server_unauthenticated(self):
        args = server_http.build_arg_parser().parse_args([])
        self.assertEqual(args.basic_auth, "")
        self.assertEqual(args.key, "")
        self.assertEqual(args.alias_url, "keyed")
        self.assertEqual(args.root_alias, "root")
        self.assertTrue(args.persist_aliases)
        self.assertEqual(args.port, 1144)

    def test_cli_basic_auth_value_gates_requests(self):
        # start_rpc_server 把 basic_auth 写进 RPCRequestHandler 类属性，同进程
        # 后跑的 WSGI 用例会继承它（shim 读类属性），必须还原
        old_basic_auth = server_http.RPCRequestHandler.basic_auth
        self.addCleanup(setattr, server_http.RPCRequestHandler,
                        "basic_auth", old_basic_auth)
        server = server_http.start_rpc_server(
            port=0, ip="127.0.0.1", globals={}, locals={},
            key="-", basic_auth="admin:pw",
        )
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        port = server.server_port
        url = f"http://127.0.0.1:{port}/-1%2B1"

        with self.assertRaises(urllib.error.HTTPError) as context:
            urllib.request.urlopen(url, timeout=10)
        self.assertEqual(context.exception.code, 401)

        request = urllib.request.Request(url)
        request.add_header(
            "Authorization",
            "Basic " + base64.b64encode(b"admin:pw").decode("ascii"),
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            self.assertEqual(response.read().decode(), "2")


class AliasPersistenceTests(unittest.TestCase):
    """别名持久化：rpc.json 的 http_aliases 段 + 根路径 / 显示 HTML。"""

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.config_path = Path(temp.name) / "rpc.json"
        # 预置 server_mqtt 的其它键，验证写入只合并自己的段、不覆盖别人
        self.config_path.write_text(
            json.dumps({"request_topic": "test/request"}), encoding="utf-8")
        old_path = server_http.RPC_CONFIG_PATH
        server_http.RPC_CONFIG_PATH = self.config_path
        self.addCleanup(setattr, server_http, "RPC_CONFIG_PATH", old_path)

    def _start(self, **kwargs):
        kwargs.setdefault("key", "-")
        kwargs.setdefault("alias_url", "raw")
        server = server_http.start_rpc_server(
            port=0, ip="127.0.0.1", globals={}, locals={}, **kwargs)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server.server_port

    def _get(self, port, path):
        return urllib.request.urlopen(
            f"http://127.0.0.1:{port}{path}", timeout=10)

    def _document(self):
        return json.loads(self.config_path.read_text(encoding="utf-8"))

    def _saved(self):
        return self._document().get("http_aliases", {})

    @staticmethod
    def _quote(code):
        return urllib.parse.quote(code, safe="")

    def test_alias_definition_is_persisted_and_restored(self):
        port = self._start()
        with self._get(port, "/-" + self._quote("#alias addx\nr=1+1")) as response:
            self.assertEqual(response.read().decode(), "2")
        self.assertEqual(self._saved(), {"addx": "r=1+1"})
        # 其它模块的键原样保留
        self.assertEqual(self._document()["request_topic"], "test/request")

        # 新服务器 = 新执行器：从 rpc.json 恢复后 /addx 免 key 前缀可用
        port2 = self._start()
        with self._get(port2, "/addx") as response:
            self.assertEqual(response.read().decode(), "2")

    def test_unalias_removes_persisted_entry(self):
        port = self._start()
        with self._get(port, "/-" + self._quote("#alias gonex\nr=7")):
            pass
        self.assertIn("gonex", self._saved())
        with self._get(port, "/-" + self._quote("#unalias gonex")) as response:
            self.assertIn("deleted", response.read().decode())
        self.assertNotIn("gonex", self._saved())

    def test_ephemeral_flags_skip_persistence(self):
        port = self._start()
        # 三种“不持久化”写法都只注册到内存
        for code in ("#alias eph1 !\nr=1", "#alias eph2 np\nr=2",
                     "#alias eph3!\nr=3"):
            with self._get(port, "/-" + self._quote(code)):
                pass
        for name, expected in (("eph1", "1"), ("eph2", "2"), ("eph3", "3")):
            with self._get(port, "/-" + name) as response:
                self.assertEqual(response.read().decode(), expected, name)
        self.assertEqual(self._saved(), {})

    def test_disabled_persistence_does_not_touch_file(self):
        port = self._start(persist_aliases=False)
        with self._get(port, "/-" + self._quote("#alias memx\nr=5")) as response:
            self.assertEqual(response.read().decode(), "5")
        self.assertEqual(self._saved(), {})
        with self._get(port, "/-memx") as response:
            self.assertEqual(response.read().decode(), "5")

    def test_root_path_runs_root_alias_as_html(self):
        port = self._start()
        body = ("response.set_header('Content-Type','text/html; charset=utf-8');"
                "response.set_data('<h1>apk</h1>')")
        with self._get(port, "/-" + self._quote("#alias root\n" + body)):
            pass
        self.assertIn("root", self._saved())
        with self._get(port, "/") as response:
            self.assertEqual(response.headers["Content-Type"],
                             "text/html; charset=utf-8")
            self.assertEqual(response.read().decode(), "<h1>apk</h1>")

    def test_root_path_stays_403_in_keyed_mode(self):
        # keyed 模式别名必须写在 key 之后：/ 不因 root 别名存在而放行
        port = self._start(alias_url="keyed")
        with self._get(port, "/-" + self._quote("#alias root\nr='x'")):
            pass
        with self.assertRaises(urllib.error.HTTPError) as context:
            self._get(port, "/")
        self.assertEqual(context.exception.code, 403)


if __name__ == "__main__":
    unittest.main()
