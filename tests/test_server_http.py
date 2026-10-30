import subprocess
import socket
import sys
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


if __name__ == "__main__":
    unittest.main()
