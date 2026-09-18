#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
server_http_wsgi.py — 把 server_http.py 的 RPC 处理器包装成 WSGI 应用。

设计原则：不重写任何 RPC 逻辑。
    * 启动时调用 server_http.start_rpc_server(..., listen=False)，
      只做 RPCRequestHandler 的类级初始化（key / executor / favicon / ...），
      不做 socket 绑定。
    * 请求时用一个 duck-typed shim 冒充 BaseHTTPRequestHandler，把请求整个
      丢给 RPCRequestHandler.handle_rpc(self)。
      RPC 语义（response/p、r 变量、favicon、错误兜底、持久命名空间）
      完全由 server_http.py 负责。

部署：
    gunicorn -w 1 -b 0.0.0.0:6080 server_http_wsgi:application
    uwsgi --http :6080 --module server_http_wsgi:application --enable-threads
    python3 server_http_wsgi.py        # 本地调试（多线程 wsgiref）

注意：
    * RPC 的持久命名空间是进程内单例，请使用单 worker（-w 1）。
    * WSGI 不支持 WebSocket 升级；需要 /ws 请继续用 server_http.py。
"""

import io
import os
import sys
import time
import traceback
import socketserver
import http.client as _http_client
from wsgiref.simple_server import WSGIServer, WSGIRequestHandler

import server_http
from server_http import RPCRequestHandler


# ---------------------------------------------------------------------------
# BaseHTTPRequestHandler 的最小模拟
# ---------------------------------------------------------------------------

class _WSGIWFile:
    """冒充 handler.wfile，只把写入的字节收集到内存。"""

    __slots__ = ('_buf',)

    def __init__(self):
        self._buf = io.BytesIO()

    def write(self, data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        self._buf.write(data)

    def flush(self):
        pass

    def getvalue(self):
        return self._buf.getvalue()


class _WSGIHandlerShim:
    """让 RPCRequestHandler.handle_rpc(self) 在 WSGI 下原样运行的薄垫片。

    handle_rpc 只依赖下列属性 / 方法，全部按 BaseHTTPRequestHandler 的语义提供；
    RPC 逻辑一行不改。
    """

    def __init__(self, environ):
        self.environ = environ
        self.command = environ.get('REQUEST_METHOD', 'GET')

        self.client_address = (
            environ.get('REMOTE_ADDR', ''),
            int(environ.get('REMOTE_PORT') or 0),
        )

        # 与 BaseHTTPRequestHandler.path 对齐：PATH + '?' + QUERY
        qs = environ.get('QUERY_STRING', '')
        path = environ.get('PATH_INFO', '') or ''
        self.path = path + ('?' + qs if qs else '')

        # 把 environ 里的请求头还原成类似 handler.headers 的 dict
        headers = {}
        for name, value in environ.items():
            if name.startswith('HTTP_'):
                headers[name[5:].replace('_', '-')] = value
        if environ.get('CONTENT_TYPE'):
            headers['Content-Type'] = environ['CONTENT_TYPE']
        if environ.get('CONTENT_LENGTH'):
            headers['Content-Length'] = environ['CONTENT_LENGTH']
        self.headers = headers

        # 从 RPCRequestHandler 同步类级状态
        self.key = RPCRequestHandler.key
        self.executor = RPCRequestHandler.executor

        # 响应累积
        self._status = 200
        self._headers = []          # list[(name, value)]
        self.wfile = _WSGIWFile()

    # ---- BaseHTTPRequestHandler 兼容方法 ---------------------------------

    def send_response(self, code, message=None):
        self._status = int(code)

    def send_header(self, keyword, value):
        self._headers.append((keyword, str(value)))

    def end_headers(self):
        pass

    def send_error(self, code, message=None, explain=None):
        # handle_rpc 在 403 / 400 / 500 兜底时会调用它
        self._status = int(code)
        self.wfile.write(f"{code} {message or ''}".encode('utf-8'))

    def log_message(self, fmt, *args):
        # 避免占用共享 stdout / 触发 Windows 控制台阻塞
        pass


# ---------------------------------------------------------------------------
# WSGI application 构造
# ---------------------------------------------------------------------------

def _build_wsgi_application():
    """基于已初始化的 RPCRequestHandler 构造 WSGI application。"""

    def application(environ, start_response):
        method = environ.get('REQUEST_METHOD', 'GET').upper()
        path = environ.get('PATH_INFO', '') or ''

        # 只放行 GET/POST —— 与 server_http 的 do_GET / do_POST 一致
        if method not in ('GET', 'POST'):
            body = b'Method Not Allowed'
            start_response('405 Method Not Allowed', [
                ('Content-Type', 'text/plain; charset=utf-8'),
                ('Content-Length', str(len(body))),
                ('Allow', 'GET, POST'),
            ])
            return [body]

        # favicon 分支（对应 do_GET 中的同名分支）
        if path == '/favicon.ico' and RPCRequestHandler.favicon_bytes:
            body = RPCRequestHandler.favicon_bytes
            start_response('200 OK', [
                ('Content-Type', 'image/x-icon'),
                ('Cache-Control', 'max-age=86400'),
                ('Content-Length', str(len(body))),
            ])
            return [body]

        # 根路径重定向（对应 do_GET 中的同名分支）
        if path == '/' and RPCRequestHandler.redirect_root:
            start_response('302 Found', [
                ('Location', RPCRequestHandler.redirect_root),
                ('Content-Length', '0'),
            ])
            return [b'']

        # 其余路径全部交给原装的 handle_rpc
        shim = _WSGIHandlerShim(environ)
        try:
            RPCRequestHandler.handle_rpc(shim)
        except Exception:
            traceback.print_exc()

        body = shim.wfile.getvalue()
        headers = list(shim._headers)
        names = {k.lower() for k, _ in headers}
        if 'content-type' not in names:
            headers.append(('Content-Type', 'text/plain; charset=utf-8'))
        if 'content-length' not in names:
            headers.append(('Content-Length', str(len(body))))

        reason = _http_client.responses.get(shim._status, 'Unknown')
        start_response(f"{shim._status} {reason}", headers)
        return [body]

    return application


# ---------------------------------------------------------------------------
# 启动：只初始化 RPCRequestHandler，不做 socket 绑定
# ---------------------------------------------------------------------------

server_http.start_rpc_server(
    port=int(os.environ.get('RPC_PORT', '6080')),
    ip='0.0.0.0',
    key='',
    globals=globals(),
    locals=locals(),
    listen=False,           # 关键：只初始化状态
)

application = _build_wsgi_application()


# ---------------------------------------------------------------------------
# 本地调试：多线程 WSGI 服务器 + 快速 Ctrl+C
# ---------------------------------------------------------------------------

class _ThreadingWSGIServer(socketserver.ThreadingMixIn, WSGIServer):
    daemon_threads = True
    allow_reuse_address = True


class _QuietHandler(WSGIRequestHandler):
    # wsgiref 默认 HTTP/1.0；保持默认，避免 keep-alive 拖住线程
    def log_message(self, fmt, *args):
        sys.stdout.write("[RPC-WSGI] %s %s %s\n" % (
            time.strftime('%H:%M:%S'), self.address_string(), fmt % args))
        sys.stdout.flush()

    def log_error(self, fmt, *args):
        self.log_message(fmt, *args)


if __name__ == '__main__':
    host = os.environ.get('RPC_HOST', '0.0.0.0')
    port = int(os.environ.get('RPC_PORT', '6080'))

    print(f"[RPC-WSGI] serving on http://{host}:{port}/", flush=True)
    httpd = _ThreadingWSGIServer((host, port), _QuietHandler)
    httpd.set_app(application)
    try:
        httpd.serve_forever(poll_interval=0.2)   # ← 200ms，Ctrl+C 立刻响应
    except KeyboardInterrupt:
        print("\n[RPC-WSGI] bye", flush=True)
    finally:
        httpd.server_close()