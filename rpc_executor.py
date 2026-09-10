"""Transport-independent Python RPC execution."""

import ast
import asyncio
import io
import sys
import threading
import textwrap
import traceback


class PythonExecutor:
    """Execute Python snippets while preserving state between requests."""

    def __init__(self, globals_dict=None, locals_dict=None, main_loop=None):
        self.globals = dict(globals_dict or {})
        self.globals.setdefault("__name__", "__rpc_exec__")
        self.locals = locals_dict if locals_dict is not None else {}
        self.main_loop = main_loop
        self.lock = threading.RLock()

    def execute(self, code,globals_dict=None,locals_dict=None):
        '''修改 PythonExecutor 支持环境注入  用于每次请求注入上下文变量'''
        if not isinstance(code, str) or not code.strip():
            return {"r": "", "stdout": "", "ok": False, "error": "code is required"}

        output = io.StringIO()
        with self.lock:
            try:
                with _redirect_stdout(output):
                    result = self._execute(code,globals_dict=globals_dict,locals_dict=locals_dict)
                return {
                    "r": result,
                    "stdout": output.getvalue(),
                    "ok": True,
                }
            except Exception:
                return {
                    "r": None,
                    "stdout": output.getvalue(),
                    "ok": False,
                    "error": traceback.format_exc(),
                }

    def _execute(self, code,globals_dict=None,locals_dict=None):
        tree = ast.parse(code, filename="<rpc>", mode="exec")
        
        is_await=False
        try:
            compiled_tree = compile(tree, "<rpc>", "exec")
        except SyntaxError:
            if "await" not in code:
                raise
            else:is_await=True
             
        if isinstance(globals_dict,dict):self.globals.update(globals_dict)
        if isinstance(locals_dict,dict):self.locals.update(locals_dict)
        
        if is_await:return self._execute_awaitable(code)
        
        
        if tree.body and isinstance(tree.body[-1], ast.Expr):
            prefix = ast.Module(body=tree.body[:-1], type_ignores=[])
            if prefix.body:
                exec(compile(prefix, "<rpc>", "exec"), self.globals, self.locals)
            expression = ast.Expression(tree.body[-1].value)
            return eval(compile(expression, "<rpc>", "eval"), self.globals, self.locals)

        exec(compiled_tree, self.globals, self.locals)
        return self.locals.get("r", self.globals.get("r"))

    def _execute_awaitable(self, code):
        indented_code = textwrap.indent(code, "    ")
        try:
            wrapped_tree = ast.parse(f"async def _rpc_async():\n{indented_code}")
            body = wrapped_tree.body[0].body
            is_single_expr = len(body) == 1 and isinstance(body[0], ast.Expr)
        except SyntaxError:
            is_single_expr = False

        if is_single_expr:
            async_code = f"async def __rpc_async__():\n    return ({code})"
        else:
            async_code = f"async def __rpc_async__():\n{indented_code}\n    return locals()"

        async_globals = self.globals.copy()
        async_globals.update(self.locals)
        exec(async_code, async_globals, self.locals)
        coroutine_function = self.locals.pop("__rpc_async__")
        result = self._run_coroutine(coroutine_function())
        if isinstance(result, dict):
            self.locals.update(result)
            return self.locals.get("r", self.globals.get("r"))
        if result is not None:
            self.locals["r"] = result
        return result

    def _run_coroutine(self, coroutine):
        if self.main_loop and self.main_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coroutine, self.main_loop)
            return future.result()

        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coroutine)
        finally:
            loop.close()


class _redirect_stdout:
    def __init__(self, stream):
        self.stream = stream
        self.previous = None

    def __enter__(self):
        self.previous = sys.stdout
        sys.stdout = self.stream
        return self.stream

    def __exit__(self, exc_type, exc_value, traceback_info):
        sys.stdout = self.previous


def format_result(value):
    if isinstance(value, str):
        return value
    try:
        import importlib
        pretty = importlib.import_module("IPython.lib.pretty").pretty
        return pretty(value, max_width=120)
    except ImportError:
        from pprint import pformat
        return pformat(value, width=120)