"""Transport-independent Python RPC execution."""

import ast
import io
import sys
import threading
import traceback


class PythonExecutor:
    """Execute Python snippets while preserving state between requests."""

    def __init__(self, globals_dict=None, locals_dict=None):
        self.globals = dict(globals_dict or {})
        self.globals.setdefault("__name__", "__rpc_exec__")
        self.locals = locals_dict if locals_dict is not None else {}
        self.lock = threading.RLock()

    def execute(self, code):
        if not isinstance(code, str) or not code.strip():
            return {"r": "", "stdout": "", "ok": False, "error": "code is required"}

        output = io.StringIO()
        with self.lock:
            try:
                with _redirect_stdout(output):
                    result = self._execute(code)
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

    def _execute(self, code):
        tree = ast.parse(code, filename="<rpc>", mode="exec")
        if tree.body and isinstance(tree.body[-1], ast.Expr):
            prefix = ast.Module(body=tree.body[:-1], type_ignores=[])
            if prefix.body:
                exec(compile(prefix, "<rpc>", "exec"), self.globals, self.locals)
            expression = ast.Expression(tree.body[-1].value)
            return eval(compile(expression, "<rpc>", "eval"), self.globals, self.locals)

        exec(compile(tree, "<rpc>", "exec"), self.globals, self.locals)
        return self.locals.get("r", self.globals.get("r"))


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
        from IPython.lib.pretty import pretty
        return pretty(value, max_width=120)
    except ImportError:
        from pprint import pformat
        return pformat(value, width=120)