"""与传输层无关的 Python RPC 执行器。"""

import ast
import asyncio
import io
import sys
import threading
import textwrap
import traceback


class PythonExecutor:
    """在多次请求之间保持状态的 Python 代码片段执行器。

    命名空间模型
    ------------
    ``self.globals_dict`` 和 ``self.locals_dict`` 指向**同一个**字典对象。
    这是 CPython 的 ``exec`` 语义所要求的：由 ``exec`` 创建出来的函数，
    在解析其全局名 / 自由名时，只会在传给 ``exec`` 的 *globals* 字典里
    查找——永远不会查 *locals* 字典。如果两者是不同对象，那么所有模块级
    的名字（import、赋值、def）在由被执行代码定义的函数内部都会变得不可
    见，例如::
        g = 3
        def fa(): return g    # 若 globals/locals 分离，这里会 NameError

    因此公开 API（``__init__`` 和 ``execute``）只暴露一个 ``globals``
    参数；``locals_dict`` 在内部作为别名存在，以便两个名字都可被内省 /
    兼容使用。
    
ipy中可以验证 globals()==locals()
Out[11]: True

如果要函数内部执行 可以直接赋值  self.locals_dict 绕过   
    """

    def __init__(self, globals=None, main_loop=None):
        # 若调用方提供了字典，则直接使用它，这样外部对它的修改依然可见；
        # 否则新建一个空字典。
        if globals is not None:
            self.globals_dict = globals
        else:
            self.globals_dict = {}

        self.globals_dict.setdefault("__name__", "__rpc_exec__")

        # 必须指向同一个对象（原因见类文档字符串）。
        self.locals_dict = self.globals_dict

        self.main_loop = main_loop
        self.lock = threading.RLock()

    # ------------------------------------------------------------------ #
    # 公开 API
    # ------------------------------------------------------------------ #

    def execute(self, code, globals=None):
        """执行 ``code``。

        ``globals`` 若提供，应是一个包含**请求级**上下文变量的映射。
        这些变量只在本次调用期间注入共享命名空间，执行完毕后会被还原
        （或删除），因此不会泄漏到持久的 REPL 状态中。
        """
        if not isinstance(code, str) or not code.strip():
            return {"r": "", "stdout": "", "ok": False, "error": "code is required"}

        saved = {}
        injected = []
        if isinstance(globals, dict):
            for k, v in globals.items():
                if k in self.globals_dict:
                    saved[k] = self.globals_dict[k]
                self.globals_dict[k] = v
                injected.append(k)

        output = io.StringIO()
        try:
            with self.lock:
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
        finally:
            # 还原 / 删除请求级名字。
            for k in injected:
                if k in saved:
                    self.globals_dict[k] = saved[k]
                else:
                    self.globals_dict.pop(k, None)

    # ------------------------------------------------------------------ #
    # 内部执行逻辑
    # ------------------------------------------------------------------ #

    def _execute(self, code):
        # 顶层的 ``await`` 会让 ``ast.parse(..., mode="exec")`` 抛出
        # SyntaxError；这种情况下回退到异步路径。
        try:
            tree = ast.parse(code, filename="<rpc>", mode="exec")
        except SyntaxError:
            if "await" in code:
                return self._execute_awaitable(code)
            raise

        # 裸 ``await`` 同样会让 ``compile`` 抛出。
        try:
            compiled_tree = compile(tree, "<rpc>", "exec")
        except SyntaxError:
            if "await" in code:
                return self._execute_awaitable(code)
            raise

        # REPL 语义：若最后一条语句是裸表达式，则求值并返回其结果。
        if tree.body and isinstance(tree.body[-1], ast.Expr):
            prefix = ast.Module(body=tree.body[:-1], type_ignores=[])
            if prefix.body:
                exec(
                    compile(prefix, "<rpc>", "exec"),
                    self.globals_dict,
                    self.locals_dict,
                )
            expression = ast.Expression(tree.body[-1].value)
            return eval(
                compile(expression, "<rpc>", "eval"),
                self.globals_dict,
                self.locals_dict,
            )

        exec(compiled_tree, self.globals_dict, self.locals_dict)
        # 若代码片段没有以表达式结尾，则回退到用户自己设置的 ``r``。
        return self.locals_dict.get("r", self.globals_dict.get("r"))

    def _execute_awaitable(self, code):
        indented_code = textwrap.indent(code, "    ")

        # 检测函数体是否为单个表达式，以便用 ``return (expr)``，
        # 从而对 await 也保留 REPL 行为。
        try:
            wrapped_tree = ast.parse(f"async def _rpc_async():\n{indented_code}")
            body = wrapped_tree.body[0].body
            is_single_expr = len(body) == 1 and isinstance(body[0], ast.Expr)
        except SyntaxError:
            is_single_expr = False

        if is_single_expr:
            async_code = f"async def __rpc_async__():\n    return ({code})"
        else:
            async_code = (
                f"async def __rpc_async__():\n{indented_code}\n"
                f"    return locals()"
            )

        exec(async_code, self.globals_dict, self.locals_dict)
        coroutine_function = self.locals_dict.pop("__rpc_async__")
        result = self._run_coroutine(coroutine_function())

        if isinstance(result, dict):
            # 多语句函数体：把异步函数内部的局部变量合并回共享命名空间，
            # 然后取出 ``r``。
            self.locals_dict.update(result)
            return self.locals_dict.get("r", self.globals_dict.get("r"))
        if result is not None:
            self.locals_dict["r"] = result
        return result

    # ------------------------------------------------------------------ #
    # 事件循环相关
    # ------------------------------------------------------------------ #

    def _run_coroutine(self, coroutine):
        # 若提供的主事件循环正在运行，就把协程调度到那里执行。
        # 警告：如果调用线程正是该循环所在的线程，这里会死锁。这种
        # 情况下，调用方应改为直接 await 该协程，或从其他线程调用本
        # 执行器。
        if self.main_loop is not None and self.main_loop.is_running():
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