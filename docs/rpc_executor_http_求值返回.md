# rpc_executor + HTTP 求值返回

适用范围：`rpc_executor.PythonExecutor.execute()`、`server_http.RPCRequestHandler.handle_rpc()`。

以下返回规则**仅针对 HTTP 通道**。

## 1. 请求到代码

```
/<key><code>        例 key='-'：/-r=1+1
/<key>/<code>       旧式兼容
/<code>             key 为空时
```

- `key` 非空且不匹配 → 403；`code` 为空 → 400。
- 原生 handler 用 `urllib.parse.unquote`；WSGI/ASGI 垫片已解码（`path_already_decoded`）不再解。
- 执行前可选处理：`#serve-file {...}` 文件直出（仅 GET）、裸路径补引号救援（非别名调用）、首行 `#alias` 指令、别名展开。
- 请求级上下文临时注入并在执行后还原：`request`、`q`、`response`、`p`。
- 持久命名空间跨请求共享，普通变量会保留；`r` 是否保留取决于第 2 节。

## 2. `r` 的取值规则（HTTP）

HTTP **禁用最后一个表达式的自动求值返回**：执行器只按语句执行代码，不把尾表达式的值当作结果，返回值只认 `r`（以及 `response.data` / stdout）。`r` 按以下四种情况处理：

| 情况 | 全局命名空间 | 代码中的 `r` | 返回 | 是否影响后续请求 |
|---|---|---|---|---|
| 1 | 无 `r` | 未定义 | `None` | 否 |
| 2 | 无 `r` | 临时定义 `r = ...` | 本次代码中的 `r` | 否，仅本请求有效 |
| 3 | 有 `r` | `r = xxx` | 修改后的 `r` | 是，等价于修改该全局变量 |
| 4 | 有 `r` | 未修改 | 原全局 `r` | 无变化 |

- 情况 2 与注入的 `request` / `response` 相同：请求结束即从命名空间移除，不进入后续请求。
- 情况 3 直接改写持久命名空间里的 `r`，后续请求读到的就是新值。
- 情况 4 不做任何写入，直接返回持久 `r`。
- 没有定义 `r`、也没有全局 `r` 时，表达式本身的值不会被返回：`/1+1` 不会返回 `2`；需要返回请显式写 `/r=1+1`，或用 `response.set_data(...)`。
- 多语句、顶层 `await` 代码同样遵循以上四条，不存在“单表达式特判返回”。

## 3. HTTP 返回选择优先级

`handle_rpc` 依次判断，命中即止：

| 顺序 | 条件 | 返回内容 |
|---|---|---|
| 1 | `response.data is not None`（`set_data`） | 直接作 body：str 按 UTF-8 编码，否则须为 bytes |
| 2 | `r` 可用且不为 `None`（第 2 节的情况 2/3/4） | `r` 的值 |
| 3 | `output`（stdout 非空） | stdout 原文 |
| 4 | 均不命中 | `None`（`format_result` 后 body 为文本 `None`） |

要点：

- 情况 1 的 `r` 为 `None` 时视同无值，继续走 stdout 兜底，最后才返回 `None`。
- `r` 是**原始对象**，HTTP 层最后统一 `format_result`。
- 流式 `response.write()`：首次调用立即提交状态头，之后直写 socket，无 `Content-Length`，不走上述收尾逻辑。
- `response.set_status()` 改状态码；未设 `Content-Type` 时默认 `text/plain; charset=utf-8`。
- 执行失败：`raise RuntimeError(execution['error'])` → 500，body 为 traceback 文本。
- 空代码：`{"r": "", "ok": False, "error": "code is required"}`。
- `execute()` 默认 `timeout=999` 秒，在 daemon 线程执行：超时返回 `ok=False`；代码仍在跑则进入 busy，后续请求快速失败，结束后自动恢复；排队超时则本请求作废（不会延迟执行）。
- `stdout` 由执行器内部 `_redirect_stdout` 捕获，与 `r` 互相独立。

## 4. 输出格式化

`format_result(obj, width=120)`：

- `str` → 原样；
- 其它 → `IPython.lib.pretty.pretty(obj, max_width=120)`；
- 无 IPython → `pprint.pformat(width=120)`；再失败 → `repr`。

示例：`/r=dir` → 先把 `dir` 赋给 `r`，再返回 `<function dir>`；`/r=gms` → 返回全局 `gms` 对象。

## 5. 与 MQTT 通道的差异

| 项 | HTTP | MQTT |
|---|---|---|
| 尾表达式 | 不自动求值返回 | 仍保留尾表达式求值 |
| 调用 | `execute(code)` | `execute(code, format_result=True)` |
| `r` | 原始对象，最后由 `format_result` | 已是 `str`，可直接 JSON |
| 取值 | 上述优先级（`response.data` → `r` → stdout → `None`） | 回包 `execution['r']` |

## 6. 历史行为变更

- `653c9bb`：`rpc_executor` 建立即具备“尾表达式求值”REPL 语义；MQTT 一直使用 `execution['r']`。
- `38c3e05`–`d97077f`：HTTP 只读 `response.data` / `r` 变量 / stdout，**丢弃** `execution['r']`，因此表现为“只认 `r=xxx`”。
- `d57108f`（2026-10-24）：HTTP 回退链在 `response.data` 后插入 `execution['r']`，尾表达式从此优先于 `r` 变量，与 MQTT 对齐。
- 2026-11-09：HTTP 返回规则修订为**禁用尾表达式自动求值返回**，只认 `response.data` / `r` / stdout；
  `r` 按第 2 节四条处理（无全局 `r` 时为请求级，有全局 `r` 时读写该全局变量）。MQTT 通道不变。

## 7. 取回 `r` 变量值

- 无全局 `r`：`/r=gms`，仅本次请求有效；
- 有全局 `r`：`/r=gms` 会修改全局 `r`，并在本次返回新值；
- 或用 `response.set_data(...)` 显式指定返回。
