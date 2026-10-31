"""rpc_executor code alias 功能的独立测试。

运行方式（任选其一）:
    python tests/test_rpc_alias.py
    python -m unittest tests.test_rpc_alias -v
    pytest tests/test_rpc_alias.py -v

覆盖: #alias 增删查改、#unalias 删除、关键字 list 自定义（如 #a / #una）、
名字与语法校验、整段精确匹配替换、定义即执行、请求级 globals 透传与还原、
以及 server_http HTTP 通道端到端（含 %23 编码与 execution['r'] 回退）。
"""
import os
import sys
import unittest
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import rpc_executor
from rpc_executor import ALIAS_KEYWORDS, UNALIAS_KEYWORDS, PythonExecutor


class AliasParamsModeTests(unittest.TestCase):
    """params 模式：签名声明可绑定集 + 默认值，参数走请求级注入管线。"""

    def setUp(self):
        self.ex = PythonExecutor()

    def test_registration_record_and_binding(self):
        res = self.ex.execute("#alias add(x, y=3)\nint(x) + int(y)")
        self.assertTrue(res["ok"], res)
        self.assertIn("alias", res)
        record = self.ex.aliases["add"]
        self.assertEqual(record["mode"], "params")
        self.assertEqual(record["signature"], "x, y=3")
        self.assertEqual(record["params"], ["x", "y"])
        self.assertEqual(record["defaults"], {"y": 3})
        self.assertEqual(record["code"], "int(x) + int(y)")

        self.assertEqual(self.ex.execute("add", globals={"x": 4})["r"], 7)
        self.assertEqual(
            self.ex.execute("add", globals={"x": 4, "y": 10})["r"], 14)

    def test_missing_and_unknown_params(self):
        self.ex.execute("#alias add(x, y=3)\nint(x) + int(y)")
        missing = self.ex.execute("add")
        self.assertFalse(missing["ok"])
        self.assertIn("缺少必填参数", missing["error"])
        self.assertIn("add(x, y=3)", missing["error"])

        unknown = self.ex.execute("add", globals={"x": 1, "y": 2, "z": 3})
        self.assertFalse(unknown["ok"])
        self.assertIn("未知参数", unknown["error"])
        self.assertIn("可绑定", unknown["error"])

    def test_lenient_mode_reports_warnings(self):
        self.ex.execute("#alias add(x, y=3)\nint(x) + int(y)")
        self.ex.params_lenient = True
        res = self.ex.execute("add", globals={"x": 1, "z": 9})
        self.assertTrue(res["ok"], res)
        self.assertEqual(res["r"], 4)                  # 未知 z 被忽略
        self.assertEqual(len(res["warnings"]), 1)
        self.assertIn("未知参数", res["warnings"][0])

    def test_params_never_leak_into_namespace(self):
        self.ex.execute("#alias add(x, y=3)\nint(x) + int(y)")
        self.ex.execute("add", globals={"x": 4})
        for name in ("x", "y", "_alias_arg"):
            self.assertNotIn(name, self.ex.globals_dict, name)

    def test_arg_form_is_orthogonal_to_params(self):
        # `add=5` 的参数进 _alias_arg，绝不参与 params 注入（alias.md 4.4）
        self.ex.execute("#alias add(x, y=3)\nx + y")
        res = self.ex.execute("add=5")
        self.assertFalse(res["ok"])
        self.assertIn("NameError", res["error"])

    def test_quoted_non_identifier_param_name(self):
        res = self.ex.execute('#alias zh("选项A"=1)\nr = 选项A')
        self.assertTrue(res["ok"], res)
        self.assertEqual(self.ex.aliases["zh"]["defaults"], {"选项A": 1})
        self.assertEqual(self.ex.execute("zh")["r"], 1)

    def test_plain_alias_free_variables_are_bindable(self):
        # 无签名别名的可绑定集 = body 顶层自由变量集（13.4）
        self.ex.execute("#alias sum3\nint(x) + int(y) + int(z)")
        self.assertEqual(self.ex.aliases["sum3"]["params"], ["x", "y", "z"])
        self.assertEqual(self.ex.aliases["sum3"]["defaults"], {})

        # 通道显式数据（HTTP query）走 C1；本地 globals 保持旧注入语义
        http_like = self.ex.execute(
            "sum3", alias_params={"x": "1", "y": "2"})
        self.assertFalse(http_like["ok"])
        self.assertIn("缺少必填参数", http_like["error"])

        ok = self.ex.execute(
            "sum3", alias_params={"x": "1", "y": "2", "z": "3"})
        self.assertEqual(ok["r"], 6)

        legacy = self.ex.execute(
            "sum3", globals={"x": 1, "y": 2, "z": 3, "request": object()})
        self.assertTrue(legacy["ok"], legacy)
        self.assertEqual(legacy["r"], 6)

    def test_show_by_signature_form(self):
        self.ex.execute("#alias add(x, y=3)\nx + y")
        res = self.ex.execute("#alias add(x, y=3)")
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"]["name"], "add")
        self.assertEqual(res["r"]["mode"], "params")
        self.assertEqual(res["r"]["signature"], "x, y=3")
        self.assertEqual(res["r"]["params"], ["x", "y"])
        self.assertEqual(res["r"]["defaults"], {"y": 3})

    def test_list_renders_each_mode(self):
        self.ex.execute("#alias add(x, y=3)\nx + y")
        self.ex.execute("#alias git+\nr = _alias_arg")
        listing = self.ex.execute("#alias")["r"]
        self.assertEqual(listing["add(x, y=3)"], "x + y")
        self.assertEqual(listing["git+"], "r = _alias_arg")


class AliasRegistrationErrorTests(unittest.TestCase):
    """R1–R6：注册期 fail fast，注册表不产生记录。"""

    def test_registration_errors_fail_fast(self):
        cases = [
            ("#alias a+\nx", "前缀太短"),
            ("#alias add(*a)\nx", "*args"),
            ("#alias add(*, x)\nx", "keyword-only"),
            ("#alias add(x: int)\nx", "类型注解"),
            ("#alias add(y=x+1)\ny", "默认值必须是字面量"),
            ("#alias bad(x, z)\nx + y", "dead param"),
            ("#alias add(_alias_arg)\nx", "保留变量"),
            ("#alias git+(x)\nx", "前缀 + 签名"),
        ]
        for code, expected in cases:
            with self.subTest(code=code):
                ex = PythonExecutor()
                res = ex.execute(code)
                self.assertFalse(res["ok"], res)
                self.assertIn(expected, res["error"])
                self.assertEqual(ex.aliases, {})


class AliasPrefixModeTests(unittest.TestCase):
    """prefix 模式：最长前缀优先，剩余串进 _alias_arg。"""

    def setUp(self):
        self.ex = PythonExecutor()

    def test_remainder_and_empty_calls(self):
        self.ex.execute("#alias git+\nr = _alias_arg")
        self.assertEqual(self.ex.execute("git status -s")["r"], "status -s")
        self.assertEqual(self.ex.execute("git")["r"], "")       # C5：空剩余
        self.assertEqual(self.ex.execute("git+")["r"], "")       # 记录名 exact
        self.assertEqual(self.ex.execute("gity")["r"], "y")

    def test_longest_prefix_wins(self):
        self.ex.execute("#alias gi+\nr = 'short:' + _alias_arg")
        self.ex.execute("#alias git+\nr = 'long:' + _alias_arg")
        self.assertEqual(self.ex.execute("git status")["r"], "long:status")
        self.assertEqual(self.ex.execute("gi hello")["r"], "short:hello")

    def test_escape_and_prefix_switch(self):
        self.ex.execute("#alias git+\nr = _alias_arg")
        hits = self.ex.aliases["git+"]["hits"]

        escaped = self.ex.execute("\\git status")       # 字面执行，不展开
        self.assertFalse(escaped["ok"])
        self.assertIn("SyntaxError", escaped["error"])
        self.assertEqual(self.ex.aliases["git+"]["hits"], hits)

        disabled = self.ex.execute("git status", alias_prefix=False)
        self.assertFalse(disabled["ok"])
        self.assertIn("SyntaxError", disabled["error"])
        self.assertEqual(self.ex.aliases["git+"]["hits"], hits)

    def test_exact_name_and_prefix_coexist(self):
        self.ex.execute("#alias git\nr = 'exact'")
        self.ex.execute("#alias git+\nr = 'prefix:' + _alias_arg")
        self.assertEqual(self.ex.execute("git")["r"], "exact")
        self.assertEqual(self.ex.execute("git pull")["r"], "prefix:pull")

    def test_prefix_record_has_no_bindable_params(self):
        # alias.md 4.1：prefix 模式无可绑定名，输入一律走 _alias_arg；
        # body 的其它自由变量仍按普通名字从持久命名空间解析。
        self.ex.globals_dict["shout"] = lambda text: "[" + text + "]"
        self.ex.execute("#alias git+\nr = shout(_alias_arg)")
        record = self.ex.aliases["git+"]
        self.assertEqual(record["mode"], "prefix")
        self.assertEqual(record["params"], [])
        self.assertEqual(record["defaults"], {})
        self.assertEqual(self.ex.execute("git status")["r"], "[status]")


class AliasCrudTests(unittest.TestCase):
    def setUp(self):
        self.ex = PythonExecutor()

    # ---------------- 增 / 用 ---------------- #

    def test_define_and_use(self):
        res = self.ex.execute("#alias abc\n1+1")
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], 2)                      # 定义即执行
        self.assertEqual(res["alias"]["action"], "saved")
        self.assertEqual(res["alias"]["name"], "abc")

        used = self.ex.execute("abc")                      # 整段精确匹配 → 替换执行
        self.assertTrue(used["ok"])
        self.assertEqual(used["r"], 2)
        self.assertEqual(self.ex.aliases["abc"]["hits"], 1)

    def test_define_multiline_body(self):
        res = self.ex.execute("#alias sum3\nx=1\ny=2\nr=x+y")
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], 3)
        self.assertEqual(self.ex.execute("sum3")["r"], 3)

    def test_use_priority_over_same_name_variable(self):
        self.ex.execute("zz=40")
        self.ex.execute("#alias zz\nr=7")
        self.assertEqual(self.ex.execute("zz")["r"], 7)    # 精确匹配 → 别名优先
        self.assertEqual(self.ex.execute("zz+2")["r"], 42) # 更长表达式 → 命名空间变量

    def test_exact_match_only(self):
        self.ex.execute("#alias abc\nr=1+1")
        longer = self.ex.execute("abc+1")                  # 不是精确匹配 → 不展开
        self.assertFalse(longer["ok"])
        self.assertIn("NameError", longer["error"])

    def test_use_surrounding_whitespace(self):
        self.ex.execute("#alias w1\nr='ok'")
        self.assertEqual(self.ex.execute("  w1 \n")["r"], "ok")

    def test_import_bindings_are_not_free_variables(self):
        # import 绑定名字（无 Name 节点）不算自由变量，否则会被误判成
        # 可绑定参数 / 必填参数（HTTP query 侧直接表现为 C1/C2）。
        self.ex.execute("#alias lib\nimport math\nr = math.floor(2.9)")
        self.assertEqual(self.ex.aliases["lib"]["params"], [])
        self.assertEqual(self.ex.execute("lib")["r"], 2)

    def test_await_body(self):
        res = self.ex.execute(
            "#alias slp\nimport asyncio\nr=await asyncio.sleep(0, 'done')"
        )
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], "done")
        self.assertEqual(self.ex.execute("slp")["r"], "done")

    def test_request_globals_passthrough_and_restore(self):
        # 定义即执行的那一次运行里，请求级注入变量可见；执行后不泄漏
        res = self.ex.execute("#alias greet\nr='hi '+name", globals={"name": "bob"})
        self.assertEqual(res["r"], "hi bob")
        self.assertNotIn("name", self.ex.globals_dict)
        # 事后单独使用时 name 不在 → 报 NameError 而不是悄悄换结果
        again = self.ex.execute("greet")
        self.assertFalse(again["ok"])

    # ---------------- 查 ---------------- #

    def test_show_single(self):
        self.ex.execute("#alias abc\n1+1")
        res = self.ex.execute("#alias abc")
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"]["name"], "abc")
        self.assertEqual(res["r"]["code"], "1+1")
        self.assertEqual(res["r"]["hits"], 0)

    def test_show_missing(self):
        res = self.ex.execute("#alias nope")
        self.assertFalse(res["ok"])
        self.assertIn("not found", res["error"])

    def test_list_all(self):
        self.ex.execute("#alias b\n2")
        self.ex.execute("#alias a\n1")
        res = self.ex.execute("#alias")
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], {"a": "1", "b": "2"})   # 排序后的 name -> code
        self.assertEqual(res["alias"]["count"], 2)

    # ---------------- 改 ---------------- #

    def test_update_overwrite(self):
        self.ex.execute("#alias abc\n1+1")
        self.ex.execute("abc")
        res = self.ex.execute("#alias abc\nr=1+2")
        self.assertEqual(res["alias"]["action"], "updated")
        self.assertEqual(self.ex.execute("abc")["r"], 3)
        info = self.ex.aliases["abc"]
        self.assertEqual(info["hits"], 2)                  # 更新保留使用计数
        self.assertLessEqual(info["created"], info["updated"])

    # ---------------- 删 ---------------- #

    def test_unalias_single_and_multi(self):
        self.ex.execute("#alias a\n1")
        self.ex.execute("#alias b\n2")
        res = self.ex.execute("#unalias a b")
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"]["deleted"], ["a", "b"])
        self.assertEqual(res["r"]["missing"], [])
        self.assertNotIn("a", self.ex.aliases)
        self.assertFalse(self.ex.execute("a")["ok"])       # 删除后恢复 NameError

    def test_unalias_missing(self):
        res = self.ex.execute("#unalias nope")
        self.assertFalse(res["ok"])
        self.assertIn("not found", res["error"])

    def test_unalias_no_args(self):
        res = self.ex.execute("#unalias")
        self.assertFalse(res["ok"])
        self.assertIn("usage", res["error"])

    # ---------------- 关键字 list ---------------- #

    def test_keywords_are_mutable_lists(self):
        # 类属性与模块级 list 是同一对象：运行期改模块 list 立即生效
        self.assertIs(PythonExecutor.alias_keywords, ALIAS_KEYWORDS)
        self.assertIs(PythonExecutor.unalias_keywords, UNALIAS_KEYWORDS)

        ALIAS_KEYWORDS.append("a")
        UNALIAS_KEYWORDS.append("una")
        try:
            res = self.ex.execute("#a t1\n2+2")            # [alias, a] → #a 也生效
            self.assertTrue(res["ok"])
            self.assertEqual(res["r"], 4)
            self.assertEqual(self.ex.execute("t1")["r"], 4)

            self.assertTrue(self.ex.execute("#una t1")["ok"])  # unalias 列表同理
            self.assertNotIn("t1", self.ex.aliases)

            self.assertTrue(self.ex.execute("#alias t1\n1")["ok"])  # 原关键字仍可用
            self.ex.execute("#unalias t1")
        finally:
            ALIAS_KEYWORDS.remove("a")
            UNALIAS_KEYWORDS.remove("una")

    # ---------------- 校验 ---------------- #

    def test_name_rule_allows_any_non_blank_name(self):
        for good in ("1abc", "a-b", "ime.apk", "v1.2", "中文", "for"):
            self.assertTrue(rpc_executor.is_valid_alias_name(good), good)
        for bad in ("", " ", "a b", "a\tb", "a\nb", "\x00"):
            self.assertFalse(rpc_executor.is_valid_alias_name(bad), bad)

    def test_dotted_name_can_be_registered_and_used(self):
        res = self.ex.execute("#alias ime.apk\nr='apk-body'")
        self.assertTrue(res["ok"], res)
        self.assertEqual(res["r"], "apk-body")             # 定义即执行
        self.assertIn("ime.apk", self.ex.aliases)

        used = self.ex.execute("ime.apk")                  # 整段精确匹配
        self.assertEqual(used["r"], "apk-body")
        self.assertEqual(self.ex.aliases["ime.apk"]["hits"], 1)

        removed = self.ex.execute("#unalias ime.apk")
        self.assertTrue(removed["ok"])
        self.assertEqual(removed["r"]["deleted"], ["ime.apk"])

    def test_keyword_name_can_be_registered(self):
        res = self.ex.execute("#alias for\nr=1")
        self.assertTrue(res["ok"], res)
        self.assertEqual(self.ex.execute("for")["r"], 1)

    # ---------------- 带参数调用 ---------------- #

    def test_alias_call_with_path_argument(self):
        # URL /<别名>=<参数> 的语义：路径本身作为参数写进 _alias_arg
        res = self.ex.execute("#alias where\nr = _alias_arg")
        self.assertTrue(res["ok"], res)
        self.assertEqual(res["r"], "")                 # 定义即执行：无参数

        used = self.ex.execute("where=/home/suishou/multi_mqtt/")
        self.assertTrue(used["ok"], used)
        self.assertEqual(used["r"], "/home/suishou/multi_mqtt/")
        self.assertEqual(self.ex.aliases["where"]["hits"], 1)

    def test_alias_argument_default_and_reset(self):
        self.ex.execute(
            "#alias gp\ndir = _alias_arg or '/tmp/default'\nr = dir")
        self.assertEqual(self.ex.execute("gp=/home/u/proj")["r"], "/home/u/proj")
        # 精确调用（不带参数）时参数复位成空串，不会沿用上一次的值
        self.assertEqual(self.ex.execute("gp")["r"], "/tmp/default")

    def test_alias_argument_keeps_slashes_and_equals(self):
        self.ex.execute("#alias echo1\nr = _alias_arg")
        self.assertEqual(self.ex.execute("echo1=/a/b=c/d/")["r"], "/a/b=c/d/")
        self.assertEqual(self.ex.execute("echo1=")["r"], "")

    def test_unregistered_prefix_is_not_expanded(self):
        # 未注册的 name=value 仍按普通 Python 执行（这里是语法错误），
        # 前缀匹配只对已注册别名生效，不会劫持普通赋值
        res = self.ex.execute("nosuch=/home/u/proj")
        self.assertFalse(res["ok"])
        self.assertIn("SyntaxError", res["error"])

    def test_split_alias_call(self):
        self.assertEqual(rpc_executor.split_alias_call("a=/x/"), ("a", "/x/"))
        self.assertEqual(rpc_executor.split_alias_call("a="), ("a", ""))
        self.assertEqual(rpc_executor.split_alias_call("a=1=2"), ("a", "1=2"))
        for bad in ("=x", "a b=1", "a", "", None):
            self.assertIsNone(rpc_executor.split_alias_call(bad), bad)

    def test_invalid_body_not_saved(self):
        res = self.ex.execute("#alias bad\n1+")
        self.assertFalse(res["ok"])
        self.assertNotIn("bad", self.ex.aliases)

    def test_too_many_names_on_directive_line(self):
        res = self.ex.execute("#alias a b\n1")
        self.assertFalse(res["ok"])

    # ---------------- alias 开关 ---------------- #

    def test_disabled_on_instance(self):
        ex = PythonExecutor(alias=False)
        res = ex.execute("#alias abc\n1+1")      # 指令行退化为普通注释
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], 2)
        self.assertEqual(ex.aliases, {})          # 未注册
        self.assertFalse(ex.execute("abc")["ok"]) # 也不做精确匹配替换

    def test_per_call_disable(self):
        ex = PythonExecutor()                     # 默认启用
        res = ex.execute("#alias tmp\n5+5", alias=False)
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], 10)
        self.assertEqual(ex.aliases, {})
        ex.execute("#alias u\nr=8")
        self.assertFalse(ex.execute("u", alias=False)["ok"])  # 关闭时不替换
        self.assertEqual(ex.execute("u")["r"], 8)             # 默认仍启用

    def test_per_call_enable_on_disabled_instance(self):
        ex = PythonExecutor(alias=False)
        res = ex.execute("#alias t\nr=3", alias=True)
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], 3)
        self.assertIn("t", ex.aliases)
        self.assertEqual(ex.execute("t", alias=True)["r"], 3)
        self.assertFalse(ex.execute("t")["ok"])  # 实例级默认仍关闭

    def test_execute_default_follows_instance(self):
        self.assertTrue(PythonExecutor().alias)
        self.assertFalse(PythonExecutor(alias=False).alias)

    def test_directive_must_be_first_nonempty_line(self):
        # 指令不在首行 → 只是普通代码里的注释，不注册任何别名
        res = self.ex.execute("x=1\n#alias y\nr=x+1")
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], 2)
        self.assertNotIn("y", self.ex.aliases)

    def test_leading_blank_lines_still_directive(self):
        self.ex.execute("\n\n#alias late\nr=9")
        self.assertEqual(self.ex.execute("late")["r"], 9)

    def test_space_after_hash_tolerated(self):
        self.ex.execute("# alias sp\nr=5")
        self.assertEqual(self.ex.execute("sp")["r"], 5)


class AliasHttpChannelTests(unittest.TestCase):
    """端到端：真实起一个 server_http，走 URL-as-code 通道。"""

    @classmethod
    def setUpClass(cls):
        import server_http
        cls.server_http = server_http
        # port=0 让系统分配空闲端口；log_sink 吞掉请求日志保持输出干净；
        # persist_aliases=False 让本用例只动内存别名表，不往仓库 rpc.json
        # 里留下测试别名（持久化本身由 test_server_http 的专门用例覆盖）。
        cls.server = server_http.start_rpc_server(
            port=0, key='', globals={}, daemon=True,
            log_sink=lambda line: None, persist_aliases=False,
        )
        cls.port = cls.server.server_address[1]

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    @classmethod
    def call(cls, code):
        url = f"http://127.0.0.1:{cls.port}/" + urllib.parse.quote(code, safe='')
        return urllib.request.urlopen(url, timeout=10).read().decode('utf-8')

    def test_roundtrip(self):
        # execution['r'] 回退修复：尾表达式直接返回，与 MQTT 通道对齐
        self.assertEqual(self.call("1+1").strip(), "2")

        # %23 / %0A 编码端到端：定义即执行
        self.assertEqual(self.call("#alias sum2\n1+1").strip(), "2")
        # 使用
        self.assertEqual(self.call("sum2").strip(), "2")
        # 查询：pformat 后能回显 body
        self.assertIn("1+1", self.call("#alias sum2"))

        # 删除后再用 → 500 NameError
        self.assertIn("deleted", self.call("#unalias sum2"))
        with self.assertRaises(urllib.error.HTTPError):
            self.call("sum2")


if __name__ == "__main__":
    unittest.main(verbosity=2)
