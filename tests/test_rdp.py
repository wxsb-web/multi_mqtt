"""client/rdp.py 的页面与按键单元测试。

重点覆盖:手机屏幕键盘太小,一个键放不下 ``2``/@`` 两个字符,所以**按住 Shift
时键面切换成实际要发送的字符(2→@、`→~、字母→大写)**,松开或发完组合后还原。
这只是页面显示层的改动,``key('shift+2')`` 仍按原来的 VK 组合发送——这里同时
用替身验证 press() 的按键序列没有变化(不会真的注入按键)。
"""
import sys
import unittest


def render_page(bw=False):
    """用假 response 渲染页面,返回 HTML 文本。"""
    from client import rdp

    class FakeResponse:
        def __init__(self):
            self.headers = {}
            self.data = ''

        def set_header(self, name, value):
            self.headers[name] = value

        def set_data(self, data):
            self.data = data

    p = FakeResponse()
    rdp.html(p, bw=bw)
    return p.data


@unittest.skipUnless(sys.platform == 'win32', 'client.rdp 依赖 user32,仅 Windows 可导入')
class ShiftLabelPageTests(unittest.TestCase):
    def test_shift_label_map_covers_printable_keys(self):
        page = render_page()
        for mapping in ("'1': '!'", "'2': '@'", "'9': '('", "'0': ')'",
                        "'backtick': '~'", "'minus': '_'", "'equal': '+'",
                        "'lbracket': '{'", "'rbracket': '}'", "'backslash': '|'",
                        "'semicolon': ':'", "'quote': '\"'",
                        "'comma': '<'", "'period': '>'", "'slash': '?'"):
            with self.subTest(mapping=mapping):
                self.assertIn(mapping, page)

    def test_keys_keep_base_label_and_code(self):
        page = render_page()
        self.assertIn('btn.dataset.code = item[1];', page)
        self.assertIn('btn.dataset.base = item[0];', page)

    def test_shift_toggle_repaints_and_restores_labels(self):
        page = render_page()
        self.assertIn('function paintKeyboard()', page)
        # 修饰键切换分支一次 + 组合发出后一次
        self.assertEqual(page.count('paintKeyboard();'), 2)

    def test_shift_only_changes_labels_not_sent_combo(self):
        page = render_page()
        self.assertIn('let label = el.dataset.base;', page)
        self.assertIn('label = SHIFT_LABELS[code];', page)
        self.assertIn('label = code.toUpperCase();', page)   # 字母键面切大写
        # 发送的仍是组合键表达式(shift+2),没有改成直接键入字符
        self.assertIn('rpc(MOD + ".key(\'" + combo + "\')")', page)

    def test_placeholders_replaced_in_both_modes(self):
        for bw in (False, True):
            with self.subTest(bw=bw):
                page = render_page(bw=bw)
                self.assertNotIn('__BW__', page)
                self.assertNotIn('__MOD__', page)


@unittest.skipUnless(sys.platform == 'win32', 'client.rdp 依赖 user32,仅 Windows 可导入')
class KeyPressBehaviorTests(unittest.TestCase):
    def test_shift_digit_still_sends_vk_combination(self):
        from client import rdp
        events = []
        orig = rdp._key_event
        rdp._key_event = lambda vk, up=False: events.append((vk, up))
        try:
            message = rdp.press('shift+2')
        finally:
            rdp._key_event = orig
        # shift 按下 → 2 按下/抬起 → shift 抬起,与页面显示改动前完全一致
        self.assertEqual(events, [(0x10, False), (0x32, False),
                                  (0x32, True), (0x10, True)])
        self.assertEqual(message, 'key shift+2')

    def test_vk_lookup_for_digits_and_symbols(self):
        from client import rdp
        self.assertEqual(rdp._vk_of('2'), 0x32)
        self.assertEqual(rdp._vk_of('shift'), 0x10)
        self.assertEqual(rdp._vk_of('backtick'), 0xC0)
        self.assertEqual(rdp._vk_of('quote'), 0xDE)
        self.assertEqual(rdp._vk_of('enter'), 0x0D)


if __name__ == '__main__':
    unittest.main()
