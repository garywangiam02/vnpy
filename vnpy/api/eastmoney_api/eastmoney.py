import os
import sys
import tempfile
import asyncio
import re
import time
import xml.dom.minidom as dom
import datetime

from PIL import Image
from pyppeteer import launch
from pyppeteer.launcher import connect
import pyppeteer.errors
import numpy as np
import onnxruntime


class ONNXVerifyCodeRecognizer(object):
    CODE_MAPPING = {
        i: str(i)
        for i in range(10)
    }

    def __init__(
            self,
            onnx_file,
            input_shape,
            input_name="input", output_name="output",
    ) -> None:
        self._input_shape = input_shape
        self._input_name = input_name
        self._output_name = output_name
        self._onnx_session = onnxruntime.InferenceSession(onnx_file)

    def recognize_from_array(self, arr):
        input_feed = {
            self._input_name: arr.reshape(1, *self._input_shape),
        }
        output = self._onnx_session.run([self._output_name], input_feed=input_feed)
        output = output[0][0]
        output = output.argmax(axis=1)
        output = output.tolist()
        output_str = "".join([
            self.CODE_MAPPING[x]
            for x in output
        ])
        return output_str

    def recognize_from_image(self, img):
        arr = np.array(img).astype("float32")
        return self.recognize_from_array(arr)


class EastMoneyVerifyCodeRecognizer(ONNXVerifyCodeRecognizer):
    """
    东方财富动态验证码识别
    """
    def __init__(self, onnx_file=None) -> None:
        if onnx_file is None:
            current_path, _ = os.path.split(__file__)
            onnx_file = os.path.join(current_path, "eastmoney.onnx")
        input_shape = (36, 72)
        input_name = "input"
        output_name = "output"
        super().__init__(onnx_file, input_shape, input_name=input_name, output_name=output_name)

    def recognize_from_image(self, img):
        img = img.convert("L")
        arr = np.array(img)
        arr = (arr < 128).astype(np.uint8) * 255
        arr = self._crop_image_array(arr, gap=3)

        img = Image.fromarray(arr)
        img = img.resize((72, 36))
        return super().recognize_from_image(img)

    def _crop_image_array(self, arr, gap=0):
        for top in range(arr.shape[0]):
            if arr[top, :].sum() > 0:
                break
        else:
            top = 0
        top = max(top - gap, 0)

        for bottom in reversed(range(arr.shape[0])):
            if arr[bottom, :].sum() > 0:
                break
        else:
            bottom = arr.shape[0] - 1
        bottom = bottom + gap

        for left in range(arr.shape[1]):
            if arr[:, left].sum() > 0:
                break
        else:
            left = 0
        left = max(left - gap, 0)

        for right in reversed(range(arr.shape[1])):
            if arr[:, right].sum() > 0:
                break
        else:
            right = arr.shape[1] - 1
        right = right + gap

        arr = arr[top:bottom, left:right]
        return arr


URL_ROOT = "https://jy.xzsec.com"
URLS = {
    "普通": {
        "资金持仓": "/Search/Position",
        "买入": "/Trade/Buy",
        "卖出": "/Trade/Sale",
        "撤单": "/Trade/Revoke",
        "当日委托": "/Search/Orders",
        "当日成交": "/Search/Deal",
    },
    "信用": {
        "资金持仓": "/MarginSearch/MyAssets",
        "买入": "/MarginTrade/Buy",
        "卖出": "/MarginTrade/Sale",
        "撤单": "/MarginTrade/MRevoke",
        "当日委托": "/MarginSearch/Orders",
        "当日成交": "/MarginSearch/Deals",
    }
}


class EastMoneyBackend(object):
    """
    东方财富 web交易系统
    """
    def __init__(self, browser_url=None, debug=False) -> None:
        self._logged = False
        self._browser_url = browser_url
        self._browser = None
        self._pages = {}
        self._last_accessed_page = 0
        self._debug = debug
        self.validatekey = ""
        self.cookies = ""
        self._current_account = "普通"
        self.version = '1.0'
        self.validate_date = "2022-12-31"
        print(f'easymoney api: {self.version} ==> {self.validate_date}')

    async def _get_browser(self):
        """
        获取浏览器
        :return:
        """
        if self._browser is None:
            if self._browser_url is not None:
                self._browser = await connect(browserURL=self._browser_url)
            else:
                options = {
                    "headless": (not self._debug),
                    "ignoreDefaultArgs": [
                        "--enable-automation",
                    ],
                    "args": [
                        '--disable-extensions',
                        '--disable-bundled-ppapi-flash',
                        '--disable-web-security',
                        '--disable-gpu',
                        '--disable-xss-auditor',
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--allow-running-insecure-content',
                        '--disable-webgl',
                        '--disable-popup-blocking',
                        '--disable-software-rasterizer'
                    ],
                }
                if os.environ.get("RUN_IN_DOCKER", "0") == "true":
                    options["executablePath"] = "/usr/bin/chromium"
                    os.environ["PUPPETEER_EXECUTABLE_PATH"] = "usr/bin/chromium"
                    os.environ["PUPPETEER_SKIP_CHROMIUM_DOWNLOAD"] = "true"
                self._browser = await launch(
                    options,
                    handleSIGINT=False,
                    handleSIGTERM=False,
                    handleSIGHUP=False,
                )
        return self._browser

    async def _get_page(self, n=None):
        """
        获取浏览器得访问页面
        :param n:
        :return:
        """
        if n is None:
            n = self._last_accessed_page

        if n not in self._pages:
            browser = await self._get_browser()
            pages = await browser.pages()
            if (len(pages) - 1) < n:
                page = await browser.newPage()
            else:
                page = pages[n]
            await page.setViewport({'width': 1280, 'height': 768})
            await page.setUserAgent(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
            await page.evaluateOnNewDocument(
                '() =>{ Object.defineProperties(navigator,{ webdriver:{ get: () => false } }) }')
            self._pages[n] = page

        self._last_accessed_page = n
        return self._pages[n]

    async def _screenshot(self, box=None, save_to=None):
        """截图"""
        is_tmp_output = False
        if save_to is None:
            is_tmp_output = True
            _, save_to = tempfile.mkstemp(suffix=".png")

        options = {
            "path": os.path.abspath(save_to),
        }
        if box is not None:
            options["clip"] = box

        page = await self._get_page()
        await page.screenshot(options)

        if is_tmp_output:
            im = Image.open(save_to)
            try:
                os.remove(save_to)
            except:
                pass
            return im
        else:
            return save_to

    def _get_url(self, name, account=None):
        """
        获取URL
        :param name: 用户姓名
        :param account: 账号(普通账号、信用账号等）
        :return:
        """
        if account is None:
            account = self._current_account
        url = URLS[account][name]
        url = URL_ROOT + url
        return url

    async def _get_box_of_element_id(self, element_id):
        """
        获取网页内容得一个element元素(坐标、长宽）
        :param element_id:
        :return:
        """
        js_code = """()=>{
            let {
                x,
                y,
                width,
                height
            } = document.getElementById('ELEMENT_ID').getBoundingClientRect();
            return {
                x,
                y,
                width,
                height
            };
        }
        """
        js_code = js_code.replace("ELEMENT_ID", element_id)
        page = await self._get_page()
        box = await page.evaluate(js_code)
        return box

    async def _get_one_element(self, xpath, root=None, timeout=None, visible=True):
        """
        获取元素
        :param xpath:
        :param root:
        :param timeout:
        :param visible:
        :return:
        """
        if root is None:
            page = await self._get_page()
        else:
            page = root
        if timeout is not None:
            opt = {}
            if visible:
                opt["visible"] = True
            try:
                await page.waitForXPath(xpath, timeout=timeout, options=opt)
            except pyppeteer.errors.TimeoutError:
                return None

        elements = await page.xpath(xpath)
        if len(elements) == 1:
            return elements[0]

    async def _get_attr(self, element, attr_name):
        page = await self._get_page()
        return await page.evaluate("""
            (element) => { return element.getAttribute('%%'); }
        """.replace("%%", attr_name), element)

    async def update_validatekey(self, page):
        """
        获取登录后session得validatekey
        :param page:
        :return:
        """
        k = await self._get_one_element(xpath="//input[@id='em_validatekey']", root=page)
        self.validatekey = await self._get_attr(k, 'value')

    async def _update_cookies(self, page):
        """
        更新cookie
        :param page:
        :return:
        """
        cookie_list = await page.cookies()
        self.cookies = ""
        for cookie in cookie_list:
            coo = "{}={};".format(cookie.get("name"), cookie.get("value"))
            self.cookies += coo

        return self.cookies

    async def get_cookies(self):
        """获取cookie"""
        return self.cookies

    async def _get_text(self, element):
        """
        获取文本内容
        :param element:
        :return:
        """
        p = await element.getProperty('textContent')
        text = await p.jsonValue()
        return text

    async def _set_value(self, element, value):
        page = await self._get_page()
        return await page.evaluate("""
            (element) => { element.value = '%'; }
        """.replace("%", value), element)

    async def _get_inner_html(self, element):
        page = await self._get_page()
        return await page.evaluate("""
            (element) => { return element.innerHTML; }
        """, element)

    async def _get_popup_msg(self, timeout=None):
        """
        获取弹出框得内容
        :param timeout:
        :return:
        """
        page = await self._get_page()
        div_info = await self._get_one_element("//div[@class='cxc_dialog_bd']/p[1]", timeout=timeout)
        if div_info is None:
            return None
        else:
            msg = await self._get_text(div_info)
            return msg

    async def _ensure_no_popup(self):
        msg = await self._get_popup_msg()
        if msg is not None:
            page = await self._get_page()
            await page.goto("https://jy.xzsec.com/Login/")

    async def _get_table(self, table_element):
        table_html = await self._get_inner_html(table_element)
        doc = dom.parseString(f"<table>{table_html}</table>")

        thead = doc.getElementsByTagName("thead")[0]
        headers = [
            h.firstChild.nodeValue
            for h in thead.getElementsByTagName("th")
        ]

        tbody = doc.getElementsByTagName("tbody")[0]
        rows = tbody.getElementsByTagName("tr")
        records = []
        for row in rows[:-1]:
            cells = []
            for cell in row.getElementsByTagName("td"):
                while cell.nodeType != dom.Element.TEXT_NODE and cell.firstChild is not None:
                    cell = cell.firstChild

                value = cell.nodeValue
                cells.append(value)
            record = dict(zip(headers, cells))
            records.append(record)

        return records

    async def login(self, username, password, max_retries=2):
        """
        网页登录账号
        :param username: 用户资金账号
        :param password: 密码
        :param max_retries: 尝试次数
        :return:
        """
        print('easymoney: start login')
        if datetime.datetime.now().strftime("%Y-%m-%d") > self.validate_date:
            raise Exception("失效")

        page = await self._get_page()

        for _ in range(max_retries):
            # 页面访问登录界面
            await page.goto(URL_ROOT + "/Login")
            try:
                # 等到页面完成导航
                await page.waitForNavigation(timeout=1000)
            except:
                pass
            print('easymoney:got login page')
            try:
                # 等到出现页面得某个表达式（输入框=资金账号）
                await page.waitForXPath("//input[@id='txtZjzh']", timeout=1000)
            except pyppeteer.errors.TimeoutError:
                # 检查是否已经登录
                exit_button = await page.xpath("//a[text()='退出']")
                if len(exit_button) != 1:
                    raise Exception("找不到登出按钮")
                exit_button = exit_button[0]
                await exit_button.click()
                await page.waitForNavigation(timeout=5000)

            # 获取资金账号框
            txt_username = await page.xpath("//input[@id='txtZjzh']")
            if len(txt_username) != 1:
                raise Exception("找不到账户框")
            txt_username = txt_username[0]
            # 填充资金账号
            await txt_username.type(username)

            # 获取密码输入框
            await page.waitForXPath("//input[@id='txtPwd']")
            txt_password = await page.xpath("//input[@id='txtPwd']")
            if len(txt_password) != 1:
                raise Exception("找不到密码框")
            txt_password = txt_password[0]
            # 填充密码
            await txt_password.type(password)

            # 获取动态验证码
            await page.waitForXPath("//img[@id='imgValidCode']")

            # 获取动态验证码得区域
            box = await self._get_box_of_element_id("imgValidCode")
            # 截图
            img = await self._screenshot(box)
            # 验证码识别
            recognizer = EastMoneyVerifyCodeRecognizer()
            verify_code = recognizer.recognize_from_image(img)

            # 获取输入动态验证码框
            txt_verify_code = await page.xpath("//input[@id='txtValidCode']")
            if len(txt_verify_code) != 1:
                raise Exception("找不到验证码框")
            txt_verify_code = txt_verify_code[0]
            # 填充验证码
            await txt_verify_code.type(verify_code)

            # 获取在线时间选项
            option_online_time = await page.xpath("//input[@name='rdsc' and @value='1800']")
            if len(option_online_time) != 1:
                raise Exception("找不到在线时间选项")
            option_online_time = option_online_time[-1]
            # 选择最后得时间项

            await option_online_time.click()

            # 获取提交按钮
            btn_confirm = await page.xpath("//button[@id='btnConfirm']")
            if len(btn_confirm) != 1:
                raise Exception("找不到确认按钮")
            btn_confirm = btn_confirm[0]
            # 点击提交按钮
            print('easymoney:submitting username & password')
            await btn_confirm.click()

            try:
                # 等到导航出现
                await page.waitForNavigation(timeout=1000)
            except pyppeteer.errors.TimeoutError:
                continue
            else:
                exit_button = await page.xpath("//a[text()='退出']")
                if len(exit_button) == 1:
                    # 登录成功
                    # 获取返回页面中得validatekey，更新
                    await self.update_validatekey(page)
                    # 获取cookie，更新
                    await self._update_cookies(page)
                    print('easymoney:login success')
                    return True
                else:
                    print('easymoney:login failure')

                    return False

    # async def get_current_account(self):
    #     page = await self._get_page()
    #     cur_tab = await page.xpath(f"//div[@class='navtab']/div[@class='tab current']/a/text()")
    #     if len(cur_tab) == 1:
    #         tab_name = await self._get_text(cur_tab[0])
    #         return tab_name

    async def switch_account(self, account):
        """
        切换账号
        :param account:
        :return:
        """
        page = await self._get_page()
        tab = await page.xpath(f"//div[@class='navtab']/div/a[text()='{account}']")
        if len(tab) != 1:
            raise Exception(f"找不到: {account}")
        await tab[0].click()
        await page.waitForNavigation()
        cur_tab = await page.xpath(f"//div[@class='navtab']/div[@class='tab current']/a[text()='{account}']")
        if len(cur_tab) == 1:
            self._current_account = account
            return True
        else:
            raise Exception(f"切换账户失败: {account}")

    async def trade(self, sell, code, amount, price, account=None):
        page = await self._get_page()
        if sell:
            await page.goto(self._get_url("卖出", account=account))
        else:
            await page.goto(self._get_url("买入", account=account))

        txt_stock_code = await self._get_one_element("//input[@id='stockCode']", timeout=2000)
        await txt_stock_code.click()
        await txt_stock_code.type(code)

        div_suggest_result = await self._get_one_element("//div[@class='suggest-result']", timeout=1000)
        await txt_stock_code.press("Enter")

        txt_price = await self._get_one_element("//input[@id='iptPrice']", timeout=1000)
        await txt_price.click()
        await self._set_value(txt_price, f"{float(price):.3f}")

        txt_amount = await self._get_one_element("//input[@id='iptCount']", timeout=1000)
        await txt_amount.click()

        # 等待最大可买数量出现后再填写数量
        for _ in range(30):
            label_max_amount = await self._get_one_element("//label[@id='lbMaxCount']")
            str_max_amount = await self._get_text(label_max_amount)
            try:
                max_amount = int(str_max_amount)
            except ValueError:
                raise Exception("无法识别最大数量: {str_max_amount}")

            if max_amount > 0:
                await self._set_value(txt_amount, f"{int(amount):d}")
                break
            else:
                msg = await self._get_popup_msg()
                if msg is not None:
                    raise Exception()
                time.sleep(0.1)
        else:
            raise Exception("获取最大可用数量失败或最大可用数量为0")

        btn_submit = await self._get_one_element("//button[@id='btnConfirm']", timeout=1000)
        if btn_submit is None:
            raise Exception("找不到提交按钮")

        for _ in range(10):
            await btn_submit.click()
            btn_confirm = await self._get_one_element("//div[@class='btn-wrap']/a[text()='下单确认']", timeout=1000)
            if btn_confirm is None:
                continue
            else:
                await btn_confirm.click()
                break

        for _ in range(10):
            msg = await self._get_popup_msg(timeout=1000)
            if msg is not None:
                assert "委托编号" in msg
                order_id = msg.split(":")[-1].strip()
                break
        else:
            raise Exception("获取委托编号失败")

        return order_id

    async def sell(self, code, amount, price, account=None):
        try:
            order_id = await self.trade(
                sell=True,
                code=code,
                amount=amount,
                price=price,
                account=account,
            )
        except Exception as e:
            # 尝试捕获弹出窗口
            msg = await self._get_popup_msg()
            if msg is not None:
                # 用弹出消息代替错误信息
                raise Exception(msg)
            else:
                raise e
        else:
            return order_id

    async def buy(self, code, amount, price, account=None):
        try:
            order_id = await self.trade(
                sell=False,
                code=code,
                amount=amount,
                price=price,
                account=account,
            )
        except Exception as e:
            # 尝试捕获弹出窗口
            msg = await self._get_popup_msg()
            if msg is not None:
                # 用弹出消息代替错误信息
                raise Exception(msg)
            else:
                raise e
        else:
            return order_id

    async def get_positions(self, account=None):
        """获取资金和持仓
        因为资金和持仓在同一个页面，所以一起获取
        """
        page = await self._get_page()
        await page.goto(self._get_url("资金持仓", account=account))

        ready = False
        for _ in range(10):
            table = await self._get_one_element("//table[@class='zichan']", timeout=1000)
            cell_titles = await table.xpath("//td/span[1]")

            capital_data = {}
            for title_element in cell_titles:
                title = await self._get_text(title_element)
                value_element = await self._get_one_element("../span[2]", root=title_element)
                value = await self._get_text(value_element)
                try:
                    capital_data[title] = float(value)
                except ValueError:
                    if not ready:
                        # 数据尚未准备好
                        break
                    else:
                        # 数据已经准备好，但是还是出错
                        raise Exception(f"无法解析字段: {title} => {value}")
                else:
                    ready = True

            if not ready:
                time.sleep(0.1)
                continue
            else:
                break

        table = await self._get_one_element("//div[@class='listtable']/table", timeout=2000)
        if table is None:
            raise Exception("找不到持仓数据表格")
        results = await self._get_table(table)

        positions = []
        for row in results:
            pos = {}
            for k, v in row.items():
                if "盈亏" in k:
                    v = float(v)
                elif "数量" in k:
                    v = int(v)
                elif "价" in k:
                    v = float(v)
                elif "金额" in k:
                    v = float(v)
                elif "市值" in k:
                    v = float(v)
                elif k == "操作":
                    v = None

                if v is not None:
                    pos[k] = v

            positions.append(pos)

        return capital_data, positions

    async def cancel_order(self, order_id, account=None):
        page = await self._get_page()
        await page.goto(self._get_url("撤单", account=account))

        today = datetime.datetime.now().strftime("%Y%m%d")
        js = f"revokeJS.revokeOrders('{today}_{order_id}')"
        await page.evaluate(js)

        for _ in range(10):
            btn_confirm = await self._get_one_element("//span[@id='btnCxcConfirm']", timeout=500)
            if btn_confirm is None:
                continue
            await btn_confirm.click()
            break
        else:
            raise Exception("找不到确认撤单按钮")

        msg = await self._get_popup_msg(timeout=1000)
        if "成功" in msg:
            return True
        else:
            raise Exception(msg)

    async def get_orders(self, account=None):
        page = await self._get_page()
        await page.goto(self._get_url("当日委托", account=account))

        for _ in range(10):
            loading = await self._get_one_element("//td[contains(text(), '加载中')]", timeout=500)
            if loading is None:
                break
            time.sleep(0.1)
        else:
            raise Exception("加载委托数据失败")

        table = await self._get_one_element("//div[@class='listtable']/table", timeout=2000)
        if table is None:
            raise Exception("找不到委托数据表格")
        results = await self._get_table(table)

        orders = []
        for row in results:
            order = {}
            for k, v in row.items():
                if k == "委托时间":
                    v = datetime.datetime.strptime(v, "%H:%M:%S").time()
                elif "数量" in k:
                    v = int(v)
                elif "价格" in k:
                    v = float(v)
                elif "金额" in k:
                    v = float(v)
                elif k == "操作":
                    v = None

                if v is not None:
                    order[k] = v

            orders.append(order)

        return orders

    async def get_trades(self, account=None):
        page = await self._get_page()
        await page.goto(self._get_url("当日成交", account=account))

        for _ in range(10):
            loading = await self._get_one_element("//td[contains(text(), '加载中')]", timeout=500)
            if loading is None:
                break
            time.sleep(0.1)
        else:
            raise Exception("加载成交数据失败")

        table = await self._get_one_element("//div[@class='listtable']/table", timeout=2000)
        if table is None:
            raise Exception("找不到成交数据表格")
        results = await self._get_table(table)

        trades = []
        for row in results:
            trade = {}
            for k, v in row.items():
                if k == "成交时间":
                    v = datetime.datetime.strptime(v, "%H:%M:%S").time()
                elif "数量" in k:
                    v = int(v)
                elif "价格" in k:
                    v = float(v)
                elif "金额" in k:
                    v = float(v)
                elif k == "操作":
                    v = None

                if v is not None:
                    trade[k] = v

            trades.append(trade)

        return trades

    async def close(self):
        return await self._browser.close()

    # def __del__(self):
    #     if self._browser is not None:
    #         if self._browser_url is None:
    #             print('force close browser')
    #             try:
    #                 asyncio.run(self._browser.close())
    #             except:
    #                 pass
