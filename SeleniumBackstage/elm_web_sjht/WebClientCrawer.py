"""
    爬取饿了么商家后台数据
"""

import time
import traceback
import configparser
import re
import pickle
import json
import os
import traceback
import datetime


from selenium import webdriver
from selenium.webdriver.common.keys import Keys

# 前往登录 https://melody.shop.ele.me/login
# 测试店铺账号 sandbox_66299494
# 测试店铺密码 fOkcFJUVFSa8
class WebClientCrawer(object):

    def __init__(self):
        self.date = datetime.datetime.now().strftime('%Y-%m-%d')
        self.browser = webdriver.Chrome()
        # driverOptions = webdriver.ChromeOptions()
        # driverOptions.add_argument(r"user-data-dir=C:\Users\Administrator\AppData\Local\Google\Chrome\User Data")
        # self.browser = webdriver.Chrome("chromedriver", 0, driverOptions)
        # self.browser.get("https://melody.shop.ele.me/login")

    def getAccount(self,account_path):
        """
            获取配置文件中的账户密码
        :return:
        """
        cf = configparser.ConfigParser()
        cf.read(account_path,encoding='utf-8')

        account_list = cf.options('account_list')
        for account in account_list:
            password = cf.get("account_list",account)
            yield [account,password]


    def getConfig(self,xpath_path):
        """
            获取配置文件中的xpath路径
        :param config:
        :return: 返回字典
        """
        cfg = configparser.ConfigParser()
        cfg.read(xpath_path,encoding='utf-8')

        params = dict()
        for s in cfg.sections():
            keys = cfg.options(s)
            arr = []
            for k in keys:
                xpath = cfg.get(s, k)
                arr.append(xpath)
            params[s] = arr
        return params

    def loginPage(self,login_url):
        """
            打开登录页面
        :param login_url: 登录url
        :return:
        """
        self.browser.get(login_url)
        time.sleep(2)

    def getElements(self,xpaths):
        """
            通过xpath路径获取页面元素
        :param xpaths:
        :return: 元素列表或单个元素
        """
        if isinstance(xpaths,list):
            arr = []
            for xpath in xpaths:
                arr.append(self.browser.find_element_by_xpath(xpath))
            return arr
        elif isinstance(xpaths,str):
            return  self.browser.find_element_by_xpath(xpaths)

    def loginReturnId(self,account,password,xpath_list):
        """
            登录网站并返回店铺id
        :param account: 账号
        :param password: 密码
        :param xpath_list: xpath路径
        :return: 返回登录商铺的rest_id
        """
        print('登录')
        print("xpath_list:",xpath_list)
        elems = self.getElements(xpath_list)
        print("elems:",elems)

        elems[0].send_keys(account)
        print(account)
        elems[1].send_keys(password)
        elems[2].send_keys(Keys.RETURN) # 点击登录
        time.sleep(3)

        url = self.browser.current_url
        print("当前url：", url)
        m = re.match(r"^.*/(\d+)/.*$", url)
        rest_id = m.group(1)
        print("店铺id：", rest_id)
        return rest_id

    def gotoPage(self,xpath_list):
        """
            点击按钮显示对应的页面
        :param xpath_list: xpath路径
        :return:
        """
        print('打开页面')
        time.sleep(2)
        elems = self.getElements(xpath_list)
        for elem in elems:
            elem.click()
            time.sleep(1)

    def goto_onePage(self, xpath_list):
        """
            点击按钮显示对应的页面
        :param xpath_list: xpath路径
        :return:
        """

        time.sleep(2)
        elems = self.browser.find_element_by_xpath(xpath_list)
        elems.click()
        time.sleep(1)

    def gotoIFrame(self, frame_xpath_list):
        """
            跳转到指定的iframe
        :param frame_xpath_list: xpath路径
        :return:
        """
        print('切换iframe')
        time.sleep(2)
        frames = self.getElements(frame_xpath_list)
        for f in frames:
            self.browser.switch_to.frame(f)

    def goto_oneIFrame(self, frame_xpath_list):
        """
            跳转到指定的iframe
        :param frame_xpath_list: xpath路径
        :return:
        """
        time.sleep(2)
        frames = self.getElements(frame_xpath_list)
        self.browser.switch_to.frame(frames)

    def switch_to_default_content(self):
        """
        返回到主文档
        :return:
        """
        self.browser.switch_to.default_content()

    def getData(self,xpath_list):
        """
            爬取数据
        :param xpath_list: xpath路径
        :return:
        """
        print('爬取页面数据')
        time.sleep(2)
        params = []
        elems = self.getElements(xpath_list)
        for elem in elems:
            params.append(str(elem.text).replace(',',''))
        return params




if __name__ == '__main__':
    client = WebClientCrawer()
    client.crawler()




