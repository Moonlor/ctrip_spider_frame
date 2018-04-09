from multiprocessing.managers import BaseManager
from selenium.webdriver.chrome.options import Options
import requests
import json
import time
import random
import datetime
import re
import pymysql

import multiprocessing

from selenium import webdriver
from RandomUserAgent import RandomUserAgent

global null, false, true
null = ''
false = 0
true = 1


class SpiderWork(object):
    def __init__(self):
        #初始化分布式进程中的工作节点的连接工作
        # 实现第一步：使用BaseManager注册获取Queue的方法名称
        BaseManager.register('get_task_queue')
        BaseManager.register('get_result_queue')
        self.totoal_count = 0
        # 实现第二步：连接到服务器:
        server_addr = '127.0.0.1'
        print('Connect to server %s...' % server_addr)
        # 端口和验证口令注意保持与服务进程设置的完全一致:
        self.m = BaseManager(address=(server_addr, 8011), authkey=b'woshinibaba')
        # 从网络连接:
        self.m.connect()
        # 实现第三步：获取Queue的对象:
        self.task = self.m.get_task_queue()
        self.result = self.m.get_result_queue()
        self.fail_flag = 0
        self.finished_airline = 0
        self.finished_date = set()
        print('init finish')


    def crawl(self):

        print("爬虫进程开始运行")
        while (True):
            try:
                if not self.task.empty():
                    airline = self.task.get()

                    print('get: <<<<<<<<' + airline + '>>>>>>>>>>>')

                    target = airline.split('|')

                    today = datetime.date.today().strftime("%Y-%m-%d")

                    d_city = target[1]
                    a_city = target[2]


            except (EOFError) as e:
                print("连接工作节点失败")
                return
            except (Exception) as e:
                print(e)
                print('Crawl  fali ')




if __name__=="__main__":
    spider = SpiderWork()
    print("连接成功")
    spider.crawl()