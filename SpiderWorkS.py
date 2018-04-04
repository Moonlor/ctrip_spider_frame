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

    def nextday(self, date):
        date = datetime.datetime(int(date[0:4]), int(date[5:7]), int(date[8:10]))
        delta = datetime.timedelta(days=+1)
        nextday = date + delta
        nextday = nextday.strftime('%Y-%m-%d')
        return nextday

    def onload(self, driver, wait_time):
        time.sleep(wait_time)
        driver.execute_script('window.scrollTo(0, document.body.scrollHeight)')

    def execute(self, driver, drop_down_number, wait_time):
        for i in range(drop_down_number):
            self.onload(driver, wait_time)

    def std_date_to_ctrip_date(self, date):
        return str(date[5:7]) + '/' + str(date[8:10])

    def filter_ctrip_date(self, date):
        return date[0:5]

    def deal_flightInfo(self, string):
        flightID = re.findall('[0-9a-zA-Z_]+', string)[0]
        airline = re.findall('[\u4e00-\u9fa5]+', string)[0]
        return airline, flightID


    def store_data(self, driver, con, cur, date, dept_city, arv_city):
        self.execute(driver, 1, 1)

        i = 0
        while (True):
            i += 1

            try:
                # ---------------------------------------------------------------------------------------
                #           update_time  date
                update_time = time.strftime("%Y-%m-%d", time.localtime())
                # ---------------------------------------------------------------------------------------
                #           airline      varchar(255)
                #           flight_id    varchar(255)
                flightInfo = driver.find_element_by_xpath(
                    '//li[@id=' + '\"' + str(i) + '\"' + ']/div[1]/div[2]/span[1]')

                flightInfo = str(flightInfo.text)
                airline, flight_id = self.deal_flightInfo(flightInfo)
                # ---------------------------------------------------------------------------------------
                #           dept_date    date
                #           dept_time    varchar(255)
                dept_time = driver.find_element_by_xpath(
                    '//li[@id=' + '\"' + str(i) + '\"' + ']/div[1]/div[1]/div[1]/div[1]/div[1]')
                dept_time = str(dept_time.text)
                dept_date = date
                # ---------------------------------------------------------------------------------------
                #           dept_city    varchar(255)
                #           dept_airport varchar(255)
                dept_airport = driver.find_element_by_xpath(
                    '//li[@id=' + '\"' + str(i) + '\"' + ']/div[1]/div[1]/div[1]/div[1]/div[2]')
                dept_airport = (dept_airport.text)
                # ---------------------------------------------------------------------------------------
                #           arv_date     date
                #           arv_time     varchar(255)
                #           flight_day   float
                arv_time = driver.find_element_by_xpath(
                    '//li[@id=' + '\"' + str(i) + '\"' + ']/div[1]/div[1]/div[1]/div[3]/div[1]')
                try:
                    arv_time, flight_day = str(arv_time.text).split('\n')
                    flight_day = flight_day[1]
                except:
                    arv_time = str(arv_time.text)
                    flight_day = 0
                arv_date = date
                if int(flight_day) >= 1:
                    arv_date = self.nextday(arv_date)
                # ---------------------------------------------------------------------------------------
                #           arv_city     varchar(255)
                #           arv_airport  varchar(255)
                arv_airport = driver.find_element_by_xpath(
                    '//li[@id=' + '\"' + str(i) + '\"' + ']/div[1]/div[1]/div[1]/div[3]/div[2]')
                arv_airport = str(arv_airport.text)
                # ---------------------------------------------------------------------------------------
                #           ontime_Rate  float
                ontime_Rate = 0
                # ---------------------------------------------------------------------------------------
                #           price        float
                price = driver.find_element_by_xpath(
                    '//li[@id=' + '\"' + str(i) + '\"' + ']/div[1]/div[1]/div[2]/div[1]/strong')
                price = str(price.text)
            except(Exception) as e:
                break

            cur.execute(
                '  INSERT INTO Flight_%s' % datetime.date.today().strftime("%Y_%m_%d") + '(update_time, airline,flight_id, dept_date,dept_time,dept_city,dept_airport,arv_date,arv_time,arv_city,arv_airport,flight_day,ontime_Rate,price) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (update_time, airline, flight_id, dept_date, dept_time, dept_city, dept_airport, arv_date, arv_time,
                 arv_city,
                 arv_airport, flight_day, ontime_Rate, price))

        con.commit()




    def camouflage_broewser(self, today, d_city, a_city, con, cur):

        print('爬虫节点正在解析: 旅行日期 %s | 出发城市 %s | 到达城市 %s' % (today, d_city, a_city))

        search_date = today
        url = 'https://m.ctrip.com/html5/flight/swift/domestic/' + d_city + '/' + a_city + '/' + search_date
        # chrome_options = Options()
        # chrome_options.add_argument('--headless')
        # chrome_options.add_argument('--disable-gpu')
        # driver = webdriver.Chrome(chrome_options=chrome_options)

        mobile_emulation = {
            "deviceMetrics": {"width": 360, "height": 640, "pixelRatio": 3.0},
            "userAgent": "Mozilla/5.0 (Linux; Android 4.2.1; en-us; Nexus 5 Build/JOP40D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Mobile Safari/535.19"}
        chrome_options = Options()
        chrome_options.add_experimental_option("mobileEmulation", mobile_emulation)
        # chrome_options.add_argument('--headless')
        # chrome_options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(chrome_options = chrome_options)

        # driver = webdriver.Chrome()
        # driver.maximize_window()
        driver.implicitly_wait(2)
        print('Waiting...')
        driver.get(url)
        print('Waiting...')

        try:
            driver.find_elements_by_class_name('page-back-button')[-1].click()
        except (Exception) as e:
            print(e)
            driver.implicitly_wait(2)
            driver.find_elements_by_class_name('page-back-button')[-1].click()

        for i in range(2):
            time.sleep(3)
            driver.execute_script('window.scrollTo(0,document.body.scrollHeight)')

        driver.find_element_by_class_name('button-primary').click()

        time.sleep(3)

        no_more_button_flag = False

        while True:
            date_buttons = driver.find_elements_by_class_name('day')

            if self.finished_airline > 5:
                self.finished_airline = 0
                break

            if no_more_button_flag:
                driver.find_element_by_class_name('more').click()

                day_buttons = driver.find_elements_by_class_name('calendar-day-item')
                contrast_text = ' '

                for each in driver.find_elements_by_class_name('calendar-day-current'):
                    contrast_text = each.text
                    break

                currect_btn = False
                for each in day_buttons:
                    if currect_btn:
                        each.click()
                        break
                    if each.text == contrast_text:
                        currect_btn = True

                no_more_button_flag = False
                time.sleep(2)

            for each in date_buttons:

                no_more_button_flag = True

                if self.filter_ctrip_date(each.text) in self.finished_date:
                    continue

                print('--------' + self.filter_ctrip_date(each.text) + '--------')
                self.store_data(driver, con, cur, search_date, d_city, a_city)
                search_date = self.nextday(search_date)
                self.finished_date.add(self.filter_ctrip_date(each.text))
                each.click()
                no_more_button_flag = False
                time.sleep(1)
                break

            self.finished_airline += 1

        # try:
        #     self.store_data(r, con, cur)
        #     print('成功爬取' + ' ' + dcity + ' ' + acity + ' ' + dtime)
        #     self.totoal_count  += 1
        #     print(self.totoal_count)
        #     # proxies.valid_IP.add(ip)
        # except (Exception) as e:
        #     print(e)
        #     print('服务器繁忙' + ' ' + dcity + ' ' + acity + ' ' + dtime)
        #     pipe2.send('refresh')
        #     time.sleep(random.random() * 10 + 60)
        #     self.fail_flag = self.fail_flag + 1


        driver.quit()
        self.fail_flag = 0
        self.finished_airline = 0
        self.finished_date.clear()



    def crawl(self):

        print("爬虫进程开始运行")
        while (True):
            try:
                if not self.task.empty():
                    airline = self.task.get()

                    con = pymysql.connect(host='localhost', user='root', passwd='woshinibaba', db='flight',
                                          port=3306,
                                          charset='utf8')
                    cur = con.cursor()

                    table_date = time.strftime("%Y_%m_%d", time.localtime())
                    try:
                        cur.execute('''CREATE TABLE Flight_%s''' % table_date + '''
                        (

                            update_time  date             NOT NULL,
                            airline      varchar(255)     NOT NULL,
                            flight_id    varchar(255)     NOT NULL,
                            dept_date    date             NOT NULL,
                             dept_time    varchar(255)     NOT NULL,
                            dept_city    varchar(255)     NOT NULL,
                            dept_airport varchar(255)     NOT NULL,
                            arv_date     date             NOT NULL,
                            arv_time     varchar(255)     NOT NULL,
                            arv_city     varchar(255)     NOT NULL,
                            arv_airport  varchar(255)     NOT NULL,
                            flight_day   float            NOT NULL,
                            ontime_Rate  float            NOT NULL,
                            price        float            NOT NULL
                        );''')
                    except:
                        print("已经存在数据库")

                    if airline == 'end':
                        print('控制节点通知爬虫节点停止工作...')
                        # 接着通知其它节点停止工作
                        # self.result.put({'confirmed_airline': 'end', 'data': 'end'})
                        return

                    print('get: <<<<<<<<' + airline + '>>>>>>>>>>>')

                    target = airline.split('|')

                    today = datetime.date.today().strftime("%Y-%m-%d")

                    d_city = target[1]
                    a_city = target[2]

                    self.camouflage_broewser(today, d_city, a_city, con, cur)

                    # if self.fail_flag > 5:
                    #     self.result.put(airline)
                    #     print("[!]通知控制节点重新爬取:  " + airline)
                    #     self.fail_flag = 0


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