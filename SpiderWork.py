from multiprocessing.managers import BaseManager
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
from RandomIP import RandomIP
from CrawlerList import CrawlerList

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
        print('init finish')


    def store_data(self, r, con, cur):
        for i in r["fltitem"]:
            flight_id = i["mutilstn"][0]["basinfo"]["flgno"]

            airline = i["mutilstn"][0]["basinfo"]["airsname"]

            model = i["mutilstn"][0]["craftinfo"]["cname"] + i["mutilstn"][0]["craftinfo"]["craft"]

            dept_date, dept_time = i["mutilstn"][0]["dateinfo"]["ddate"].split(' ')

            dept_city = i["mutilstn"][0]["dportinfo"]["cityname"]

            dept_airport = i["mutilstn"][0]["dportinfo"]["aportsname"] + i["mutilstn"][0]["dportinfo"]["bsname"]

            arv_date, arv_time = i["mutilstn"][0]["dateinfo"]["adate"].split(' ')

            arv_city = i["mutilstn"][0]["aportinfo"]["cityname"]

            arv_airport = i["mutilstn"][0]["aportinfo"]["aportsname"] + i["mutilstn"][0]["aportinfo"]["bsname"]

            isstop = i["mutilstn"][0]["isstop"]

            try:
                if isstop == 1:
                    tran_city = i["mutilstn"][0]["fsitem"][0]["city"]
                    tran_arvdate, tran_arvtime = i["mutilstn"][0]["fsitem"][0]["arrtime"].split(' ')
                    tran_depdate, tran_deptime = i["mutilstn"][0]["fsitem"][0]["deptime"].split(' ')
                else:
                    tran_city = ""
                    tran_arvdate, tran_arvtime = "0001-01-01", ""
                    tran_depdate, tran_deptime = "0001-01-01", ""
            except:
                tran_city = ""
                tran_arvdate, tran_arvtime = "0001-01-01", ""
                tran_depdate, tran_deptime = "0001-01-01", ""

            try:
                flight_id = i["mutilstn"][0]["basinfo"]["flgno"] + '|' + i["mutilstn"][1]["basinfo"]["flgno"]
                airline = i["mutilstn"][0]["basinfo"]["airsname"] + '|' + i["mutilstn"][1]["basinfo"]["airsname"]
                model = i["mutilstn"][0]["craftinfo"]["cname"] + i["mutilstn"][0]["craftinfo"]["craft"] + '|' + \
                        i["mutilstn"][1]["craftinfo"]["cname"] + i["mutilstn"][1]["craftinfo"]["craft"]
                tran_city = arv_city
                arv_city = i["mutilstn"][1]["dportinfo"]["cityname"]
                arv_airport = i["mutilstn"][1]["aportinfo"]["aportsname"] + i["mutilstn"][1]["aportinfo"]["bsname"]
                tran_arvdate, tran_arvtime = arv_date, arv_time
                tran_depdate, tran_deptime = i["mutilstn"][1]["dateinfo"]["ddate"].split(' ')
                arv_date, arv_time = i["mutilstn"][1]["dateinfo"]["adate"].split(' ')
                isstop = 1
            except:
                pass
                # print("非中转航班")

            flight_day = i["fltoday"]

            ontime_Rate = 0
            try:
                for j in i["mutilstn"][0]["comlist"]:
                    if j["type"] == 2:
                        ontime_Rate = j["stip"]
            except:
                pass

            price_1, price_2, price_3 = 99999, 99999, 99999
            for k in i["policyinfo"]:
                if re.match("经济舱", k["classinfor"][0]["display"]):
                    price_1 = k["tprice"]
                if re.match(".*公务舱", k["classinfor"][0]["display"]):
                    price_2 = k["tprice"]
                if re.match(".*头等舱", k["classinfor"][0]["display"]):
                    price_3 = k["tprice"]

            date = time.strftime("%Y_%m_%d", time.localtime())
            cur.execute(
                '  INSERT INTO Flight_%s' % date + '(airline,flight_id,model,dept_date,dept_time,dept_city,dept_airport,arv_date,arv_time,arv_city,arv_airport,isstop,tran_city,tran_arvdate,tran_arvtime,tran_depdate,tran_deptime,flight_day,ontime_Rate,price_1,price_2,price_3) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (airline, flight_id, model, dept_date, dept_time, dept_city, dept_airport, arv_date, arv_time, arv_city,
                 arv_airport, isstop, tran_city, tran_arvdate, tran_arvtime, tran_depdate, tran_deptime, flight_day,
                 ontime_Rate, price_1, price_2, price_3))

            con.commit()

    def crawler(self, dcity, acity, dtime, cid, con, cur, headers):

        payload = json.dumps({"preprdid": "","trptpe": 1,"flag": 8,"searchitem": [{"dccode": "%s" % dcity, "accode": "%s" % acity, "dtime": "%s" % dtime}],"version": [{"Key": "170710_fld_dsmid", "Value": "Q"}],"head": {"cid": "%s" % cid, "ctok": "", "cver": "1.0", "lang": "01", "sid": "8888","syscode": "09", "auth": 'null',"extension": [{"name": "protocal", "value": "https"}]},"contentType": "json"})
        # ip = proxies.proxy()

        # print("正在使用IP ：" + ip + "  |  " "正在使用cid : " + cid)
        print("正在使用cid : " + cid)

        header = {'User-Agent': headers.head()}
        # proxy = {'http': ip}
        time.sleep(2)

        tmp = requests.post('https://sec-m.ctrip.com/restapi/soa2/11781/Domestic/Swift/FlightList/Query?_fxpcqlniredt=' + cid, data=payload, headers=header) # proxies=proxy

        r = eval(tmp.content.decode('utf-8'))
        print(r)

        try:
            self.store_data(r, con, cur)
            print('成功爬取' + ' ' + dcity + ' ' + acity + ' ' + dtime)
            # proxies.valid_IP.add(ip)
        except (Exception) as e:
            print(e)
            print('服务器繁忙' + ' ' + dcity + ' ' + acity + ' ' + dtime)
            time.sleep(random.random() * 10)
            # crawlerList.add(dcity + ' ' + acity + ' ' + dtime)
            # proxies.invalid_IP.add(ip)

    def camouflage_broewser(self, pipe, date, d_city, a_city):
        url = 'https://m.ctrip.com/html5/flight/swift/domestic/' + d_city + '/' + a_city + '/' + date
        # url = 'https://m.ctrip.com/html5/flight/swift/index'
        driver = webdriver.PhantomJS()
        driver.maximize_window()
        driver.implicitly_wait(2)
        print('Waiting...')
        driver.get(url)
        print('Waiting...')

        for i in range(2):
            time.sleep(0.3)
            driver.execute_script('window.scrollTo(0,document.body.scrollHeight)')

        remote_cookies = driver.get_cookies()

        local_cookies = {}

        for each in remote_cookies:
            print(each)
            name = each['name']
            value = each['value']
            local_cookies[name] = value

        local_cookies['MKT_Pagesource'] = 'H5'


        pipe.send(local_cookies)

        if pipe.recv() == 'ok':
            print("浏览器关闭，单条航线爬取完毕")
            driver.quit()

        driver.quit()

    def mainWork(self, date, d_city, a_city, cookies, con, cur):

        headers = RandomUserAgent()
        cid = cookies['GUID']
        # proxies = RandomIP()

        self.crawler(d_city, a_city, date, cid, con, cur, headers)

        # while (crawlerList.len() != 0):
        #     dept_city_code, arv_city_code, date = crawlerList.delete().split(' ')
        #     print('重新爬取' + ' ' + dept_city_code + ' ' + arv_city_code + ' ' + date)
        #     self.crawler(dept_city_code, arv_city_code, date, "09031157411153518960", con, cur, headers, proxies, crawlerList)

    def crawl(self):

        pip = multiprocessing.Pipe()
        pipe1 = pip[0]
        pipe2 = pip[1]

        print("爬虫进程开始运行")
        while (True):
            try:
                if not self.task.empty():
                    airline = self.task.get()

                    con = pymysql.connect(host='127.0.0.1', user='papa', passwd='woshinibaba', db='Flight',
                                          port=3306,
                                          charset='utf8')
                    cur = con.cursor()

                    table_date = time.strftime("%Y_%m_%d", time.localtime())
                    try:
                        cur.execute('''CREATE TABLE Flight_%s''' % table_date + ''' (
                                          airline      varchar(255)     NOT NULL,
                                          flight_id    varchar(255)     NOT NULL,
                                          model        varchar(255)     NOT NULL,
                                          dept_date    date             NOT NULL,
                                          dept_time    varchar(255)     NOT NULL,
                                          dept_city    varchar(255)     NOT NULL,
                                          dept_airport varchar(255)     NOT NULL,
                                          arv_date     date             NOT NULL,
                                          arv_time     varchar(255)     NOT NULL,
                                          arv_city     varchar(255)     NOT NULL,
                                          arv_airport  varchar(255)     NOT NULL,
                                          isstop       float            NOT NULL,
                                          tran_city    varchar(255)     NOT NULL,
                                          tran_arvdate date             NOT NULL,
                                          tran_arvtime varchar(255)     NOT NULL,
                                          tran_depdate date             NOT NULL,
                                          tran_deptime varchar(255)     NOT NULL,
                                          flight_day   float            NOT NULL,
                                          ontime_Rate  float            NOT NULL,
                                          price_1      float            NOT NULL,
                                          price_2      float            NOT NULL,
                                          price_3      float            NOT NULL
                                          );''')
                    except:
                        print("已经存在数据库")

                    if airline == 'end':
                        print('控制节点通知爬虫节点停止工作...')
                        # 接着通知其它节点停止工作
                        self.result.put({'confirmed_airline': 'end', 'data': 'end'})
                        return

                    print('get: <<<<<<<<' + airline + '>>>>>>>>>>>')

                    target = airline.split('|')
                    date_list = []
                    for i in range(180):
                        date_list.append((datetime.date.today() + datetime.timedelta(days=i + 1)).strftime("%Y-%m-%d"))


                    d_city = target[1]
                    a_city = target[2]

                    browser_proc = multiprocessing.Process(target = self.camouflage_broewser, args = (pipe1, date_list[0], d_city, a_city))
                    browser_proc.start()

                    cookie = pipe2.recv()

                    for i in range(len(date_list)):
                        print('爬虫节点正在解析: 旅行日期 %s | 出发城市 %s | 到达城市 %s' % (date_list[i], d_city, a_city))
                        self.mainWork(date_list[i], d_city, a_city, cookie,con ,cur)
                        time.sleep(random.random() + 1)

                    pipe2.send('ok')
                    browser_proc.join()
                    print("浏览器已经关闭，线程同步")

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