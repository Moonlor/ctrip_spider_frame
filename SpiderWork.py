from multiprocessing.managers import BaseManager
import requests
import json
import time
import re
import pymysql
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
        # self.m.connect()
        # 实现第三步：获取Queue的对象:
        # self.task = self.m.get_task_queue()
        # self.result = self.m.get_result_queue()
        print('init finish')

    def crawl(self):
        while(True):
            try:
                if not self.task.empty():
                    airline = self.task.get()

                    print(airline)

                    if airline =='end':
                        print('控制节点通知爬虫节点停止工作...')
                        #接着通知其它节点停止工作
                        self.result.put({'confirmed_airline':'end','data':'end'})
                        return
                    print('爬虫节点正在解析:%s'%airline)
                    pass
            except (EOFError) as e:
                print("连接工作节点失败")
                return
            except (Exception) as e:
                print(e)
                print('Crawl  fali ')

    def proData(self, r, con, cur):
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
            if isstop == 1:
                tran_city = i["mutilstn"][0]["fsitem"][0]["city"]
                tran_arrdate, tran_arrtime = i["mutilstn"][0]["fsitem"][0]["arrtime"].split(' ')
                tran_depdate, tran_deptime = i["mutilstn"][0]["fsitem"][0]["deptime"].split(' ')
            else:
                tran_city = ""
                tran_arrdate, tran_arrtime = "0001-01-01", ""
                tran_depdate, tran_deptime = "0001-01-01", ""
            try:
                flight_id = i["mutilstn"][0]["basinfo"]["flgno"] + '|' + i["mutilstn"][1]["basinfo"]["flgno"]
                airline = i["mutilstn"][0]["basinfo"]["airsname"] + '|' + i["mutilstn"][1]["basinfo"]["airsname"]
                model = i["mutilstn"][0]["craftinfo"]["cname"] + i["mutilstn"][0]["craftinfo"]["craft"] + '|' + \
                        i["mutilstn"][1]["craftinfo"]["cname"] + i["mutilstn"][1]["craftinfo"]["craft"]
                tran_city = arv_city
                arv_city = i["mutilstn"][1]["dportinfo"]["cityname"]
                arv_airport = i["mutilstn"][1]["aportinfo"]["aportsname"] + i["mutilstn"][1]["aportinfo"]["bsname"]
                tran_depdate, tran_deptime = arv_date, arv_time
                arv_date, arv_time = i["mutilstn"][1]["dateinfo"]["adate"].split(' ')
                isstop = 1
            except:
                isstop = 0
                # print("非中转航班")

            flight_day = i["fltoday"]

            for j in i["mutilstn"][0]["comlist"]:
                if j["type"] == 2:
                    ontime_Rate = j["stip"]

            # print(flight_id)
            # print(airline)
            # print(model)
            # print(dept_date)
            # print(dept_time)
            # print(dept_city)
            # print(dept_airport)
            # print(arv_date)
            # print(arv_time)
            # print(arv_city)
            # print(flight_day)
            # print(arv_airport)
            # print(ontime_Rate)

            price_1, price_2, price_3 = 99999, 99999, 99999
            for k in i["policyinfo"]:
                if re.match("经济舱", k["classinfor"][0]["display"]):
                    price_1 = k["tprice"]
                if re.match(".*公务舱", k["classinfor"][0]["display"]):
                    price_2 = k["tprice"]
                if re.match(".*头等舱", k["classinfor"][0]["display"]):
                    price_3 = k["tprice"]

            cur.execute(
                '  INSERT INTO FlightInfo(airline,flight_id,model,dept_date,dept_time,dept_city,dept_airport,arv_date,arv_time,arv_city,arv_airport,isstop,tran_city,tran_arrdate,tran_arrtime,tran_depdate,tran_deptime,flight_day,ontime_Rate,price_1,price_2,price_3) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (airline, flight_id, model, dept_date, dept_time, dept_city, dept_airport, arv_date, arv_time, arv_city,
                 arv_airport, isstop, tran_city, tran_arrdate, tran_arrtime, tran_depdate, tran_deptime, flight_day,
                 ontime_Rate, price_1, price_2, price_3))

            con.commit()

    def crawler(self, dcity, acity, dtime, cid, con, cur, headers, proxies, crawlerList):

        payload = json.dumps({"preprdid": "","trptpe": 1,"flag": 8,"searchitem": [{"dccode": "%s" % dcity, "accode": "%s" % acity, "dtime": "%s" % dtime}],"version": [{"Key": "170710_fld_dsmid", "Value": "Q"}],"head": {"cid": "%s" % cid, "ctok": "", "cver": "1.0", "lang": "01", "sid": "8888","syscode": "09", "auth": 'null',"extension": [{"name": "protocal", "value": "https"}]},"contentType": "json"})
        ip = proxies.proxy()

        print("正在使用IP ：" + ip + "  |  " "正在使用cid : " + cid)

        header = {'User-Agent': headers.head()}
        proxy = {'http': ip}
        time.sleep(2)

        tmp = requests.post('https://sec-m.ctrip.com/restapi/soa2/11781/Domestic/Swift/FlightList/Query?_fxpcqlniredt=' + cid, data=payload, headers=header, proxies=proxy)  # , proxies=proxy
        print(tmp.content.decode('utf-8'))
        r = eval(tmp.content.decode('utf-8'))

        try:
            self.proData(r, con, cur)
            print('成功爬取' + ' ' + dcity + ' ' + acity + ' ' + dtime)
            proxies.valid_IP.add(ip)
        except:
            print('服务器繁忙' + ' ' + dcity + ' ' + acity + ' ' + dtime)
            crawlerList.add(dcity + ' ' + acity + ' ' + dtime)
            proxies.invalid_IP.add(ip)

    def mainWork(self):
        con = pymysql.connect(host='localhost', user='root', passwd='woshinibaba', db='Flight', port=3306,
                              charset='utf8')
        cur = con.cursor()
        date = time.strftime("%Y-%m-%d", time.localtime())
        try:
            cur.execute('''CREATE TABLE %s''' %date + '''
                      (
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
                      tran_arrdate date             NOT NULL,
                      tran_arrtime varchar(255)     NOT NULL,
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

        cityToCodeList = {u'\u5357\u5145': u'NAO', u'\u5b81\u6ce2': u'NGB', u'\u8fbe\u53bf': u'DAX',
                          u'\u5fb7\u4ee4\u54c8': u'HXD', u'\u53f0\u5dde': u'HYN', u'\u5929\u6c34': u'THQ',
                          u'\u666e\u6d31': u'SYM', u'\u4e14\u672b': u'IQM', u'\u4e34\u6c82': u'LYI',
                          u'\u8354\u6ce2': u'LLB',
                          u'\u897f\u660c': u'XIC', u'\u9f99\u5ca9': u'LCX', u'\u4f5b\u5c71': u'FUO',
                          u'\u5b89\u987a': u'AVA',
                          u'\u4fdd\u5c71': u'BSD', u'\u5341\u5830': u'WDS', u'\u4e8c\u8fde\u6d69\u7279': u'ERL',
                          u'\u5609\u4e49': u'CYI', u'\u5317\u4eac': u'BJS', u'\u77f3\u6cb3\u5b50': u'SHF',
                          u'\u9526\u5dde': u'JNZ', u'\u5854\u57ce': u'TCG', u'\u5927\u5e86': u'DQA',
                          u'\u963f\u91cc': u'NGQ',
                          u'\u897f\u5b89': u'SIA', u'\u897f\u5b81': u'XNN', u'\u5580\u4ec0': u'KHG',
                          u'\u4e09\u4e9a': u'SYX',
                          u'\u6069\u65bd': u'ENH', u'\u7389\u6811': u'YUS', u'\u6e5b\u6c5f': u'ZHA',
                          u'\u8fde\u4e91\u6e2f': u'LYG', u'\u9ad8\u96c4': u'KHH', u'\u7261\u4e39\u6c5f': u'MDG',
                          u'\u4f0a\u6625': u'LDS', u'\u963f\u62c9\u5584\u5de6\u65d7': u'AXF', u'\u5415\u6881': u'LLV',
                          u'\u9e21\u897f': u'JXA', u'\u662d\u901a': u'ZAT', u'\u94f6\u5ddd': u'INC',
                          u'\u82b1\u83b2': u'HUN',
                          u'\u8fbe\u5dde': u'DAX', u'\u5b9c\u5bbe': u'YBP', u'\u516d\u76d8\u6c34': u'LPF',
                          u'\u6ee1\u6d32\u91cc': u'NZH', u'\u6b66\u5937\u5c71': u'WUS', u'\u901a\u8fbd': u'TGO',
                          u'\u6f8e\u6e56\u5217\u5c9b': u'MZG', u'\u5ef6\u5409': u'YNJ', u'\u6000\u5316': u'HJJ',
                          u'\u5408\u80a5': u'HFE', u'\u676d\u5dde': u'HGH', u'\u9ed1\u6cb3': u'HEK',
                          u'\u65e0\u9521': u'WUX',
                          u'\u53f0\u5357': u'TNN', u'\u4e4c\u9c81\u6728\u9f50': u'URC', u'\u4e07\u5dde': u'WXN',
                          u'\u4e5d\u6c5f': u'JIU', u'\u629a\u8fdc': u'FYJ', u'\u4e4c\u6d77': u'WUA',
                          u'\u963f\u514b\u82cf': u'AKU', u'\u53a6\u95e8': u'XMN', u'\u9ece\u5e73': u'HZH',
                          u'\u6f6e\u6c55': u'SWA', u'\u6606\u660e': u'KMG', u'\u53f0\u5317': u'TPE',
                          u'\u9075\u4e49\u8305\u53f0': u'WMT', u'\u6df1\u5733': u'SZX', u'\u91cd\u5e86': u'CKG',
                          u'\u6587\u5c71': u'WNH', u'\u6ca7\u6e90': u'CWJ', u'\u5efa\u4e09\u6c5f': u'JSJ',
                          u'\u6cc9\u5dde': u'JJN', u'\u4f0a\u5b81': u'YIN', u'\u5927\u7406': u'DLU',
                          u'\u63ed\u9633': u'SWA',
                          u'\u90af\u90f8': u'HDG', u'\u5357\u9633': u'NNY', u'\u5927\u540c': u'DAT',
                          u'\u8fd0\u57ce': u'YCU',
                          u'\u52a0\u683c\u8fbe\u5947': u'JGD', u'\u547c\u548c\u6d69\u7279': u'HET',
                          u'\u6bd5\u8282': u'BFJ',
                          u'\u743c\u6d77': u'BAR', u'\u4e4c\u5170\u6d69\u7279': u'HLH', u'\u67f3\u5dde': u'LZH',
                          u'\u5357\u5b81': u'NNG', u'\u679c\u6d1b': u'GMQ', u'\u6210\u90fd': u'CTU',
                          u'\u5e86\u9633': u'IQN',
                          u'\u8425\u53e3': u'YKH', u'\u5e93\u5c14\u52d2': u'KRL', u'\u4e09\u660e': u'SQJ',
                          u'\u91d1\u95e8': u'KNH', u'\u901a\u5316': u'TNH', u'\u590f\u6cb3': u'GXH',
                          u'\u5b89\u5e86': u'AQG',
                          u'\u671d\u9633': u'CHG', u'\u897f\u53cc\u7248\u7eb3': u'JHG', u'\u6e29\u5dde': u'WNZ',
                          u'\u5fb7\u5b8f': u'LUM', u'\u664b\u6c5f': u'JJN', u'\u4e0a\u9976': u'SQD',
                          u'\u4e0a\u6d77': u'SHA',
                          u'\u5ffb\u5dde': u'WUT', u'\u90b5\u9633': u'WGN', u'\u6f4d\u574a': u'WEF',
                          u'\u68a7\u5dde': u'WUZ',
                          u'\u5f20\u6396': u'YZY', u'\u6b66\u6c49': u'WUH', u'\u7ea2\u539f': u'AHJ',
                          u'\u6c60\u5dde': u'JUH',
                          u'\u4e3d\u6c5f': u'LJG', u'\u4f73\u6728\u65af': u'JMU', u'\u957f\u6cbb': u'CIH',
                          u'\u5b81\u8497': u'NLH', u'\u627f\u5fb7': u'CDE', u'\u65e5\u7167': u'RIZ',
                          u'\u6cf0\u5dde': u'YTY',
                          u'\u9075\u4e49\u65b0\u821f': u'ZYI', u'\u54c8\u5c14\u6ee8': u'HRB', u'\u9a6c\u7956': u'MFK',
                          u'\u5bcc\u8574': u'FYN', u'\u978d\u5c71': u'AOG', u'\u6f6e\u5dde': u'SWA',
                          u'\u5174\u4e49': u'ACX',
                          u'\u70df\u53f0': u'YNT', u'\u5929\u6d25': u'TSN', u'\u5e38\u5fb7': u'CGD',
                          u'\u6797\u829d': u'LZY',
                          u'\u9ed4\u6c5f': u'JIQ', u'\u963f\u62c9\u5584\u53f3\u65d7': u'RHT', u'\u957f\u6c99': u'CSX',
                          u'\u62c9\u8428': u'LXA', u'\u9a6c\u516c': u'MZG', u'\u6986\u6797': u'UYN',
                          u'\u4e2d\u536b': u'ZHY',
                          u'\u9102\u5c14\u591a\u65af': u'DSN', u'\u798f\u5dde': u'FOC', u'\u8944\u9633': u'XFN',
                          u'\u6cf8\u5dde': u'LZO', u'\u8292\u5e02': u'LUM', u'\u6c55\u5934': u'SWA',
                          u'\u4e34\u6ca7': u'LNJ',
                          u'\u5410\u9c81\u756a': u'TLQ', u'\u6cb3\u6c60': u'HCJ', u'\u7a3b\u57ce': u'DCY',
                          u'\u6d4e\u5357': u'TNA', u'\u5510\u5c71': u'TVS', u'\u767d\u5c71': u'NBS',
                          u'\u77f3\u5bb6\u5e84': u'SJW', u'\u5357\u660c': u'KHN', u'\u5f20\u5bb6\u754c': u'DYG',
                          u'\u795e\u519c\u67b6': u'HPG', u'\u6d77\u62c9\u5c14': u'HLD', u'\u94dc\u4ec1': u'TEN',
                          u'\u5357\u901a': u'NTG', u'\u6f9c\u6ca7': u'JMJ', u'\u5e7f\u5dde': u'CAN',
                          u'\u5b9c\u6625': u'YIC',
                          u'\u592a\u539f': u'TYN', u'\u961c\u9633': u'FUG', u'\u677e\u539f': u'YSQ',
                          u'\u9f50\u9f50\u54c8\u5c14': u'NDG', u'\u5927\u8fde': u'DLC', u'\u666f\u5fb7\u9547': u'JDZ',
                          u'\u5f90\u5dde': u'XUZ', u'\u5f20\u5bb6\u53e3': u'ZQZ', u'\u8d35\u9633': u'KWE',
                          u'\u91d1\u660c': u'JIC', u'\u5e03\u5c14\u6d25': u'KJI', u'\u73e0\u6d77': u'ZUH',
                          u'\u4e1c\u8425': u'DOY', u'\u5e38\u5dde': u'CZX', u'\u4e39\u4e1c': u'DDG',
                          u'\u6d4e\u5b81': u'JNG',
                          u'\u5df4\u5f66\u6dd6\u5c14': u'RLK', u'\u6c38\u5dde': u'LLF', u'\u8862\u5dde': u'JUZ',
                          u'\u9ec4\u5c71': u'TXN', u'\u60e0\u5dde': u'HUZ', u'\u65b0\u6e90': u'NLT',
                          u'\u4e49\u4e4c': u'YIW',
                          u'\u963f\u5c14\u5c71': u'YIE', u'\u767e\u8272': u'AEB', u'\u5170\u5dde': u'LHW',
                          u'\u5ef6\u5b89': u'ENY', u'\u6885\u5dde': u'MXZ', u'\u4e4c\u5170\u5bdf\u5e03': u'UCB',
                          u'\u548c\u7530': u'HTN', u'\u65e5\u5580\u5219': u'RKZ', u'\u626c\u5dde': u'YTY',
                          u'\u6d77\u53e3': u'HAK', u'\u957f\u6625': u'CGQ', u'\u90d1\u5dde': u'CGO',
                          u'\u54c8\u5bc6': u'HMI',
                          u'\u5e93\u8f66': u'KCA', u'\u6d1b\u9633': u'LYA', u'\u970d\u6797\u90ed\u52d2': u'HUO',
                          u'\u5609\u5cea\u5173': u'JGN', u'\u666f\u6d2a': u'JHG', u'\u6c88\u9633': u'SHE',
                          u'\u7ef5\u9633': u'MIG', u'\u514b\u62c9\u739b\u4f9d': u'KRY', u'\u6fb3\u95e8': u'MFM',
                          u'\u6c49\u4e2d': u'HZG', u'\u8d63\u5dde': u'KOW', u'\u5a01\u6d77': u'WEH',
                          u'\u5357\u7aff': u'LZN',
                          u'\u8fea\u5e86': u'DIG', u'\u989d\u6d4e\u7eb3\u65d7': u'EJN', u'\u76d0\u57ce': u'YNZ',
                          u'\u817e\u51b2': u'TCZ', u'\u963f\u52d2\u6cf0': u'AAT', u'\u6dee\u5b89': u'HIA',
                          u'\u51ef\u91cc': u'KJH', u'\u79e6\u7687\u5c9b': u'BPE', u'\u683c\u5c14\u6728': u'GOQ',
                          u'\u624e\u5170\u5c6f': u'NZL', u'\u53f0\u4e1c': u'TTT', u'\u6f20\u6cb3': u'OHE',
                          u'\u8861\u9633': u'HNY', u'\u6842\u6797': u'KWL', u'\u838e\u8f66': u'QSZ',
                          u'\u4e95\u5188\u5c71': u'JGS', u'\u5b9c\u660c': u'YIH', u'\u660c\u90fd': u'BPX',
                          u'\u535a\u4e50': u'BPL', u'\u8d64\u5cf0': u'CIF', u'\u6500\u679d\u82b1': u'PZI',
                          u'\u4e34\u6c7e': u'LFQ', u'\u4e5d\u5be8\u6c9f': u'JZH', u'\u9521\u6797\u6d69\u7279': u'XIL',
                          u'\u767d\u57ce': u'DBC', u'\u9999\u6e2f': u'HKG', u'\u5e7f\u5143': u'GYS',
                          u'\u5357\u4eac': u'NKG',
                          u'\u821f\u5c71': u'HSN', u'\u9752\u5c9b': u'TAO', u'\u5317\u6d77': u'BHY',
                          u'\u56fa\u539f': u'GYU',
                          u'\u82b1\u571f\u6c9f': u'HTT', u'\u5305\u5934': u'BAV', u'\u6566\u714c': u'DNH',
                          u'\u5eb7\u5b9a': u'KGT', u'\u53f0\u4e2d': u'RMQ'}
        cityList = [u'北京', u'上海']
        dateList = ['2018-04-01', '2018-04-02']
        crawlerList = CrawlerList()
        headers = RandomUserAgent()
        proxies = RandomIP()

        for dept_city in cityList:
            for arv_city in cityList:
                if dept_city != arv_city:
                    dept_city_code = cityToCodeList[dept_city]
                    arv_city_code = cityToCodeList[arv_city]
                    for date in dateList:
                        self.crawler(dept_city_code, arv_city_code, date, "09031068311537560002", con, cur, headers, proxies, crawlerList)

        while (crawlerList.len() != 0):
            dept_city_code, arv_city_code, date = crawlerList.delete().split(' ')
            print('重新爬取' + ' ' + dept_city_code + ' ' + arv_city_code + ' ' + date)
            self.crawler(dept_city_code, arv_city_code, date, "09031068311537560002", con, cur, headers, proxies, crawlerList)




if __name__=="__main__":
    spider = SpiderWork()
    spider.mainWork()