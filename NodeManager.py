#coding:utf-8
from multiprocessing import Queue, Process
from multiprocessing.managers import BaseManager

import time
import datetime

from AirlineManager import *
from DataOutput import *

#airline_q  将要爬取的航班队列
#result_q   爬虫返回的结果
#conn_q     获取的最新的需要爬取的航班队列
#store_q    将爬取的反馈信息进行存储


class NodeManager(object):

    def start_Manager(self, airline_q,result_q):

        #把创建的两个队列注册在网络上，利用register方法，callable参数关联了Queue对象，
        # 将Queue对象在网络中暴露
        BaseManager.register('get_task_queue',callable=lambda:airline_q)
        BaseManager.register('get_result_queue',callable=lambda:result_q)
        #绑定端口8011，设置验证口令，这个相当于对象的初始化
        manager=BaseManager(address=('127.0.0.1',8011),authkey=b'woshinibaba')
        #返回manager对象
        return manager



    def airline_manager_proc(self, airline_q, conn_q):
        o_day = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        c_day = time.strftime("%Y-%m-%d", time.localtime())
        while True:

            if(c_day == o_day):
                # time.sleep(1000)
                c_day = time.strftime("%Y-%m-%d", time.localtime())
                continue
            o_day = c_day
            print("开始新一天的爬取")

            airline_manager = AirlineManager(c_day)
            airline_manager.generate_airline_list(c_day)

            flag = False

            date = datetime.date.today() + datetime.timedelta(days=1)

            while True:

                # if airline_manager.new_airlines_size() == 0 and airline_manager.hot_airline_started == False:
                #     airline_manager.generate_airline_list(date.strftime("%Y-%m-%d"))

                while (airline_manager.has_new_airline()):

                    if (airline_manager.old_airlines_size() > 300):
                        if flag:
                            time.sleep(10)
                            break
                            # return
                        # 通知爬行节点工作结束
                        # for i in range(30):
                        #     airline_q.put('end')
                        print('当日爬取结束!')
                        # 关闭管理节点，同时存储set状态
                        airline_manager.save_progress('./' + c_day + '|new_airlines.txt',
                                                      airline_manager.new_airlines)
                        airline_manager.save_progress('./' + c_day + '|old_airlines.txt',
                                                      airline_manager.old_airlines)
                        flag = True

                    # 从航线管理器获得新的航线
                    new_airline = airline_manager.get_new_airline()
                    # 将新的航线发送给工作节点
                    airline_q.put(new_airline)
                    airline_manager.old_airlines.add(new_airline)

                # 将从result_solve_proc获取到的urls添加到URL管理器之间
                try:
                    if not conn_q.empty():
                        confirmed_airline = conn_q.get()
                        print("[+]航线：" + confirmed_airline + "从已爬航线中移除，加入待爬航线")
                        airline_manager.old_airlines.remove(confirmed_airline)
                        content = airline_q.get()
                        while content == 'end':
                            content = airline_q.get()
                        airline_q.put(content)
                        airline_manager.add_new_airlines(confirmed_airline)
                        airline_manager.save_progress('./' + c_day + '|new_airlines.txt',
                                                      airline_manager.new_airlines)
                        airline_manager.save_progress('./' + c_day + '|old_airlines.txt',
                                                      airline_manager.old_airlines)
                except (BaseException) as e:
                    time.sleep(0.1)  # 延时休息
                    print(e)





    def result_solve_proc(self,result_q,conn_q,store_q):
        while(True):
            try:
                if not result_q.empty():
                    content = result_q.get(True)
                    print("接收到爬虫的反馈" + content)
                    if content == 'end':
                        #结果分析进程接受通知然后结束
                        print('结果分析进程接受通知然后结束!')
                        store_q.put('end')
                        # return
                    else:
                        print('[!][!][!]航班：' + content + '  爬取失败!')
                        conn_q.put(content)  # airline为set类型

                    # store_q.put(content['data'])#解析出来的数据为dict类型
                else:
                    time.sleep(0.1)#延时休息
            except (BaseException) as e:
                print(e)
                time.sleep(0.1)#延时休息

    def store_proc(self,store_q):
        pass
        # output = DataOutput()
        # while True:
        #     if not store_q.empty():
        #         data = store_q.get()
        #         if data=='end':
        #             print('存储进程接受通知然后结束!')
        #             output.ouput_end(output.filepath)
        #
        #             return
        #         output.store_data(data)
        #     else:
        #         time.sleep(0.1)
        # pass


if __name__=='__main__':

    #初始化4个队列
    airline_q = Queue()
    result_q = Queue()
    store_q = Queue()
    conn_q = Queue()

    #创建分布式管理器
    node = NodeManager()
    manager = node.start_Manager(airline_q,result_q)

    #创建Airline管理进程、 数据提取进程和数据存储进程
    airline_manager_proc = Process(target=node.airline_manager_proc, args=(airline_q,conn_q))
    result_solve_proc = Process(target=node.result_solve_proc, args=(result_q,conn_q,store_q, ))
    store_proc = Process(target=node.store_proc, args=(store_q,))

    #启动3个进程和分布式管理器
    airline_manager_proc.start()
    result_solve_proc.start()
    # store_proc.start()
    manager.get_server().serve_forever()
