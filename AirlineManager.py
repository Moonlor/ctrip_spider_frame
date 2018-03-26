#coding:utf-8
import pickle
import hashlib
import json

class AirlineManager(object):

    def __init__(self, current_date):
        self.new_airlines = self.load_progress('./' + current_date + '|new_airlines.txt')#未爬取URL集合
        self.old_airlines = self.load_progress('./' + current_date + '|old_airlines.txt')#已爬取URL集合
        self.urgent_request = set()
        self.normal_airlines = set()
        self.hot_airline_started = False

#==================集合相关=======================

    def has_urgent_request(self):
        return self.urgent_request_size() != 0

    def urgent_request_size(self):
        return len(self.urgent_request)

    def has_new_airline(self):
        #当热门航线被爬取完成时，将剩余的航线加入热门航线集合

        # if self.new_airlines_size() == 0 and self.hot_airline_started == True:
        #     self.new_airlines.clear()
        #     self.new_airlines = self.new_airlines.union(self.normal_airlines)
        #     self.hot_airline_started = False

        return self.new_airlines_size() != 0

    def new_airlines_size(self):
        return len(self.new_airlines)

    def old_airlines_size(self):
        return len(self.old_airlines)

# ==================初始化，生成爬取列表===============================

    def generate_airline_list(self, date):
        hot_airline_set = set()
        normal_airline_set = set()
        self.hot_airline_started = False
        with open('./city_list.json', 'r', encoding='utf-8') as load_f:
            load_cities = json.load(load_f)

        hot_cities_list = []
        all_cities_list = []

        for each_group in load_cities:
            if each_group == '热门':
                hot_cities = load_cities[each_group]
                for each_city in hot_cities:
                    hot_cities_list.append(each_city)
            else:
                for each_list_index in each_group:
                    try:
                        each_list = load_cities[each_group][each_list_index]
                    except:
                        continue
                    for each_city in each_list:
                        all_cities_list.append(each_city)

        for each_depart_city in hot_cities_list:
            for each_arrive_city in hot_cities_list:
                if (each_depart_city == each_arrive_city):
                    continue
                airline = date + '|' + each_depart_city['code'] + '|' + each_arrive_city['code'] + '|'\
                          + each_depart_city['display'] + '|' + each_arrive_city['display']
                if airline not in hot_airline_set:
                    hot_airline_set.add(airline)


        for each_depart_city  in all_cities_list:
            for each_arrive_city in all_cities_list:
                if (each_depart_city == each_arrive_city):
                    continue
                airline = date + '|' + each_depart_city['code'] + '|' + each_arrive_city['code'] + '|' \
                          + each_depart_city['display'] + '|' + each_arrive_city['display']
                if airline not in hot_airline_set:
                    normal_airline_set.add(airline)

        self.new_airlines = self.new_airlines.union(hot_airline_set)
        self.normal_airlines = self.normal_airlines.union(normal_airline_set)
        self.hot_airline_started = True

        load_f.close()


#==================I/O相关===============================

    def load_progress(self,path):
        #加载爬取进度
        print('[+] 从文件加载进度: %s' % path)
        try:
            with open(path, 'rb') as f:
                tmp = pickle.load(f)
                return tmp
        except:
            print('[!] 无进度文件, 创建: %s' % path)
        return set()

    def save_progress(self,path,data):
        #保存进度
        with open(path, 'wb') as f:
            pickle.dump(data, f)

# ==================航线获取相关===============================

    def get_new_airline(self):
        #获取一条待爬取的新航线，并将这条航线信息存储到set()中
        new_airline = self.new_airlines.pop()
        #当航线信息过长时，启用md5哈希，并取128位来避免过多
        # m = hashlib.md5()
        # m.update(new_airline)
        # self.old_airlines.add(m.hexdigest()[8:-8])
        return new_airline

    def add_new_airline(self, airline):
        #添加单条航线
        if airline is None:
            return
        # m = hashlib.md5()
        # m.update(url)
        # url_md5 =  m.hexdigest()[8:-8]
        if airline not in self.new_airlines and airline not in self.old_airlines:
            self.new_airlines.add(airline)

    def add_new_airlines(self, airlines):
        #添加多条航线
        if airlines is None or len(airlines) == 0:
            return
        for airline in airlines:
            self.add_new_airline(airline)


