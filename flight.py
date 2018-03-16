import time
import json
import re

#==========================================================================
# print (time.strftime("%Y-%m-%d", time.localtime()) )


#==========================================================================
in_path = './cities.json'
out_path = './city_list.json'

with open(in_path, 'r', encoding='utf-8') as load_f:
    load_cities = json.load(load_f)

# pattern_code = re.compile(r'[A-Z]+')
# pattern_city = re.compile(r'[\u4e00-\u9fa5]+')

# print(re.search(pattern_code, '北京(BJS)').group(), re.search(pattern_city, '北京(BJS)').group())

hot_cities = load_cities['热门']

for each_group in load_cities:
    if each_group == '热门' :
        hot_cities = load_cities[each_group]
        for each_city in hot_cities :
            detail_information = each_city['data'].split('|')
            each_city['code'] = detail_information[3]
            each_city['index'] = detail_information[2]
            each_city['pinyin'] = detail_information[0]
            del each_city['data']
    else :
        for each_list_index in each_group:
            print(each_list_index)
            try :
                each_list = load_cities[each_group][each_list_index]
            except :
                continue
            for each_city in each_list:
                detail_information = each_city['data'].split('|')
                each_city['code'] = detail_information[3]
                each_city['index'] = detail_information[2]
                each_city['pinyin'] = detail_information[0]
                del each_city['data']


with open(out_path, 'w', encoding='utf-8') as out_f:
    json.dump(load_cities, out_f, ensure_ascii=False)


load_f.close()
out_f.close()



