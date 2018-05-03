# -!- coding: utf-8 -!-

import requests
import json
import time
import re



headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept - Encoding':'gzip, deflate, br',
               'Accept-Language':'zh-Hans-CN, zh-Hans; q=0.9',
               'Connection':'Keep-Alive',
               'Host':'api.dongqiudi.com',
                'Upgrade-Insecure-Requests': '1',
               'User-Agent':'Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Mobile Safari/537.36'}


r = requests.get('https://api.dongqiudi.com/data/v1/person_ranking/0?type=goals&version=149&refer=person_ranking&season_id=1933', headers = headers, verify=False)

print(r.content.decode('unicode_escape'))