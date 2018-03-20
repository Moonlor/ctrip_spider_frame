import random
import requests
import re
from RandomUserAgent import RandomUserAgent

__metaclass__ = type

class RandomIP:
    proxies = []
    def __init__(self):
        api_url = "http://dev.kuaidaili.com/api/getproxy/?orderid=962118612590645&num=500&b_android=1&b_iphone=1&b_ipad=1&protocol=2&method=2&an_an=1&an_ha=1&sp1=1&sep=3"
        r = requests.get(api_url)
        if r.status_code == 200:
            r.enconding = "utf-8"
            r = str(r.content).lstrip("b'")
            r = r.rstrip("'")
        for i in (r.split(' ')):
            i = 'http://' + i
            self.proxies.append(i)

        self.invalid_IP = set()

    def proxy(self):
        ip = random.choice(self.proxies)
        print(ip)
        while(ip in self.invalid_IP):
            ip = random.choice(self.proxies)
        return ip

