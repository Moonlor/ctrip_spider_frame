import requests
import json

payload = json.dumps({"preprdid":"","trptpe":1,"flag":8,"searchitem":[{"dccode":"AKU","accode":"BSD","dtime":"2018-03-19"}],"version":[{"Key":"170710_fld_dsmid","Value":"Q"}],"head":{"cid":"09031047310033750449","ctok":"","cver":"1.0","lang":"01","sid":"8888","syscode":"09","auth":'null',"extension":[{"name":"protocal","value":"https"}]},"contentType":"json"})

r = requests.post('https://sec-m.ctrip.com/restapi/soa2/11781/Domestic/Swift/FlightList/Query?_fxpcqlniredt=09031047310033750449',
                  data = payload)

print(r.content.decode('utf-8'))