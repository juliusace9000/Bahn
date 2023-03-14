import http.client
from xml.etree import cElementTree as ElementTree
from XML import *
import pandas as pd

clientid = "52e03cb779352cb1a4665642fdca25fe"
apikey = "28d386dd30329b4a7975ec458409c424"
# clientid = "4b20783637b74f5655ca2363f31f5594"
# apikey = "14ad2eeee362b17212cd4ce382cd2453"


EVANO = "8002549" #Ahrensburg = 8000446, Winsen = 8006484, Ham HBF = 8002549
DATE = "230314"#YYMMDD
HOUR = "13" #HH

def drop(I, dict):
    for i in I:
        try: 
            dict = dict.drop(columns=[i])
        except:
            pass
    return dict

def getPlan(date, hour, evano):
    headers = {
    'DB-Client-Id': clientid,
    'DB-Api-Key': apikey,
    'accept': "application/xml"}
    conn = http.client.HTTPSConnection("apis.deutschebahn.com")
    request = str("/db-api-marketplace/apis/timetables/v1/plan/")+evano+"/"+date+"/"+hour
    conn.request("GET", request, headers=headers)
    res = conn.getresponse()
    data = res.read()
    root = ElementTree.XML(data.decode("utf-8"))
    return XmlDictConfig(root)

def getRecChg(evano):
    headers = {
    'DB-Client-Id': clientid,
    'DB-Api-Key': apikey,
    'accept': "application/xml"}
    conn = http.client.HTTPSConnection("apis.deutschebahn.com")
    request = str("/db-api-marketplace/apis/timetables/v1/rchg/")+evano
    conn.request("GET", request, headers=headers)
    res = conn.getresponse()
    data = res.read()
    root = ElementTree.XML(data.decode("utf-8"))
    return XmlDictConfig(root)

def getComChg(evano):
    headers = {
    'DB-Client-Id': clientid,
    'DB-Api-Key': apikey,
    'accept': "application/xml"}
    conn = http.client.HTTPSConnection("apis.deutschebahn.com")
    request = str("/db-api-marketplace/apis/timetables/v1/fchg/")+evano
    conn.request("GET", request, headers=headers)
    res = conn.getresponse()
    data = res.read()
    root = ElementTree.XML(data.decode("utf-8"))
    return XmlDictConfig(root)

delays = getComChg(EVANO)
delays = pd.json_normalize(delays['s'])
#print(delays[['dp.l','dp.m','ar.m.t','ar.m.c','ar.m.ts']])
#print(delays[['dp.pp','dp.ppth','dp.pt','m.from','m.to']])
#print(delays['dp.pp'], delays['dp.ppth'], delays['dp.pt'], delays['m.from'], delays['m.to'])
# print("\n\nPLAN:")
print(delays)
plan = None
for hour in range(0,24):
    try:
        planHour = getPlan(DATE, str(hour).zfill(2), EVANO)
    except:
        continue
    try:
        planHour = pd.json_normalize(planHour['s'])
        planHour = drop(['ar.pde', 'dp.pde', 'tl.f', 'tl.t', 'tl.n', 'tl.c', 'tl.o', 'ar.ppth', 'dp.ppth', 'ar.pp'], planHour)
        plan = pd.concat([plan, planHour])
    except:
        pass
plan.to_csv('plan.csv')
print(plan)

# ids = []
# for id in plan['id']:
#     print(id)
#     ids.append(id)
# i = 0
# table = pd.DataFrame()
# for d_id, d_ar_ct, d_ar_l, d_dp_ct, d_dp_l in zip(delays['id'], delays['ar.ct'], delays['ar.l'], delays['dp.ct'], delays['dp.l']):
#     print(d_dp_l)
#     for p_id, p_ar_pt, p_ar_l, p_dp_pt, p_dp_pp, p_dp_l in zip(plan['id'], plan['ar.pt'], plan['ar.l'], plan['dp.pt'], plan['dp.pp'], plan['dp.l']):
#         if p_id == d_id:
#             try:
#                 d = str(int(d_dp_ct)-int(p_dp_pt))
#             except:
#                 d = 'canceled'
#             if d_dp_l != d_dp_l:
#                 line = p_dp_l
#             else:
#                 line = d_dp_l
#             if line != line:
#                 continue
#             temp = pd.DataFrame([[d_id, p_dp_pt,      d_dp_ct,        line,   p_dp_pp,    d]],
#                         columns=['id',  'dep. plan', 'dep. cur.', 'line', 'platform', 'delay'],
#                         index=[i])
#             table=pd.concat([table, temp])
#             i+=1
# table.sort_values('dep. plan')
# table.reset_index()
# print(table)