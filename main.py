import http.client
from xml.etree import cElementTree as ElementTree
from XML import *
import pandas as pd
import time
clientid = "52e03cb779352cb1a4665642fdca25fe"
apikey = "28d386dd30329b4a7975ec458409c424"
# clientid = "4b20783637b74f5655ca2363f31f5594"
# apikey = "14ad2eeee362b17212cd4ce382cd2453"


EVANO = "8000446" #Ahrensburg = 8000446, Winsen = 8006484, Ham HBF = 8002549
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

while True:

    min = time.strftime("%M", time.localtime())
    hour = time.strftime("%H", time.localtime())
    if int(time.strftime("%M", time.localtime())) % 5 == 0 or True:
        try:
            table = None
            table = pd.read_feather("feather")
        except:
            table = None
            table = pd.DataFrame()
        delays = getComChg(EVANO)
        delays = pd.json_normalize(delays['s'])
        plan = getPlan(str(time.strftime("%Y%m%d", time.localtime()))[2:], str(hour).zfill(2), EVANO)
        plan = pd.json_normalize(plan['s'])
        
        for p_id, p_ar_pt, p_ar_l, p_dp_pt, p_dp_pp, p_dp_l in zip(plan['id'], plan['ar.pt'], plan['ar.l'], plan['dp.pt'], plan['dp.pp'], plan['dp.l']):
            for d_id, d_ar_ct, d_ar_l, d_dp_ct, d_dp_l in zip(delays['id'], delays['ar.ct'], delays['ar.l'], delays['dp.ct'], delays['dp.l']):
                if p_id == d_id:
                    try:
                        d = str(int(d_dp_ct)-int(p_dp_pt))
                    except:
                        d = 'canceled'
                    if d_dp_l != d_dp_l:
                        line = p_dp_l
                    else:
                        line = d_dp_l
                    if line != line:
                        continue
                    if line == "SEV":
                        continue
                    temp = pd.DataFrame([[d_id, int(p_dp_pt),      d_dp_ct,        line,   p_dp_pp,    d]],
                                columns=['id',  'dep. plan', 'dep. cur.', 'line', 'platform', 'delay'])
                    table=pd.concat([table, temp])
                    
        print(table.shape)
        table.drop_duplicates(subset=['dep. plan'], keep='last')
        print(table.shape)
        print(table['dep. plan'])
        table.sort_values('dep. plan')
        table = table.reset_index()
        table.to_feather("table")
        time.sleep(30)
    time.sleep(30)