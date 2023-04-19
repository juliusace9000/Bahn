import http.client
from xml.etree import cElementTree as ElementTree
from XML import *
import pandas as pd
import time
clientid = "52e03cb779352cb1a4665642fdca25fe"
apikey = "28d386dd30329b4a7975ec458409c424"
# clientid = "4b20783637b74f5655ca2363f31f5594"
# apikey = "14ad2eeee362b17212cd4ce382cd2453"


EVANO = "8002549" #Ahrensburg = 8000446, Winsen = 8006484, Ham HBF = 8002549 8002549
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
            table = pd.read_feather("table")
        except:
            table = None
            table = pd.DataFrame()
        delays = getComChg(EVANO)
        delays = pd.json_normalize(delays['s'])
        plan = getPlan(str(time.strftime("%Y%m%d", time.localtime()))[2:], str(hour).zfill(2), EVANO)
        plan = pd.json_normalize(plan['s'])
        i = 0
        # for p_id, p_ar_pt, p_ar_l, p_dp_pt, p_dp_pp, p_dp_l in zip(plan['id'], plan['ar.pt'], plan['ar.l'], plan['dp.pt'], plan['dp.pp'], plan['dp.l']):
        #     for d_id, d_ar_ct, d_ar_l, d_dp_ct, d_dp_l in zip(delays['id'], delays['ar.ct'], delays['ar.l'], delays['dp.ct'], delays['dp.l']):
                # if p_id == d_id:
                #     try:
                #         d = str(int(d_dp_ct)-int(p_dp_pt))
                #     except:
                #         d = 'canceled'
                #     if d_dp_l != d_dp_l:
                #         line = p_dp_l
                #     else:
                #         line = d_dp_l
                #     if line != line:
                #         continue
                #     if line == "SEV":
                #         continue
                #     temp = pd.DataFrame([[d_id, int(p_dp_pt),      d_dp_ct,        line,   p_dp_pp,    d]],
                #                 columns=['id',  'dep. plan', 'dep. cur.', 'line', 'platform', 'delay'], index = [i])
                #     table=pd.concat([table, temp])
                #     i+=1
                
        
        for i in range(0, len(plan.values)):
            for j in range(0, len(delays.values)):
                if plan['id'][i] == delays['id'][j] and delays['dp.l'][j] != delays['dp.l'][j]: #Regionalbahn
                    try:
                        d = str(int(delays['dp.ct'][i])-int(plan['dp.pt'][j]))
                    except:
                        d = '-1'
                    if delays['dp.l'][j] != delays['dp.l'][j]:
                        line = delays['dp.l'][j]
                    else:
                        line = delays['dp.l'][j]
                    if line != line:
                        continue
                    if line == "SEV":
                        continue
                    temp = pd.DataFrame([[delays['id'][j], int(plan['dp.pt'][i]), delays['dp.ct'][j], line,   plan['dp.pp'][i],    d]],
                                columns=['id',             'dep. plan',          'dep. cur.',        'line', 'platform', 'delay'], index = [i])
                    table=pd.concat([table, temp])
                    i+=1
                else: #plan['id'][i] == delays['id'][j] and delays['tl.n'] != delays['tl.n']:
                    try:
                        d = str(int(delays['dp.ct'][j])-int(plan['dp.pt'][i]))
                    except:
                        d = '-1'
                    line = delays['tl.c'][j]+delays['tl.n'][j]
                    if line != line:
                        continue
                    print(5)
                    try:
                        temp = pd.DataFrame([[delays['id'][j], EVANO,int(plan['dp.pt'][i]), delays['dp.ct'][j], line,   plan['dp.pp'][i],    d]],
                                    columns=['id',             'evano', 'dep. plan',          'dep. cur.',        'line', 'platform', 'delay'], index = [i])
                        table=pd.concat([table, temp])
                        i+=1
                    except:
                        pass
        
        print(table.shape)
        table.drop_duplicates(subset=['id'], keep='last', inplace=True)
        print(table.shape)
        print(table)
        #table.drop(['index'])
        try:
            del table['index']
            del table['level_0']
        except:
            pass
        table = table.reset_index()
        
        table.sort_values('dep. plan')
        try:
            del table['index']
            table.drop(['index'])
            del table['level_0']
        except:
            pass
        table.to_feather("table")
        print(table)
        time.sleep(3)
    time.sleep(30)
    
table = pd.read_feather("table")
print(table)