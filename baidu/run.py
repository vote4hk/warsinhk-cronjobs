import requests
from bs4 import BeautifulSoup,  SoupStrainer
import json
import hashlib
import os
import urllib
import multiprocessing
from time import sleep
import sys
from datetime import datetime
from dateutil.tz import tzoffset
from datetime import datetime, timedelta
from hanziconv import HanziConv

toTraditional = HanziConv.toTraditional


def get_memory():
    """ Look up the memory usage, return in MB. """
    proc_file = '/proc/{}/status'.format(os.getpid())
    scales = {'KB': 1024.0, 'MB': 1024.0 * 1024.0}
    with open(proc_file, 'rU') as f:
        for line in f:
            if 'VmHWM:' in line:
                fields = line.split()
                size = int(fields[1])
                scale = fields[2].upper()
                return size*scales[scale]/scales['MB']
    return 0.0 

def print_memory():
    print("Peak: %f MB" % (get_memory()))

def run_query(query):
    ADMIN_SECRET = os.getenv('ADMIN_SECRET')
    ENDPOINT = os.getenv('ENDPOINT')
    HEADERS = {
        'Content-Type': 'application/json',
        'X-Hasura-Admin-Secret': ADMIN_SECRET,
    }
    j = {"query": query, "operationName": "MyQuery"}
    resp = requests.post(ENDPOINT, data=json.dumps(j), headers=HEADERS)
    j = resp.json()
    if "data" not in j:
        print(query)
    return j


def upsert_area_within_china(areas):
    template = """
      {{
        confirmed: {confirmed},
        died: {died},
        crued: {crued},
        curConfirm: {curConfirm},
        area: "{area}"
        date: "{date}",
        time: "{time}",
        dateTime: "{dateTime}",
        city: "{city}"
      }}
    """
    objects = ",\n".join([template.format(**area) for area in areas])
    objects = "[%s]" % objects
    query = """
      mutation MyQuery {
        insert_wars_BaiduChinaData(
          objects: %s,
          on_conflict: {constraint: BaiduChinaData_pkey,update_columns: []}
        ){
          affected_rows
          returning {
            date
            dateTime
            area
          }
        }
      }
    """ % (objects)
    return run_query(query)   


def upsert_international_area(areas):
    template = """
      {{
        confirmed: {confirmed},
        died: {died},
        crued: {crued},
        area: "{area}"
        date: "{date}",
        time: "{time}",
        dateTime: "{dateTime}",
      }}
    """
    objects = ",\n".join([template.format(**area) for area in areas])
    objects = "[%s]" % objects
    query = """
      mutation MyQuery {
        insert_wars_BaiduInternationalData(
          objects: %s,
          on_conflict: {constraint: BaiduInternationalData_pkey,update_columns: []}
        ){
          affected_rows
          returning {
            date
            dateTime
            area
          }
        }
      }
    """ % (objects)
    return run_query(query)   


def parse_int_in_dict(case):
    for key in ['died', 'crued', 'confirmed', 'curConfirm']:
        if key in case:
            case[key] = int('0' if case[key] == '' else case[key])
    return case


def fetch_baidu():
    print("Fetching Page")
    url = "https://voice.baidu.com/act/newpneumonia/newpneumonia"
    r = requests.get(url)
    only_script = SoupStrainer("script", {"id": "captain-config"})
    soup = BeautifulSoup(r.content, features="html.parser", parse_only=only_script)
    script = soup.find("script")
    j = json.loads(script.text)
    comp = j["component"][0]
    last_updated = comp["mapLastUpdatedTime"]
    last_updated_date, last_updated_time = last_updated.split(' ')
    last_updated_dict = {'dateTime': last_updated, 'date': last_updated_date, 'time': last_updated_time}
    case_list = comp["caseList"]
    case_outside_list = comp["caseOutsideList"]
    china_cases = []
    for case in case_list:
        case.update(parse_int_in_dict(case))
        case['area'] = toTraditional(case['area'])
        case['city'] = ''
        sub_list = case["subList"]
        del case["subList"]
        for d in sub_list:
            d.update({'area': case['area']})
            d.update(parse_int_in_dict(d))
            d.update(last_updated_dict)
            china_cases.append(d)
        case.update(last_updated_dict)
        china_cases.append(case)
    print(upsert_area_within_china(china_cases))
    international_cases = []
    for case in case_outside_list:
        case.update(parse_int_in_dict(case))
        case.update(last_updated_dict)
        international_cases.append(case)
        print(toTraditional(case['area']), case)
    print(upsert_international_area(international_cases))
    print(last_updated)
    print("Finished")
    print_memory()



fetch_baidu()
