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
from apiclient import discovery
from google.oauth2 import service_account
import base64


def toTraditional(s):
    s = HanziConv.toTraditional(s)
    mapping = {
      "內濛古": "内蒙古",
      "颱灣": "台灣",
      "寜夏": "寧夏"
    }
    return mapping.get(s, s)


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


def upload_to_google_sheet(china_cases, international_cases):
    keys = ['dateTime', 'area', 'confirmed', 'died', 'crued']
    output_rows = [['international'] + [case[key] for key in keys] for case in international_cases]
    output_rows += [['national'] + [ case[key] for key in keys] for case in china_cases if case['city'] == '']
    keys = ['dateTime', 'city', 'confirmed', 'died', 'crued']
    output_rows += [['subnational'] + [ case[key] for key in keys] for case in china_cases if case['city'] != '']
    base64_credentials = os.getenv('GOOGLE_CRED', '')
    decoded_credentials = base64.b64decode(base64_credentials)
    info = json.loads(decoded_credentials)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    service = discovery.build('sheets', 'v4', credentials=credentials)

    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    print(spreadsheet_id)
    sheet_name = "Source"
    range_name = "%s!A2:G%d" % (sheet_name, len(output_rows) + 1)
    values = output_rows
    data = {
        'values' : values
    }

    service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range='%s!A2:G'% (sheet_name)).execute()
    service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, body=data, range=range_name, valueInputOption='USER_ENTERED').execute()
    pass


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
            d['city'] = toTraditional(d['city'])
            d.update({'area': case['area']})
            d.update(parse_int_in_dict(d))
            d.update(last_updated_dict)
            china_cases.append(d)
        case.update(last_updated_dict)
        china_cases.append(case)
    print(upsert_area_within_china(china_cases))
    international_cases = []
    for case in case_outside_list:
        case['area'] = toTraditional(case['area'])
        case.update(parse_int_in_dict(case))
        case.update(last_updated_dict)
        international_cases.append(case)
    print(upsert_international_area(international_cases))
    print(last_updated)
    upload_to_google_sheet(china_cases, international_cases)
    print("Finished")
    print_memory()



fetch_baidu()
