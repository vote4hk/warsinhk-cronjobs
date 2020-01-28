import math
import pandas as pd
import re
import datetime
import tika
from tika import parser
from io import BytesIO
from time import sleep
import requests
from tabula import read_pdf
import json
import os


def run_query(query):
    ADMIN_SECRET = os.getenv('ADMIN_SECRET')
    ENDPOINT = os.getenv('ENDPOINT')
    HEADERS = {
        'Content-Type': 'application/json',
        'X-Hasura-Admin-Secret': ADMIN_SECRET,
    }
    j = {"query": query, "operationName": "MyQuery"}
    resp = requests.post(ENDPOINT, data=json.dumps(j), headers=HEADERS)
    return resp.json()



def get_cases():
    pdf_chi_file = requests.get('https://www.chp.gov.hk/files/pdf/enhanced_sur_pneumonia_wuhan_chi.pdf')
    pdf_eng_file = requests.get('https://www.chp.gov.hk/files/pdf/enhanced_sur_pneumonia_wuhan_eng.pdf')
    chi_mapping = {'入住醫院名稱': 'hospital_zh',
               '患者狀況': 'status_zh'}
    eng_mapping = {'Date of\rlaboratory\rconfirmation': 'confirmation_date',
               'Case no.': 'case_no', 
               'Gender': 'gender_en',
               'Age': 'age', 'Discharge\rstatus': 'status_en', 'Name of hospital\radmitted': 'hospital_en'}
    en_df = read_pdf(BytesIO(pdf_eng_file.content), lattice=True)[0].rename(columns=eng_mapping)
    print(en_df.columns)
    ch_df = read_pdf(BytesIO(pdf_chi_file.content), lattice=True)[0].rename(columns=chi_mapping)
    combined_df = en_df.copy()
    combined_df['hospital_zh'] = ch_df['hospital_zh']
    combined_df['status_zh'] = ch_df['status_zh']
    combined_df['confirmation_date'] = combined_df['confirmation_date'].apply(lambda x: '{2}-{1}-{0}'.format(*x.split('/')))
    combined_df = combined_df[['case_no','confirmation_date','gender_en','age','hospital_en','status_en','hospital_zh','status_zh']]
    return combined_df.to_dict(orient='record')
    

def get_daily_stats():
    r = requests.get('https://www.chp.gov.hk/files/pdf/enhanced_sur_pneumonia_wuhan_eng.pdf')
    raw = parser.from_buffer(r.content)
    last_updated = None
    confirmed_case = None
    fulfilling = None
    ruled_out = None
    still_investigated = None
    for line in raw['content'].split('\n'):
        if len(line.strip()) == 0:
            continue
        line = line.lower()
        m = re.match('.*last updated on (.*)\)', line)
        if m is not None:
            last_updated = m.group(1)
        m = re.match('including (\d)+ confirmed', line)
        if m is not None:
            confirmed_case = int(m.group(1))
        m = re.match('.* total of (\d+) cases fulfilling .*', line)
        if m is not None:
            fulfilling = int(m.group(1))
        m = re.match('.* (\d+) cases which were ruled out .*', line)
        if m is not None:
            ruled_out = int(m.group(1))
        m = re.match('.* (\d+) cases were still hospitalised.*', line)
        if m is not None:
            still_investigated = int(m.group(1))        

    last_updated = datetime.datetime.strptime(last_updated, '%d %B %Y').date().strftime("%Y-%m-%d")
    output = {'last_updated': last_updated, 
              'confirmed_case': confirmed_case,
              'fulfilling': fulfilling,
              'ruled_out': ruled_out,
              'still_investigated': still_investigated
             }
    return output

def upsert_daily_stats(daily_stats):
    template = """
      {{
        last_updated: "{last_updated}",
        fulfilling: {fulfilling},
        ruled_out:{ruled_out},
        still_investigated:{still_investigated},
        confirmed_case:{confirmed_case},
      }}
    """
    objects = template.format(**daily_stats)
    objects = "[%s]" % objects
    query = """
      mutation MyQuery {
        insert_wars_DailyStats(
          objects: %s,
          on_conflict: {constraint: DailyStats_pkey,update_columns: [fulfilling,ruled_out, still_investigated,confirmed_case]}
        ){
          affected_rows
        }
      }
    """ % (objects)
    print(query)
    return run_query(query)


def upsert_cases(cases):
    template = """
      {{
        age:{age},
        case_no:{case_no},
        gender_en:"{gender_en}",
        hospital_en:"{hospital_en}",
        hospital_zh:"{hospital_zh}",
        status_en:"{status_en}",
        status_zh:"{status_zh}",
        confirmation_date:"{confirmation_date}",
      }}
    """

    objects = "\n".join([template.format(**case) for case in cases])
    objects = "[%s]" % objects
    query = """
      mutation MyQuery {
        insert_wars_Case(
          objects: %s,
          on_conflict: {constraint: Case_pkey,update_columns: [confirmation_date, gender_en, age, hospital_en,hospital_zh,status_en, status_zh]}
        ){
          affected_rows
        }
      }
    """ % (objects)
    print(query)
    return run_query(query)

            
daily_stats = get_daily_stats()
print(daily_stats)
print(upsert_daily_stats(daily_stats))

cases = get_cases()
print(cases)
print(upsert_cases(cases))
