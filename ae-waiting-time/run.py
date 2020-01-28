import math
import pandas as pd
import re
import datetime
from io import StringIO, BytesIO
from time import sleep
import requests
from tabula import read_pdf
import json
import os
from bs4 import BeautifulSoup
from tabula import read_pdf
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.pdfpage import PDFPage
from pdfminer.layout import LAParams


def clease_str_nan_to_int(s):
    if type(s) is float and math.isnan(s):
        return 0
    s = s.strip()
    if s == '–':
        return 0
    else:
        return int(s.replace('*', '').replace('#', '').replace('%', ''))
    
def cleanse_hospital_name(s):
    if type(s) is not str:
        return s
    return s.replace('^', '').strip()


def extract_text(fp, password='', page_numbers=None, maxpages=0,
                 caching=True, codec='utf-8', laparams=None):
    if laparams is None:
        laparams = LAParams()

    with StringIO() as output_string:
        rsrcmgr = PDFResourceManager()
        device = TextConverter(rsrcmgr, output_string, codec=codec,
                               laparams=laparams)
        interpreter = PDFPageInterpreter(rsrcmgr, device)

        for page in PDFPage.get_pages(
                fp,
                page_numbers,
                maxpages=maxpages,
                password=password,
                caching=caching,
                check_extractable=True,
        ):
            interpreter.process_page(page)
        return output_string.getvalue()

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


def get_last_30_days_service_demand_links():
    r = requests.get('https://www.search.gov.hk/result?ui_lang=zh-hk&proxystylesheet=ogcio_home_adv_frontend&output=xml_no_dtd&ui_charset=utf-8&a_submit=false&query=%22%E5%85%AC%E7%AB%8B%E9%86%AB%E9%99%A2%E6%80%A5%E7%97%87%E5%AE%A4%22&ie=UTF-8&oe=UTF-8&site=gia_home&tpl_id=stdsearch&gp=0&gp0=gia_home&gp1=&p_size=30&num=30&doc_type=html&as_filetype=&as_q=&as_epq=%E5%85%AC%E7%AB%8B%E9%86%AB%E9%99%A2%E6%80%A5%E7%97%87%E5%AE%A4&is_epq=&as_oq=&is_oq=&as_eq=&is_eq=&r_lang=&lr=&web=this&sw=1&txtonly=0&rwd=0&date_v=within&date_last=%2330&s_date_year=2020&s_date_month=01&s_date_day=01&e_date_year=2020&e_date_month=12&e_date_day=31&last_mod=%2330&sort=date%3AS%3AS%3Ad1')
    soup = BeautifulSoup(r.text)
    items = soup.find_all('div', {'class': 'item'})
    output = []
    #"https://www.info.gov.hk/gia/general/202001/15/P2020011400654.htm" is not
    for item in items:
        link = item.find('a', {'class': 'itemDetailsTitle'})
        if link is None:
            continue
        item_href = link['href']
        item_r = requests.get(item_href)
        item_soup = BeautifulSoup(item_r.content)
        title = item_soup.title.text.strip()
        if title != '公立醫院急症室服務及住院病床使用率':
            continue
        item_attachment_link = item_soup.find('a', {'class': 'attach_text'})
        if item_attachment_link is None:
            continue
        item_attachment_link = item_attachment_link['href']
        #print(title, item_href, item_attachment_link)
        output.append({'title': title, 'url': item_href, 'attachment_url': item_attachment_link})
    return output



def process_service_demand_rows(rows, date_formatted):
    output = []
    for row in rows:
        hospital = None
        ae_total = 0
        im_total = 0
        midnight_pct = 0.0
        paediatrics_midnight_pct = 0.0
        
        row = [cell  if type(cell) != str else cell.strip().replace('\r', ' ') for cell in row]
        row[0] = cleanse_hospital_name(row[0])
        row[1] = cleanse_hospital_name(row[1])
        #print(row)
        
        is_last_row_nan = type(row[-1]) is float and math.isnan(row[-1])
        if is_last_row_nan:
            hospital = row[0].split(' ')[-1]
            ae_total = clease_str_nan_to_int(row[1])
            im_total = clease_str_nan_to_int(row[2])
            midnight_pct = clease_str_nan_to_int(row[3]) / 100.0
            paediatrics_midnight_pct = clease_str_nan_to_int(row[4]) / 100.0
        
        else:
            hospital = row[1].split(' ')[-1]
            ae_total = clease_str_nan_to_int(row[2])
            im_total = clease_str_nan_to_int(row[3])
            midnight_pct = clease_str_nan_to_int(row[4]) / 100.0
            paediatrics_midnight_pct = clease_str_nan_to_int(row[5]) / 100.0
            
        if hospital is not  None:
            output.append({
                'date': date_formatted,
                'hospital': hospital,
                'ae_total': ae_total,
                'im_total': im_total,
                'midnight_pct': midnight_pct,
                'paediatrics_midnight_pct': paediatrics_midnight_pct
            })
    return output


def process_service_demand_pdf_from_url(url):
    pdf_file = requests.get(url)
    raw = extract_text(BytesIO(pdf_file.content))
    for line in raw.split('\n'):
        if 'Highlights' in line:
            #print(line)
            result = re.match('.* on (.*) are .*', line)
            date_str = result.group(1)
            date_parsed = datetime.datetime.strptime(date_str, '%d %b %Y').date()
            date_formatted = date_parsed.strftime("%Y-%m-%d")
    rows = read_pdf(BytesIO(pdf_file.content), lattice=True)[0].values.tolist()
    output = process_service_demand_rows(rows, date_formatted)
    #output_df.to_csv('%s.csv' % date_formatted, index=False)
    return output

def get_daily_service_demands():
    links = get_last_30_days_service_demand_links()
    all_output = []
    for link in [l['attachment_url'] for l in links]:
        print(link)
        all_output = all_output + process_service_demand_pdf_from_url(link)
        sleep(1)
    return all_output


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
    ch_df = read_pdf(BytesIO(pdf_chi_file.content), lattice=True)[0].rename(columns=chi_mapping)
    combined_df = en_df.copy()
    combined_df['hospital_zh'] = ch_df['hospital_zh']
    combined_df['status_zh'] = ch_df['status_zh']
    combined_df['confirmation_date'] = combined_df['confirmation_date'].apply(lambda x: '{2}-{1}-{0}'.format(*x.split('/')))
    combined_df = combined_df[['case_no','confirmation_date','gender_en','age','hospital_en','status_en','hospital_zh','status_zh']]
    return combined_df.to_dict(orient='record')
    

def get_daily_stats():
    r = requests.get('https://www.chp.gov.hk/files/pdf/enhanced_sur_pneumonia_wuhan_eng.pdf')
    raw = extract_text(BytesIO(r.content))
    last_updated = None
    confirmed_case = None
    fulfilling = None
    ruled_out = None
    still_investigated = None
    for line in raw.split('\n'):
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
    #print(query)
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
    #print(query)
    return run_query(query)


def upsert_service_demands(demands):
    template = """
      {{
        date:"{date}",
        hospital:"{hospital}",
        ae_total: {ae_total},
        im_total: {im_total},
        midnight_pct: {midnight_pct},
        paediatrics_midnight_pct: {paediatrics_midnight_pct}
      }}
    """

    objects = "\n".join([template.format(**demand) for demand in demands])
    objects = "[%s]" % objects
    query = """
      mutation MyQuery {
        insert_wars_ServiceDemand(
          objects: %s,
          on_conflict: {constraint: ServiceDemand_pkey,update_columns: [ae_total, im_total, midnight_pct,paediatrics_midnight_pct]}
        ){
          affected_rows
        }
      }
    """ % (objects)
    #print(query)
    return run_query(query)




   
daily_stats = get_daily_stats()
#print(daily_stats)
print(upsert_daily_stats(daily_stats))

cases = get_cases()
#print(cases)
print(upsert_cases(cases))

demands = get_daily_service_demands()
#print(demands)
print(upsert_service_demands(demands))
