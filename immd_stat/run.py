import requests
from datetime import date, datetime, timedelta
from bs4 import BeautifulSoup
import os
import json
from time import sleep


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


def upsert_records(stat):
    template = """
      {{
        date: "{date}",
        location: "{location}",
        arrival_hong_kong: {arrival_hong_kong},
        arrival_mainland: {arrival_mainland},
        arrival_other: {arrival_other},
        arrival_total: {arrival_total},
        departure_hong_kong: {departure_hong_kong},
        departure_mainland: {departure_mainland},
        departure_other: {departure_other},
        departure_total: {departure_total},
      }}
    """
    records = ",\n".join([template.format(**r) for r in stat])
    records = "[%s]" % records
    query = """
      mutation MyQuery {
        insert_wars_immd(
          objects: %s,
          on_conflict: {constraint: immd_pkey,update_columns: []}
        ){
          affected_rows
        }
      }
    """ % (records)
    return run_query(query)   


def fetch_data_by_date(d):
    url = "https://www.immd.gov.hk/eng/stat_%d%.2d%.2d.html" % (d.year, d.month, d.day)
    r = requests.get(url)
    output = []
    if r.status_code == requests.codes.ok:
        soup = BeautifulSoup(r.content, features="html.parser")
        table = soup.find("table", {'title': 'Statistics on Passenger Traffic'})
        for row in table.find_all("tr")[5:]:
            cells = [c.text for c in row.find_all('td')]
            location = cells[0].strip()
            numbers = [int(c.strip().replace(',', '')) for c in cells[1:]]
            record = {
              'location': location,        
              'arrival_hong_kong': numbers[0],
              'arrival_mainland': numbers[1],
              'arrival_other': numbers[2],
              'arrival_total': numbers[3],
              'departure_hong_kong': numbers[4],
              'departure_mainland': numbers[4],
              'departure_other': numbers[6],
              'departure_total': numbers[7],
              'date': '%d-%.2d-%.2d' % (d.year, d.month, d.day)
            }
            output.append(record)
            print(record)
    return output



end_date = datetime.today().date()
for i in range(0, 7):
    current_date = end_date - timedelta(i)
    output = fetch_data_by_date(current_date)
    print(current_date, len(output))
    upsert_records(output)
    sleep(1.0)
