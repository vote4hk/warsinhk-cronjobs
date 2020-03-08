import httplib2
import os
import requests
from apiclient import discovery
from google.oauth2 import service_account
import csv
from io import StringIO
from datetime import datetime
import base64
import json
import base64
import urllib.parse
from time import sleep


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]



def get_gps_by_location(district, address):
    address_encoded = urllib.parse.quote_plus(address)
    district_encoded = urllib.parse.quote_plus(district)
    location = None
    key = os.getenv("MAP_API_KEY", "")
    for i in range(0, 3):
        url = "https://maps.googleapis.com/maps/api/geocode/json?address=" + district_encoded + "," + address_encoded + "&key=" + key
        r = requests.get(url)
        j = r.json()
        if j.get("status", "") == "REQUEST_DENIED":
            sleep(0.5)
            continue
        print(url)
        if "results" in j:
            results = j["results"]
            if len(results) == 0:
                break
            geom = results[0].get("geometry", None)
            if geom is not None:
                loc = geom["location"]
                location = (loc["lat"], loc["lng"])
                break
    return (address, location)



try:
    r = requests.get("http://www.chp.gov.hk/files/misc/home_confinees_tier2_building_list.csv")
    r.encoding = "utf-8"
    f = StringIO(r.text)
    reader = csv.reader(f, delimiter=",")
    row = next(reader)
    output_rows = []
    rows_by_district = {}
    c = 0
    for row in reader:
        c += 1
        num = int(row[0])
        district_zh = row[1].split(' ')[0].strip()
        district_en = ' '.join(row[1].split(' ')[1:]).strip()
        addresses = row[2].split('\n')
        address_zh = addresses[0].strip()
        address_en = addresses[-1].strip()
        address_en = address_en if len(address_en) > 0 else address_zh
        formatted_date = "{2}-{1}-{0}".format(*row[3].split("/"))
        cleansed_row = [num, district_zh, district_en, address_zh, address_en, formatted_date]
        output_rows.append(cleansed_row)
        if district_en not in rows_by_district:
            rows_by_district[district_en] = []
        rows_by_district[district_en].append(cleansed_row)
        #print(num, district_zh, district_en, address_zh, address_en, formatted_date)
    print("Total %d" % c)
    scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/spreadsheets"]
    base64_credentials = os.getenv('CRED', '')
    decoded_credentials = base64.b64decode(base64_credentials)
    info = json.loads(decoded_credentials)
    credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    #credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
    service = discovery.build('sheets', 'v4', credentials=credentials)

    all_district_rows = []

    #District Level
    for spreadsheet_id, district in [
            ("1JbeMGc4hlbv98axD73xJ7ibtsDtxV5cXq7anrL4cqKY","Islands"),
            ("1AKYtYvNJldF4TTwv2lokfV-96YGZ7KOHr76Qss8ntSI", "Kowloon City"),
            ("1hLku-fHBFRN_pGfn7W4qT-bEM5s0XUTn372SAqjIDsg", "Sham Shui Po"),
            ("1TnqjMld2CGTzhPPC0gdXN1nNYEHX0-bhUoQJf0BTEx8", "Yuen Long"),
            ("1QWbIhkPoewfEDzyHhaK6gyhySzYoT61N1TOhCzz_P9s", "North"),
            ("1kb0khP5_704XzHjz5dzOCiyxJs0BRmGrmVyoOfUfAeY", "Wan Chai"),
            ("1e8Z8M_YWsu45Z-3yZy4fH2zfgR2y7Vh4u9n6ChYb2q0", "Tai Po"),
            ("1AnEUvlhf4Wq8Z4X2dqqgPkpfW1PhJt207OTu32zuazI", "Southern"),
            ("1N7-wuoJnxnJp5u7U16CHnwIuX0jAXPP49vCFu3ZbDso", "Tuen Mun"),
            ("15SCiiiBbeHwi4h_6lau2_b1cMPM6IZvZz4tzNBFbmY8", "Wong Tai Sin"),
            ("1GxnHikzhWaADtEbHDZF9dzKgMhWVSOT3PO9rmzYP57U", "Kwun Tong"),
            ("1ay1MuzEgMCCLmUwW0t4NrCTSc5cOs6myV618Z_oitcE", "Shatin"),
            ("1buEmKKkPVVNvqeUOMRekkYKN7ZD3_-_08llP8FHumZs", "Kwai Tsing"),
            ("1QN0jqPryPG0udd2ITRgWL0MvRHtUDrqBerMVsxuLtPc","Yau Tsim Mong"),
            ("1Q4EdB4qkXZFJSnuWOWtcpcR3f6ZtcSd3w77mzD693gs","Eastern"),
            ("1UHFKshH81D2JYbZOhfAYUdZQ4dtM_WC22CAjOYmfpH8","Tsuen Wan"),
            ("1sF5UrHoY5QP2uEIcUgOqiI1iteLKV1-9yjYS46eBVjE","Sai Kung"),
            ("1NsODdZ9KjbBqVOLT27ib8ZOwYRlq9YTIbb2_rdCSzac", "Central & Western")
        ]:
        output_rows = rows_by_district[district]
        district_zh = output_rows[0][1]
        if district_zh == "九龍城":
            district_zh = "九龍"
        if district_zh == "南區":
            district_zh = ""
        print("Updating %s with %d records" % (district, len(output_rows)))
        sheet_name = "%s Only" % district
        gps_sheet_name = "%s GPS" % district
        range_name = '%s!A2:H%d' % (sheet_name, len(output_rows) + 1)
        lat_field = "=VLOOKUP($D%d,'%s'!$A$2:$C, 2, FALSE)"
        lng_field = "=VLOOKUP($D%d,'%s'!$A$2:$C, 3, FALSE)"
        values = [row + [lat_field % (index, gps_sheet_name), lng_field % (index, gps_sheet_name)] for index, row in enumerate(output_rows, start=2)]
        data = {
            'values' : values
        }
        service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range='%s!A2:H' % (sheet_name)).execute()
        service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, body=data, range=range_name, valueInputOption='USER_ENTERED').execute()

        #Last Updated
        sheet_name = "Last Updated"
        range_name = '%s!A2' % (sheet_name)
        now = datetime.now()
        last_updated = now.strftime("%m/%d/%Y, %H:%M:%S")
        data = {
            'values': [[last_updated]]
        }
        service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, body=data, range=range_name, valueInputOption='USER_ENTERED').execute()

        print("Appending address")
        print("Reading address")
        address_range = "%s GPS!$A2:A" % district
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=address_range).execute()
        existing_address_rows = [r[0] for r in result.get('values', [])]
        all_addresses = [r[3] for r in output_rows]
        new_addresses = []
        for a in all_addresses:
            if a not in existing_address_rows:
                new_addresses.append(a)
        new_addresses = new_addresses
        print(district_zh, len(new_addresses))
        print(new_addresses)
        append_address_rows = []
        for new_addr in new_addresses:
            address, location = get_gps_by_location(district_zh, new_addr)
            if location is None:
                continue
            r = [address, location[0], location[1]]
            append_address_rows.append(r)
        print(append_address_rows)
        service.spreadsheets().values().append(
                  spreadsheetId=spreadsheet_id,
                  range=address_range,
                  body={"values": append_address_rows},valueInputOption="USER_ENTERED").execute()
        #Read Updated Only List
        sheet_name = "%s Only" % district
        range_name = '%s!A2:H' % (sheet_name)
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        existing_district_rows = result.get('values', [])
        print(len(existing_district_rows))
        all_district_rows += existing_district_rows
        sleep(5)
    #Full List
    spreadsheet_id = '1gG0NBzWE2YE0C7ZDt7kdJ1EmSj2upTDSxRHlmsplbmU'
    sheet_name = "master_automated"
    range_name = "%s!A2:H%d" % (sheet_name, len(all_district_rows) + 1)
    service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range='%s!A2:H'% (sheet_name)).execute()
    k = 200
    print("Total %d" % (len(all_district_rows)))
    for i, output in enumerate(chunks(all_district_rows, k)):
        start = 2 + k  * i
        range_name = "%s!A%d:H%d" % (sheet_name, 2 + k* i, start + len(output) - 1)
        values = output
        data = {
            'values' : output
        }
        print(range_name)
        service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, body=data, range=range_name, valueInputOption='USER_ENTERED').execute()
        sleep(0.5)

except OSError as e:
    print(e)
