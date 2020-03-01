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
    for row in reader:
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
       
    scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/spreadsheets"]
    base64_credentials = os.getenv('CRED', '')
    decoded_credentials = base64.b64decode(base64_credentials)
    info = json.loads(decoded_credentials)
    credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    #credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
    service = discovery.build('sheets', 'v4', credentials=credentials)
 
    #Kowloon City


    #Full List
    spreadsheet_id = '1gG0NBzWE2YE0C7ZDt7kdJ1EmSj2upTDSxRHlmsplbmU'
    sheet_name = "master_automated"
    range_name = "%s!A2:F%d" % (sheet_name, len(output_rows) + 1)
    values = output_rows
    data = {
        'values' : values 
    }
    service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range='%s!A2:F'% (sheet_name)).execute()
    service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, body=data, range=range_name, valueInputOption='USER_ENTERED').execute()
    
    #District Level
    for spreadsheet_id, district in [("1AKYtYvNJldF4TTwv2lokfV-96YGZ7KOHr76Qss8ntSI", "Kowloon City"), ("1hLku-fHBFRN_pGfn7W4qT-bEM5s0XUTn372SAqjIDsg", "Sham Shui Po")]:
        output_rows = rows_by_district[district]
        district_zh = output_rows[0][1]
        if district_zh == "九龍城":
            district_zh = "九龍"
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

     

except OSError as e:
    print(e)
