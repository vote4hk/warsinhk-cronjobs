import requests
from bs4 import BeautifulSoup
from pyjsparser import parse
import json
import hashlib
import os
import urllib
import multiprocessing
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

def send_to_telegram_channel(item):
    text = "%s\n%s" % (item["title"], item["link"])
    qs = urllib.parse.urlencode({'chat_id': os.getenv("TELEGRAM_CHANNEL_ID"), 'text':text})
    url = "https://api.telegram.org/bot%s/sendMessage?%s" % (os.getenv("TELEGRAM_TOKEN"), qs)   
    print(url)
    print(requests.post(url).json())


def check_existence(url):
    key = hashlib.md5(url.encode('utf-8')).hexdigest()
    query = """
    query MyQuery {
      __typename
      wars_News(where: {key: {_eq: "%s"}}, limit: 1) {
        key
      }
    }

    """ % key
    result = run_query(query)
    return len(result["data"]["wars_News"]) > 0


def upsert_news(news):
    template = """
      {{
        source: "{source}",
        date: "{date}",
        image: "{image}",
        link: "{link}",
        text: "{text}",
        key: "{key}",
        title: "{title}"
      }}
    """
    news_objects = ",\n".join([template.format(**article) for article in news])
    news_objects = "[%s]" % news_objects
    query = """
      mutation MyQuery {
        insert_wars_News(
          objects: %s,
          on_conflict: {constraint: News_pkey,update_columns: []}
        ){
          affected_rows
          returning {
            source
            link
            text
            date
            image
            title
          }
        }
      }
    """ % (news_objects)
    return run_query(query)   


def related(text, title):
    keywords = ["武漢", "湖北", "肺炎", "衞生", "疫情", "確診個案", "口罩"]
    for keyword in keywords:
       if keyword in text or keyword in title:
           return True
    return False

def retrieve_url(orig_url):
    r = requests.get(orig_url)
    sleep(0.1)
    return r.url


def get_news_articles():
    url = "https://hk.news.appledaily.com/realtime/realtimelist/all?page=local"
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    r = requests.get(url, headers=headers)
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.content, features="html.parser")
    elements = soup.find_all("div", {"class": "text"})
    links = []
    for element in elements:
        a = element.find('a')
        href = a['href']
        links.append(href)
    pool = multiprocessing.Pool(5)
    result = pool.map_async(retrieve_url, links).get()
    result = [l for l in result if "/local/" in l or "/international/" in l or "/china/" in l or "/breaking/" in l]
    return result


def get_article(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    r = requests.get(url, headers=headers)
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.content, features="html.parser")
    scripts = soup.find_all('script')
    contents = []
    for script in scripts:
        t = script.text

        if "content_elements" in t:
            d = parse(t)["body"]
            for x in d:
                d_type = x.get("type", None)
                if d_type != "ExpressionStatement":
                    continue
                expression = x["expression"]
                left = expression["left"]
                left_object = expression["left"].get("object", None)
                if left_object is None:
                    continue
                left_object_name = left_object.get("name", None)
                if left_object_name != "Fusion":
                    continue
                left_prop = left.get("property", None)
                if left_prop is None:
                    continue
                prop_name = left_prop.get('name', None)
                if prop_name != "globalContent":
                    continue
                right = expression["right"]
                right_props = right["properties"]
                for s in right_props:
                    if s["key"]["value"] == "content_elements":
                        v = s["value"]
                        for e in v["elements"]:
                            for p in e["properties"]:
                               if p["key"]["value"] == "content": 
                                   contents.append(p["value"]["value"])
    item = {}
    item["text_orig"] = "<br/>".join(contents)
    item["text"] = json.dumps(item["text_orig"])[1:-2]
    item["source"] = "appledaily"
    item["title"] = soup.find("meta",  property="og:title")["content"]
    item["image"] = soup.find("meta",  property="og:image")["content"]
    item["link"] = soup.find("meta",  property="og:url")["content"]
    item["key"] = hashlib.md5(item["link"].encode()).hexdigest()
    item["date"] = (lambda x:"%s-%s-%s"% (x[0:4], x[4:6],x[6:8]))(item["link"].split('/')[-3])
    return item


print("Fetching Links")
links = get_news_articles()
print("%d links available" % len(links))
#items = [get_article(link) for link in links[0:100]]
print(links)
k = 0
for link in links:
    if k > 30:
        print("send enough")
        break
    is_existed = check_existence(link)
    if is_existed:
        print("%s already exists" % (link))
        continue
    item = get_article(link)
    result = upsert_news([item])
    if not related(item["text_orig"], item["title"]):
        print("%s not related" % (link))
        continue
    new_rows = result["data"]["insert_wars_News"]["returning"]
    if len(new_rows) > 0:
        send_to_telegram_channel(item)
        k = k + 1
    sleep(0.5)
    
