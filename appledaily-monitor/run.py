import requests
from bs4 import BeautifulSoup,  SoupStrainer
import json
import hashlib
import os
import urllib
import multiprocessing
from time import sleep
import sys



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

def send_to_telegram_channel(item):
    text = "%s\n%s" % (item["title"], item["link"])
    qs = urllib.parse.urlencode({'chat_id': os.getenv("TELEGRAM_CHANNEL_ID"), 'text':text})
    url = "https://api.telegram.org/bot%s/sendMessage?%s" % (os.getenv("TELEGRAM_TOKEN"), qs)   
    print(url)
    print(requests.post(url).json())


def check_existence(url):
    key = hashlib.md5(url.encode()).hexdigest()
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
    only_div = SoupStrainer("div", {"class": "text"})
    soup = BeautifulSoup(r.content, features="html.parser", parse_only=only_div)
    elements = list(soup.find_all("div", {"class": "text"}))
    soup.decompose()
    links = []
    for element in elements:
        a = element.find('a')
        href = a['href']
        links.append(href)
    links = links
    pool = multiprocessing.Pool(2,maxtasksperchild=1)
    result = pool.map_async(retrieve_url, links).get()
    pool.close()
    pool.join()
    result = [l for l in result if "/local/" in l or "/international/" in l or "/china/" in l or "/breaking/" in l]
    return result


def get_article(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    r = requests.get(url, headers=headers)
    r.encoding = "utf-8"
    only_div = SoupStrainer("div", {"id": "articleBody"})
    only_meta = SoupStrainer("meta")
    soup = BeautifulSoup(r.content, features="html.parser", parse_only=only_div)
    meta_soup = BeautifulSoup(r.content, features="html.parser", parse_only=only_meta)
    item = {}
    contents = []
    for div in soup.find("div"):
        contents.append(div.text)
    item["text_orig"] = "\n".join(contents)
    item["text"] = json.dumps(item["text_orig"])[1:-1]
    item["source"] = "appledaily"
    item["title"] = meta_soup.find("meta",  property="og:title")["content"]
    item["image"] = meta_soup.find("meta",  property="og:image")["content"]
    item["link"] = meta_soup.find("meta",  property="og:url")["content"]
    item["key"] = hashlib.md5(url.encode()).hexdigest()
    item["date"] = (lambda x:"%s-%s-%s"% (x[0:4], x[4:6],x[6:8]))(item["link"].split('/')[-3])
    return item


print("Fetching Links")
links = get_news_articles()
print("%d links available" % len(links))
print(links)
print_memory()
k = 0
for link in links:
    if k > 10:
        print("send enough")
        break
    is_existed = check_existence(link)
    if is_existed:
        print("%s already exists" % (link))
        continue
    print(link)
    sleep(1)
    item = get_article(link)
    print_memory()
    result = upsert_news([item])
    print(result)
    if not related(item["text_orig"], item["title"]):
        print("%s not related" % (link))
        continue
    if "data" in result:
        new_rows = result["data"]["insert_wars_News"]["returning"]
        if len(new_rows) > 0:
            send_to_telegram_channel(item)
            k = k + 1
        else:
            print("already existed")
    else:
        print(result)

print("finished")
print_memory()
