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

def send_to_telegram_channel(item, title_prefix=""):
    text = "%s\n%s" % (title_prefix + item["title"], item["link"])
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
        date
      }
    }

    """ % key
    result = run_query(query)
    data = result["data"]["wars_News"]
    existed = len(data) > 0
    d = None
    if existed:
        d = data[0]["date"]
    return existed, d


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
    keywords = ["武漢", "湖北", "肺炎", "衞生", "疫情", "確診個案", "口罩", "國家衛健委疾控局", "新冠肺炎", "檢疫", "新型冠狀病毒", "新型肺炎"]
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



def fetch_apple_daily():
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
        is_existed, _ = check_existence(link)
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


def get_rthk_news_article():
    url = "https://news.rthk.hk/rthk/ch/latest-news.htm"
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    r = requests.get(url, headers=headers)
    r.encoding = "utf-8"
    only_div = SoupStrainer("div", {"class": "ns2-row-inner"})
    soup = BeautifulSoup(r.content, features="html.parser", parse_only=only_div)
    elements = list(soup.find_all("div", {"class": "ns2-row-inner"}))
    soup.decompose()
    links = []
    for element in elements:
        a = element.find("a")
        if a is not None:
            l = a.get('href', None)
            if l is None:
                continue
            l = l.split('?')[0]
            links.append(l)
    return links


def get_rthk_article(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    r = requests.get(url, headers=headers)
    r.encoding = "utf-8"
    only_div = SoupStrainer("div", {"class": "itemFullText"})
    only_div_date = SoupStrainer("div", {"class": "createddate"})
    only_meta = SoupStrainer("meta")

    date_soup = BeautifulSoup(r.content, features="html.parser", parse_only=only_div_date)
    soup = BeautifulSoup(r.content, features="html.parser", parse_only=only_div)
    meta_soup = BeautifulSoup(r.content, features="html.parser", parse_only=only_meta)
    item = {}
    contents = []
    for div in soup.find_all("div"):
        contents.append(div.text.strip())
    item["text_orig"] = "\n".join(contents)
    item["text"] = json.dumps(item["text_orig"])[1:-1]
    item["source"] = "rthk"
    item["title"] = meta_soup.find("meta",  property="og:title")["content"]
    meta_image =  meta_soup.find("meta",  property="og:image")
    item["image"] = "" if meta_image is None else meta_image.get("content")
    item["link"] = url
    item["key"] = hashlib.md5(url.encode()).hexdigest()
    date_div = date_soup.find("div")
    item["date"] = "" if date_div is None else date_div.text.split(' ')[0]
    return item



def fetch_rthk():
    print("Fetching Links")
    links = get_rthk_news_article()
    print("%d links available" % len(links))
    print(links)
    print_memory()
    k = 0
    for link in links:
        if k > 10:
            print("send enough")
            break
        is_existed, _ = check_existence(link)
        if is_existed:
            print("%s already exists" % (link))
            continue
        print(link)
        sleep(1)
        item = get_rthk_article(link)
        print(item)
        result = upsert_news([item])
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


def get_icable_article(url):
    r = requests.get(url)
    only_div = SoupStrainer("div", {"class": "video_content_area"})
    only_meta = SoupStrainer("meta")
    soup = BeautifulSoup(r.content, features="html.parser", parse_only=only_div)
    meta_soup = BeautifulSoup(r.content, features="html.parser", parse_only=only_meta)
    contents = [d.text for d in soup.find_all("div", {"class": "video_content"})]
    date_div = soup.find("div", {"class": "video_date"})
    item_date = "1900/01/01" if date_div is None else date_div.text.strip().split(" ")[0]
    item_date = "{0}-{1}-{2}".format(*item_date.split("/"))
    item = {}
    item["text_orig"] = "\n".join(contents)
    item["text"] = json.dumps(item["text_orig"])[1:-1]
    item["source"] = "icable"
    item["title"] = meta_soup.find("meta",  property="og:title")["content"]
    item["image"] = meta_soup.find("meta",  property="og:image")["content"]
    item["link"] = meta_soup.find("meta",  property="og:url")["content"]
    item["key"] = hashlib.md5(url.encode()).hexdigest()
    item["date"] = item_date
    return item


def fetch_icable():
    print("Fetching Links")
    all_news = requests.get("http://cablenews.i-cable.com/ci/news/listing/api").json()
    links = ["http://cablenews.i-cable.com/ci/videopage/news/%s" % (news["id"]) for news in all_news]
    print(len(all_news))
    offset = tzoffset(None, 8 * 3600)  # offset in seconds
    cutoff_date = (datetime.now(offset).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(cutoff_date)
    print_memory()
    k = 0
    for link in links:
        if k > 10:
            print("send enough")
            break
        is_existed, d = check_existence(link)
        if is_existed:
            print("%s already exists" % (link))
            print(d, cutoff_date)
            if d < cutoff_date:
                break
            continue
        print(link)
        sleep(1)
        item = get_icable_article(link)
        if item["date"] < cutoff_date:
            break
        result = upsert_news([item])
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


def fetch_nowtv():
    print("Fetching nowtv")
    r = requests.get("https://news.now.com/home/tracker/detail?catCode=123&topicId=1031")
    only_link = SoupStrainer("a")
    only_content = SoupStrainer("div", {"itemprop": "articleBody"})
    only_time = SoupStrainer("time", {"class": "published"})
    soup = BeautifulSoup(r.content, features="html.parser", parse_only=only_link)
    links = soup.find_all("a")
    print_memory()
    k = 0
    for link in links:
        if "clearfix" in link.get("class", ""):
            href = "https://news.now.com" + link["href"]
            img = link.find("img")["src"]
            desc_node = link.find("div", {"class": "newsDecs"})
            if k > 10:
                print("send enough")
                break
            is_existed, d = check_existence(href)
            if is_existed:
                print("%s already exists" % (href))
                continue

            print(link)
            sleep(1)
            print(desc_node)
            title = desc_node.find("div", {"class": "newsTitle"}).text
            r2 = requests.get(href)
            content_soup = BeautifulSoup(r2.content, features="html.parser", parse_only=only_content)
            time_soup = BeautifulSoup(r2.content, features="html.parser", parse_only=only_time)
            published_date = time_soup.find("time")["datetime"].split(" ")[0]
            d = content_soup.find("div")
            contents = d.text.strip()
            item = {}
            item["text_orig"] = contents
            item["text"] = json.dumps(item["text_orig"])[1:-1]
            item["source"] = "nowtv"
            item["title"] = title
            item["image"] = img
            item["link"] = href
            item["key"] = hashlib.md5(href.encode()).hexdigest()
            item["date"] = published_date
            print(item)
            result = upsert_news([item])
            if not related(item["text_orig"], item["title"]):
                print("%s not related" % (link))
                continue
            if "data" in result:
                new_rows = result["data"]["insert_wars_News"]["returning"]
                if len(new_rows) > 0:
                    send_to_telegram_channel(item, "【Now新聞台】")
                    k = k + 1
                else:
                    print("already existed")
            else:
                print(result)
    print("finished")
    print_memory()


    

CMD = os.getenv('CMD')
if CMD == "rthk":
    fetch_rthk()
elif CMD == "icable":
    fetch_icable()
elif CMD == "nowtv":
    fetch_nowtv()
else:
    fetch_apple_daily()
