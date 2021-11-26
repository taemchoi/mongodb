#크롤링 예제

import pymongo
import requests
import re
from bs4 import BeautifulSoup as bs
import datetime


connection = pymongo.MongoClient("localhost", 27017)
movie_db = connection.movie
movie_c = movie_db.c


url = "http://www.cine21.com/rank/person/content"
data = {'section': 'actor',
        'period_start' : "2020-01",
        'gender' :  'all',
        'page': 1
        }


response = requests.post(url, data=data)
soup = bs(response.content.decode('utf-8'), 'html.parser')


actors = soup.select('li.people_li div.name')
for a in actors:
    print(re.sub('\([a-z0-9가-힣A-Z]+\)','',a.text))
    detail_url="http://www.cine21.com" + a.select_one('a').attrs['href']
    response_detail = requests.post(detail_url)
    soup_detail = bs(response_detail.content.decode('utf-8'), 'html.parser')
    actor_data = soup_detail.select('ul.default_info') 
    for item in actor_data[0].select('li'):
        print(item.select_one('span.tit').text)
        actor_item = re.sub('<span.*?>.*?</span>','',str(item))
        actor_item= re.sub('<.*?>','',actor_item)
        print(actor_item)