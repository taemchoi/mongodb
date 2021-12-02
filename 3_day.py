import pymongo
import requests
import re
from bs4 import BeautifulSoup as bs
import datetime

"""영화 디테일 크롤링 및 insert mongoDB"""

connection = pymongo.MongoClient("localhost", 27017)
movie_db = connection.movie
movie_c = movie_db.c
url = "http://www.cine21.com/rank/person/content"
pages = 20
actor_detail_info = list()
for i in range(pages):
    page = i+1
    data = {'section': 'actor',
            'period_start' : "2020-01",
            'gender' :  'all',
            'page': page
            }

    response = requests.post(url, data=data)
    soup = bs(response.content.decode('utf-8'), 'html.parser')

    actors = soup.select('li.people_li div.name')
    hit = soup.select('ul.num_info li strong')
    movie_list = soup.select('ul.mov_list')
    ranking = soup.select('li.people_li > span.grade')
    for a,j,k,l in zip(actors, hit, movie_list, ranking):
        print(re.sub('\([a-z0-9가-힣A-Z]+\)','',a.text))
        detail_url="http://www.cine21.com" + a.select_one('a').attrs['href']
        response_detail = requests.post(detail_url)
        soup_detail = bs(response_detail.content.decode('utf-8'), 'html.parser')
        actor_data = soup_detail.select('ul.default_info') 

        actor_info_dict = dict()


        actor_info_dict["배우명"] = re.sub('\([0-9가-힣]*\)','',a.text)
        actor_info_dict["흥행지수"] = int(j.text.replace(',',''))
        movies=k.select('li a span')
        actor_info_dict["영화명"] = [i.text for i in movies] 
        actor_info_dict['랭킹'] = int(l.text)
        for item in actor_data[0].select('li'):
            actor_item = re.sub('<span.*?>.*?</span>','',str(item))
            actor_item= re.sub('<.*?>','',actor_item)
            actor_info_dict[item.select_one('span.tit').text] = actor_item.strip()
        actor_detail_info.append(actor_info_dict)
        
movie_c.insert_many(actor_detail_info)