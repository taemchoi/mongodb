# index는 검색을 더 빠르게 하기 위해 만든 데이터 structure(index가 없으면 collection있는 데이터를 하나하나 조회해서 속도가 느림)
# default index = '_id'
# index key : {'필드명', direction} 구조, direction : 1(default, ascending), -1(descending), text('text' 표현)
import pymongo
import requests
import re
from bs4 import BeautifulSoup as bs
import datetime
connection = pymongo.MongoClient("localhost", 27017)
movie_db = connection.movie
movie_c = movie_db.c
actor = movie_c

# 싱글 인덱스 생성
actor.create_index([('영화명','text')])
# 여러개 싱글 인덱스 생성
actor.create_index([('배우명',1)])

actor.index_information()

# 띄어쓰기 단위로 text일 경우 검색어가 포함되어 있으면 검색가능
docs=actor.find({'$text':{'$search':'후보'}}).limit(2)
for i in docs:
    print(i)
    
# compound 필드 인덱스(최대 31개)
actor.create_index([('출연영화',pymongo.TEXT),('배우명','text')])

docs=actor.find({'$text':{'$search':'황정민'}}).limit(2)
for i in docs:
    print(i)
    
# search 와 같은결과로 regex 활용 가능    
docs=actor.find({'영화명':{'$regex':'인'}}).limit(2)
for i in docs:
    print(i)
    
    
text_col=movie_db.text_col
text_col.insert_many(
    [
        {"name":"choi",'food':'cake and choco', 'f_number':1},
        {"name":"tae",'food':'coffee and sugar', 'f_number':3},
        {"name":"min",'food':'soup and steak', 'f_number':5},
        {"name":"lee",'food':'cake and coffee', 'f_number':7},
        {"name":"bom",'food':'bob and soup', 'f_number':9}
    ]
)
# e가 0번이거나 1개 이상인 모든 값 찾기
docs = text_col.find({'name':{"$regex":'e.*'}})
for i in docs:
    print(i)
    
text_col.drop_indexes()
text_col.create_index([('name','text'),('food','text')])    

# coffe and sugar가 있는 것을 찾고 싶은데 띄어쓰기 단위로 개별 검색하여 원하는 결과가 나오지 않음
# 즉, sugar가 있는 것, coffee가 있는 것, and는 무시함
docs = text_col.find({'$text':{"$search":'coffee and sugar'}})
for i in docs:
    print(i)
    
print("-"*40)    
# \"를 붙여줌으로써 구분가능하다. +알파로 $caseSensitive를 True 인자로 주면 대소문자 구분이 가능해진다.
docs = text_col.find({'$text':{"$search":'\"coffee and sugar\"', '$caseSensitive':True}})
for i in docs:
    print(i)
    
actor.index_information()
actor.drop_indexes()

# 중앙대나온 배우 흥행지수순으로 10명, 아래 두개 find 모두 같은 결과
actor.create_index([('학교','text')])
docs = actor.find({'$text':{'$search':'중앙대학교'}},{'배우명':1,'_id':0,"학교":1}).sort('흥행지수',pymongo.DESCENDING).limit(10)
docs = actor.find({'학교':{'$regex':'중앙'}},{'배우명':1,'_id':0,"학교":1}).sort('흥행지수',pymongo.DESCENDING).limit(10)

for i in docs:
    print(i)