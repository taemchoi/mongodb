import pymongo
import requests
import re
from bs4 import BeautifulSoup as bs
import datetime
connection = pymongo.MongoClient("localhost", 27017)
movie_db = connection.movie
movie_c = movie_db.c
# 컬럼명 변경 $rename
movie_c.update_many({},{'$rename':{'다른 이름':'다른이름'}})

# .sort()
# pymongo.DESCENDING
docs = movie_c.find({},{'배우명':1,'흥행지수':1,'생년월일':1,'_id':0}).sort('생년월일', pymongo.DESCENDING).limit(3)
for doc in docs:
    print(doc)
##{'배우명': '박소이', '흥행지수': 3108, '생년월일': '2012-03-12'}
##{'배우명': '허율', '흥행지수': 3890, '생년월일': '2009-06-26'}
##{'배우명': '이지원', '흥행지수': 4675, '생년월일': '2006-08-30'}
    
    
#$exists
hobbys=movie_c.find({'특기':{'$exists':True}},{'배우명':1,'특기':1,'생년월일':1,'_id':0}).sort('생년월일').limit(3)
for hobby in hobbys:
    print(hobby)
## {'배우명': '허준호', '생년월일': '1964-03-03', '특기': '야구, 농구'}
## {'배우명': '배종옥', '생년월일': '1964-05-13', '특기': '수상 스키'}
## {'배우명': '한석규', '생년월일': '1964-11-03', '특기': '노래부르기'}



# 흥행지수가 10000 이상인 데이터를 가지고 출연영화가 뭐있는지?
docs=movie_c.find({'흥행지수':{'$gte':10000}},{'_id':0,'배우명':1,'흥행지수':1,'영화명':1})
for doc in docs:
    print(doc)
 
# 흥행지수가 3000 이상이고 신세계에 나온 배우는 누가 있는가?
docs = movie_c.find({'$and':[{'흥행지수':{'$gte':3000}},{'영화명':'신세계'}]},{'_id':0,'배우명':1})
for doc in docs:
    print(doc)
# 흥행지수가 30194, 21857 신세계에 나온 배우는 누가 있는가?
docs = movie_c.find({'$nor':[{'흥행지수':{'$in':[30194,21857]}},{'영화명':'신세계'}]},{'_id':0,'배우명':1, '흥행지수':1})
for doc in docs:
    print(doc)
    
# skip : n개만큼 건너뜀,
# limit : n개만 보여줌
docs=movie_c.find({'흥행지수':{'$gte':10000}}).skip(3).limit(3)
for i in docs:
    print(i)
    
# find list 검색방법
# 리스트 안에 신세계 혹은 다만 악에서 구하소서가 있는 것을 가져와라
docs = movie_c.find({'$or':[{'영화명':'신세계'},{'영화명':'다만 악에서 구하소서'}]}) 
# 리스트 안에 신세계와 다만 악에서 구하소서가 모두 들어있는 것을 가져와라
docs = movie_c.find({'영화명':{'$all':['신세계','다만 악에서 구하소서']}}) 
# 리스트 0번째 요소가 신세계인 것을 가져와라
docs = movie_c.find({'영화명.0':'신세계'}) 
 # 리스트 길이가 1인 것 가져와라
docs = movie_c.find({'영화명':{'$size':1}}).limit(1)

elemmatch_sample=movie_db.sample
elemmatch_sample.insert_many([{'results':[82,85,88]},{'results':[75,88,91]}])
# 리스트안에 하나의 값이 두가지조건을 동시에 만족하지 않고 조건별로 하나라도 값이 매칭되면 찾아줌 
docs=elemmatch_sample.find({'results':{'$gte':90, '$lt':76}})
for i in docs:
    print(i)

# 리스트안에 하나의 값이 모든 조건을 만족하면 찾아줌
docs = elemmatch_sample.find({'results':{'$elemMatch':{'$gte':90, '$lt':76}}})
for i in docs:
    print(i)
    
# 직업이 가수인 배우 중, 흥행지수가 가장 높은 배우 열명 출력
docs=movie_c.find({'직업':{'$in':['가수']}}).sort('흥행지수', pymongo.DESCENDING).limit(10)
# 국가보도의 날 출연 배우 흥행지수 높은 순으로 열명 출력
docs=movie_c.find({'영화명':'국가부도의 날'},{'배우명':1,'_id':0,'흥행지수':1}).sort('흥행지수',pymongo.DESCENDING).limit(10)
for i in docs:
    print(i)