import pymongo
from pprint import pprint
connection = pymongo.MongoClient("localhost", 27017)
agg_db = connection.aggDB
agg_z=agg_db.zip

def printer(docs):
    for i in docs:
        pprint(i)
        

docs=agg_z.find({}).limit(5)
for i in docs:
    pprint(i)
    
# project 명령어 == select
docs = agg_z.aggregate([
    {'$project':{'_id':1,'city':1}},
    {'$limit':5}
    ])
printer(docs)
# 각명령별로 처리되고 다음 파이프라인에 넘어간다고 생각하면 되는데 
# project 파이프라인이 실행되고 limit 파이프라인이 실행됨을 의미


docs = agg_z.aggregate(
    [
        {'$project':{'state':1,'pop':1,'_id':0}},
        {"$limit":3}
    ]
)
printer(docs)

# aggregation의 주요 명령
# GROUP = GROUP BY
""" select count(*) as count from agg_z"""
docs = agg_z.aggregate([
    {'$group' :
        {
            '_id' : 'null',
            'count' : {'$sum' : 1}
        }
        
    }
])# _id를 null로 하게 되면 전체 document에 대한 계산을 하게 됨

printer(docs)

"""select sum(pop) as pop from agg_z"""
docs = agg_z.aggregate([
    {'$group':
        {
            '_id' : 'null',
            'pop':{'$sum':'$pop'}
        }
    }
])
printer(docs)

"""Sql : select state, sum(pop) from agg_z group by state having sum_pop>=10000 order by _id limit 5"""
docs = agg_z.aggregate([
    {'$group':
        {
            '_id':'$state',
            'sum_pop':{'$sum':'$pop'}
        }
    },
    {'$match':{'sum_pop':{'$gte':10000}}},
    {'$sort':{'_id':1}},
    {'$limit':5}
])
printer(docs)

# 여러 조건으로 grouping 하기
docs = agg_z.aggregate([
    {'$group':
        {
            '_id':{'state':'$state','city':'$city'},
            'sum_pop':{'$sum':'$pop'}
        }
    },
    {'$match':{'sum_pop':{'$gte':10000}}},
    {'$sort':{'_id.state':1}},
    {'$limit':5}
])
printer(docs)

# multi group에서 match 사용하기
""" sql : select state, city, sum(pop) as sum_pop from agg_z group by state, city having city = 'NORTH POLE'"""
docs = agg_z.aggregate([
    {'$group':
        {
            '_id':{'state':'$state','city':'$city'},
            'sum_pop':{'$sum':'$pop'}
        }
    },
    {'$match':{'_id.city':'NORTH POLE'}}
])
printer(docs)

# 주별 도시 인구 평균 구하기
docs = agg_z.aggregate([
    {'$group':{'_id':{'state':'$state','city':'$city'},
     'avg_pop':{'$avg':'$pop'}}},
    {'$match':{'_id.state':'CO'}},
    {'$project':{'_id.state':0}},
    {'$limit':5}

])
printer(docs)

# 주와 도시 기준으로 도시별 인구 수 합을 구해서 인구 수 기준으로 정렬한 후, 
# 주를 기준으로 최고 인구와 최저 인구 수를 가진 도시를 구하고 
# 출력은 주, 최대 인구가진 도시(이름, 인구 수) 최소 인구 가진 도시(이름, 인구 수 ) 출력
docs = agg_z.aggregate([
    {'$group':{'_id':{'state':'$state','city':'$city'},
               'pop':{'$sum':'$pop'}}
    },
    {'$sort':{'pop':1}},
    {'$group':{'_id':'$_id.state',
               'biggest_city':{'$last':'$_id.city'},
               'biggest_pop':{'$last':'$pop'},
               'smallest_city':{'$first':'$_id.city'},
               'smallest_pop':{'$first':'$pop'},
               }},
    {'$project':{'_id':0,
                 'state':'$_id',
                 'biggest_city':{'name':'$biggest_city','pop':'$biggest_pop'},
                 'smallest_city':{'name':'$smallest_city','pop':'$smallest_pop'}}},
    {'$limit':5}
])
printer(docs)