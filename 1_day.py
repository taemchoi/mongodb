# pymongo import
import pymongo


# pymongo connect
conn = pymongo.MongoClient("localhost", 27017)

# DB 생성
db = conn.test_db

# db 정보확인
print(db)

# collection 생성
db_t = db.test

# insert data
# 리스트형태도 삽입가능
# db_t.insert_one({"name":"choi", "age":26, "hobby" : ["basketball", "slamdunk"]})
# post = [{"name":"taem", "age":29, "hobby" : ["basketball", "slamdunk"]},
#          {"name":"lee", "age":30, "hobby" : ["shopping", "sleep"]}]
# db_t.insert_many(post)
# collection 정보 확인
print(db_t)

# dictionary append
# db_t.insert_one({"name":"lee", "age":"30", "favorite":{"person":"choi","animal":"mong","food":"spicy soup"}})

# db 내 도큐먼트 갯수 확인
print(db_t.estimated_document_count())
##5


# collection 내 document 확인
print(db_t.find_one())
## {'_id': ObjectId('619f2fddc1624c07ce4c003f'), 'name': 'choi', 'age': 26, 'hobby': ['basketball', 'slamdunk']}

print(db_t.find_one({"name":"lee"}))
## {'_id': ObjectId('619f301448605d3c796a5463'), 'name': 'lee', 'age': 30, 'hobby': ['shopping', 'sleep']}

docs = db_t.find()

for i in docs:
    print(i)
## {'_id': ObjectId('619f2fddc1624c07ce4c003f'), 'name': 'choi', 'age': 26, 'hobby': ['basketball', 'slamdunk']}
## {'_id': ObjectId('619f301448605d3c796a5461'), 'name': 'choi', 'age': 26, 'hobby': ['basketball', 'slamdunk']}
## {'_id': ObjectId('619f301448605d3c796a5462'), 'name': 'taem', 'age': 29, 'hobby': ['basketball', 'slamdunk']}
## {'_id': ObjectId('619f301448605d3c796a5463'), 'name': 'lee', 'age': 30, 'hobby': ['shopping', 'sleep']}
## {'_id': ObjectId('619f3247ccd5e156b02fef75'), 'name': 'lee', 'age': '30', 'favorite': {'person': 'choi', 'animal': 'mong', 'food': 'spicy soup'}}

docs = db_t.find({"name":"choi"})

for i in docs:
    print(i)
## {'_id': ObjectId('619f2fddc1624c07ce4c003f'), 'name': 'choi', 'age': 26, 'hobby': ['basketball', 'slamdunk']}
## {'_id': ObjectId('619f301448605d3c796a5461'), 'name': 'choi', 'age': 26, 'hobby': ['basketball', 'slamdunk']}


# 조건부 갯수 확인
print(db_t.count_documents({"name": "choi"}))
## 2


#정렬하여 프린트
for post in db_t.find().sort("age"):
    print(post)   
## {'_id': ObjectId('619f2fddc1624c07ce4c003f'), 'name': 'choi', 'age': 26, 'hobby': ['basketball', 'slamdunk']}
## {'_id': ObjectId('619f301448605d3c796a5461'), 'name': 'choi', 'age': 26, 'hobby': ['basketball', 'slamdunk']}
## {'_id': ObjectId('619f301448605d3c796a5462'), 'name': 'taem', 'age': 29, 'hobby': ['basketball', 'slamdunk']}
## {'_id': ObjectId('619f301448605d3c796a5463'), 'name': 'lee', 'age': 30, 'hobby': ['shopping', 'sleep']}
## {'_id': ObjectId('619f3247ccd5e156b02fef75'), 'name': 'lee', 'age': '30', 'favorite': {'person': 'choi', 'animal': 'mong', 'food': 'spicy soup'}}

print("-"*20)
docs=db_t.find({"age":{"$lt":30}})

for i in docs:
    print(i)
## {'_id': ObjectId('619f2fddc1624c07ce4c003f'), 'name': 'choi', 'age': 26, 'hobby': ['basketball', 'slamdunk']}
## {'_id': ObjectId('619f301448605d3c796a5461'), 'name': 'choi', 'age': 26, 'hobby': ['basketball', 'slamdunk']}
## {'_id': ObjectId('619f301448605d3c796a5462'), 'name': 'taem', 'age': 29, 'hobby': ['basketball', 'slamdunk']}

 
 
# update
db_t.update_one({"name":"taem"},{"$set":{"name":"choi"}})
db_t.update_many( {"age": {"$lt":30}}, {"$set": { "name":"taemchoi"}})

# delete
db_t.delete_one( {"age":26})
db_t.delete_many({"$or": [{"name":"taemchoi"},{"name":"lee"}]})
