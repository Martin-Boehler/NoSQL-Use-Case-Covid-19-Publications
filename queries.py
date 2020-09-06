import pymongo
import redis

# NOSQL DATABASE CONNECTIONS:
host = "localhost"

# MONGO CONNECTION:
port = 27017
username = "mongo"
password = "password"
database = "Information-Systems"
collection = "Covid-19"
mongo_connection = pymongo.MongoClient("mongodb://{2}:{3}@{0}:{1}".format(host, port, username, password))
mongo_database = mongo_connection.get_database(database)
mongo_collection = mongo_database.get_collection(collection)

# MONGO QUERIES:
print(len(mongo_collection.distinct("author")))
print(len(mongo_collection.distinct("title")))
print(len(mongo_collection.distinct("journal")))
print(len(mongo_collection.distinct("source")))

print(mongo_collection.distinct("author"))
print(mongo_collection.distinct("journal"))
print(mongo_collection.distinct("source"))

mongo_query = [
    {"$group": {
        "_id": "$author",
        "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
]
print(list(mongo_collection.aggregate(mongo_query)))

# REDIS CONNECTION:
port = 6379
database = 0
redis_connection = redis.StrictRedis(host, port, database, charset="utf-8", decode_responses=True)

# REDIS QUERIES:
redis_query = redis_connection.hgetall("protein")
for key, value in redis_query.items():
    print(key, "\n ===> ", value, "\n")

# NEO4J:
""""
MATCH (a:Author) RETURN count(a)
MATCH (t:Title) RETURN count(t)
MATCH (j:Journal) RETURN count(j)
MATCH (s:Source) RETURN count(s)
MATCH ()-[r:HAS_PUBLISHED]->() RETURN count(r)
MATCH ()-[r:WORKED_TOGETHER]->() RETURN count(r)

MATCH (a:Author)-[r:WORKED_TOGETHER]->(b:Author)
WITH a, count(b) AS clustersize
WHERE clustersize >= 30
RETURN a

MATCH (s:Source)-[r:CONTAIN]->(a:Author)
WITH a, count(r) as count
WHERE count = 2
RETURN a

MATCH (j:Journal)-[r:HAS_PUBLISHED]->(t:Title)
WITH j, count(r) as count
WHERE count >= 50
RETURN j

MATCH (j:Journal { name: 'Plos One' } )-[r:HAS_PUBLISHED]-() RETURN count(r) as count
MATCH (j:Journal { name: 'Plos One' } )-[r:HAS_PUBLICATED]-() RETURN count(r) as count
"""

# DISCONNECT:
mongo_connection.close()
redis_connection.close()
