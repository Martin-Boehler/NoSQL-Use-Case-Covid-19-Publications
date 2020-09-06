import re
import csv
import neo4j  # https://neo4j.com/docs/cypher-manual/4.1/ # http://localhost:7474/browser/
import pymongo  # https://pymongo.readthedocs.io/en/stable/
import redis  # https://redis-py.readthedocs.io/en/stable/

# NOSQL DATABASE CONNECTIONS:
host = "localhost"

# MONGO:
port = 27017
username = "mongo"
password = "password"
database = "Information-Systems"
collection = "Covid-19"
mongo_connection = pymongo.MongoClient("mongodb://{2}:{3}@{0}:{1}".format(host, port, username, password))
mongo_database = mongo_connection.get_database(database)
mongo_collection = mongo_database.get_collection(collection)

# NEO4J:
port = 7687
username = "neo4j"
password = "password"
neo4j_connection = neo4j.GraphDatabase.driver("bolt://{0}:{1}".format(host, port), auth=(username, password))
neo4j_session = neo4j_connection.session()

# REDIS:
port = 6379
database = 0
redis_connection = redis.StrictRedis(host, port, database, charset="utf-8", decode_responses=True)

# RESET DATABASES:
mongo_collection.delete_many({})
neo4j_session.run("MATCH (n) DETACH DELETE n")
redis_connection.flushall()

# CSV SOURCE DEFINITIONS:
sources = ["CORD-19 by White House", "COVID-19 by WHO"]
files = [".\data\cord-19_2020-08-29.csv", ".\data\who_covid-19_20200831.csv"]

# READ CSV FILES AND CREATE DATA MODELS:
for file in files:
    index = files.index(file)
    with open(file, encoding="utf8") as data:
        rows = csv.DictReader(data)
        for row in rows:
            title = row["Title"]
            authors = row["Authors"].split("; ")
            journal = row["Journal"]
            if title and authors[0] and journal:
                for author in authors:

                    # CLEANING:
                    title = title.replace('"', "").replace("'", "").replace("  ", " ").strip().title()
                    author = author.replace(".", " ").replace('"', "").replace("'", "").replace("  ", " ").strip().title()
                    journal = journal.replace('"', "").replace("'", "").replace("  ", " ").strip().title()
                    source = sources[index]

                    # MONGO: USER STORY 1
                    mongo_document = {"title": title, "author": author, "journal": journal, "source": source}
                    mongo_collection.insert_one(mongo_document)

                    # NEO4J: USER STORY 2, 4, 5
                    neo4j_session.run("MERGE (t:Title { name: '%s' })" % (title))
                    neo4j_session.run("MERGE (a:Author { name: '%s' })" % (author))
                    neo4j_session.run("MERGE (j:Journal { name: '%s' })" % (journal))
                    neo4j_session.run("MERGE (s:Source { name: '%s' })" % (source))
                    neo4j_session.run("MATCH (a:Author { name: '%s' }) MATCH (t:Title { name: '%s' }) MERGE (a)-[:HAS_WRITTEN]->(t)" % (author, title))
                    neo4j_session.run("MATCH (j:Journal { name: '%s' }) MATCH (t:Title { name: '%s' }) MERGE (j)-[:HAS_PUBLISHED]->(t)" % (journal, title))
                    neo4j_session.run("MATCH (a:Author { name: '%s' }) MATCH (j:Journal { name: '%s' }) MERGE (a)-[:HAS_PUBLICATED]->(j)" % (author, journal))
                    neo4j_session.run("MATCH (s:Source { name: '%s' }) MATCH (t:Title { name: '%s' }) MERGE (s)-[:CONTAIN]->(t)" % (source, title))
                    neo4j_session.run("MATCH (s:Source { name: '%s' }) MATCH (a:Author { name: '%s' }) MERGE (s)-[:CONTAIN]->(a)" % (source, author))
                    neo4j_session.run("MATCH (s:Source { name: '%s' }) MATCH (j:Journal { name: '%s' }) MERGE (s)-[:CONTAIN]->(j)" % (source, journal))
                    for partner in authors:
                        partner = partner.replace(".", " ").replace('"', "").replace("'", "").replace("  ", " ").strip().title()  # CLEANING
                        if partner != author:
                            neo4j_session.run("MATCH (a:Author { name: '%s' }) MATCH (p:Author { name: '%s' }) MERGE (a)-[:WORKED_TOGETHER]->(p)" % (author, partner))

                    # REDIS: USER STORY 3
                    words = title.split(" ")
                    for word in words:
                        word = re.sub("\W+", "", word).lower()  # CLEANING
                        if word:
                            value = redis_connection.hget(word, title)
                            redis_connection.hset(word, title, "{0} {1};".format("" if not value else value, author).strip())

            # DEBUG, STOP IN LINE:
            if rows.line_num == 1000:
                break

# DISCONNECT NOSQL DATABASES:
mongo_connection.close()
neo4j_connection.close()
redis_connection.close()
