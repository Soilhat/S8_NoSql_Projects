import json
from cassandra.cluster import Cluster
cluster=Cluster(['127.0.0.1'])
session = cluster.connect()
session.execute("CREATE KEYSPACE IF NOT EXISTS DBLP WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':3};")
session.set_keyspace('dblp')
session.execute("CREATE TYPE pagesType ( \
                    start INT, \
                    end INT\
                 );")
session.execute("CREATE TYPE journalType ( \
                series VARCHAR,\
                editor VARCHAR,\
                volume VARCHAR,\
                isbn LIST<VARCHAR>\
               );")
session.execute("CREATE TABLE IF NOT EXISTS DBLP ( \
                id VARCHAR, \
                type VARCHAR,\ 
                year INT, \
                title VARCHAR,\ 
                authors LIST<VARCHAR>,\ 
                pages frozen<pagesType>,\ 
                booktitle VARCHAR, \
                journal frozen<journalType>,\ 
                url VARCHAR, \
                cites LIST<VARCHAR>,\ 
                PRIMARY KEY(id) \
               );")

session.execute('TRUNCATE dblp;')
with open('DBLPTest.json', 'r') as file:
    for data in file.readlines():
        data =data.replace("'", "''")
        #dataJSON = json.loads(data)
        #dataJSON['year'] = int(dataJSON['year'])
        #data = str(dataJSON)
        data =data.replace('\n', "")
        data =data.replace("_id", "id")
        
        statement = "INSERT INTO dblp JSON '"+ data + "';"
        print(statement)
        #session.execute(statement)