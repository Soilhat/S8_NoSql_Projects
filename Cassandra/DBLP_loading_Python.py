import json
from cassandra.cluster import Cluster
cluster=Cluster(['127.0.0.1'])
session = cluster.connect()
session.execute("CREATE KEYSPACE IF NOT EXISTS DBLP WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':3};")
session.set_keyspace('dblp')
session.execute("CREATE TYPE IF NOT EXISTS pagesType ( \
                    start INT, \
                    end INT\
                 );")
session.execute("CREATE TYPE IF NOT EXISTS journalType ( \
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
with open('DBLP_clean.json', 'r') as file:
    for data in file.readlines():
        dataJSON = json.loads(data.replace("'", "''"))
        dataJSON['year'] = int(dataJSON['year'])
        if (dataJSON['pages']['start'] != None):
            dataJSON['pages']['start']= int(dataJSON['pages']['start'])
        if (dataJSON['pages']['end'] != None):
            dataJSON['pages']['end']= int(dataJSON['pages']['end'])
        data = str(dataJSON)
        data = data.replace("'", '"')
        data = data.replace('""', "''")
        data = data.replace('\n', "")
        data = data.replace("_id", "id")
        data = data.replace('None', 'null')
        data = data.replace("''\"", "\"''")
        data = data.replace("\"'',","''\",")
        
        
        statement = "INSERT INTO dblp JSON '"+ data + "';"
        session.execute(statement)