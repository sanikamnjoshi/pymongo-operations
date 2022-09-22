import pymongo
import datetime as dt
import pandas as pd

class Connect():

    def __init__(self):
        client = pymongo.MongoClient("mongodb://usr:pwd@db-server:port/")
        # example: client = pymongo.MongoClient("mongodb://usr:pwd@localhost:27017/")
        db = client["db-name"]
        coll = db["collection-name"]

        ### bulding the query
        time_day = dt.datetime.strptime("2022-08-11", "%Y-%m-%d")
        query = {"time_day": { "$gte": time_day },
                 "network_id": { "$in": [742,156] }}

        ### running the query
        doc = coll.find(query)
        list_doc = list(doc)

        # sanity check
        df = pd.DataFrame(list_doc)
        print(df.head)


if __name__ == "__main__":
    Connect()