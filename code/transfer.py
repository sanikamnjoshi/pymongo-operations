import pymongo
import datetime as dt
import pandas as pd

class Transfer():

    def __init__(self):

        collection_list = [
            "collection_1",
            "collection_2",
            "collection_3",
            # ...
            "collection_n"
        ]

        for collection_name in collection_list:
            self.set_source(collection_name)
            self.set_target(collection_name)
            self.transfer(collection_name)

    def set_source(self, collection_name):
        source_client = pymongo.MongoClient("mongodb://usr:pwd@db-server:port/")
        source_db = source_client["source-db-name"]
        self.source_coll = source_db[collection_name]

    def set_target(self, collection_name):
        target_client = pymongo.MongoClient("mongodb://usr:pwd@db-server:port/")
        target_db = target_client["target-db-name"]
        self.target_coll = target_db[collection_name]

    def transfer(self, collection_name):
        print("Now transferring {}...".format(collection_name))

        ### building the query
        query = {}

        ### start time
        start = dt.datetime.now()

        ### transfer
        source_doc = self.source_coll.find(query)
        list_source_doc = list(source_doc)
        self.target_coll.insert_many(list_source_doc)

        ### sanity check
        # df_source_doc = pd.DataFrame(list_source_doc)
        # print(df_source_doc.head)

        ### end time
        end = dt.datetime.now()
        transfer_time = (end - start).seconds
        print("Transferring {} took {} second(s).".format(collection_name, transfer_time))


if __name__ == "__main__":
    Transfer()