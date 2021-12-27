import pymongo
import json 
import os 

def get_file_names(base_dir):
    return os.listdir(base_dir)

def read_file(base_dir,name):
    path = os.path.join(base_dir,name)
    with open(path,'r',encoding="utf-8") as f:
        raw_json = f.read()
    return json.loads(raw_json)

def get_client(path = None):
    if path is None:
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    else:
        myclient = pymongo.MongoClient(path)
    return myclient

def create_database(myclient,db_name):
    db = myclient[db_name]
    return db

def create_collection(db,collection_name):
    collection = db[collection_name]
    return collection

def insert_collection(collection,data):
    try:
        col =  collection.insert_many(data)
        return True 
    except:
        return False
def clean_doc(data):
    for doc in data:
        if type(doc['_id'])==dict:
            doc["_id"] = doc["_id"]["$oid"]

if __name__=="__main__":
    base_dir = "/Users/gunnvantsaini/Downloads/mongo-export-02.12.2021 _ 2"
    file_names = get_file_names(base_dir)
    print(file_names)
    client = get_client()
    db_name = "jigsaw"
    db = create_database(client,db_name)
    for file_name in file_names:
        data = read_file(base_dir,file_name)
        clean_doc(data)
        collection = create_collection(db,file_name)
        if insert_collection(collection,data):
            print(f"Created collection {file_name} successfully and dumped data")
        else:
            print(f"Created collection {file_name} successfully but data dump failed")



    
