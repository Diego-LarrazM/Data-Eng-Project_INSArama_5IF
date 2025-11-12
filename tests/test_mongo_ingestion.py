from pymongo import MongoClient

host = "mongodb://localhost:27017/"
client = MongoClient(host, username="insarama", password="insarama")
print(client.list_database_names())
print(client["insarama_db"].list_collection_names())
for doc in client["insarama_db"].test_collection.find():
    print(doc)