import pymongo


MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "my_db"
COLLECTION_NAME = "suspicious_customers_orders"

mongo_client = pymongo.MongoClient(MONGO_URI)
collection = mongo_client[DB_NAME][COLLECTION_NAME]



