
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["MarketDB"]
mycol = mydb["MarketData"]

mydict = { "name": "John", "address": "Highway 37", "età" : "324" }

x = mycol.insert_one(mydict)