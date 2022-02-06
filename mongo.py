import pymongo

connection_url = 'mongodb+srv://sourabh:sourabh@cluster0.il3sa.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
client = pymongo.MongoClient(connection_url)
print(client)
Database = client.get_database('myFirstDB')
SampleTable = Database.SampleTable

def insertOne(name, id):
    queryObject = {
        'Name': name,
        'ID': id
    }
    query = SampleTable.insert_one(queryObject)
    print(query)
insertOne('Aakash', 12)