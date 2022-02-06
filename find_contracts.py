import json
import eth_abi
from web3 import Web3
from web3.middleware import geth_poa_middleware
import asyncio
import ray
import os
import pymongo
from ray.util import inspect_serializability

infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0) 

connection_url = 'mongodb+srv://sourabh:sourabh@cluster0.il3sa.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
client = pymongo.MongoClient(connection_url)
Database = client.get_database('myFirstDB')



def mongoDb(address, fromBlock, toBlock):
    connection_url = 'mongodb+srv://sourabh:sourabh@cluster0.il3sa.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
    client = pymongo.MongoClient(connection_url)
    Database = client.get_database('myFirstDB')
    fromToTable = Database.fromToTable
    queryObject = {
                'contractAddress': address,
                'limitBlocks':{
                'from':fromBlock,
                'to': toBlock
                }
            }
    print(address, fromBlock,toBlock)
    fromToTable.insert_one(queryObject)



@ray.remote
def holdersContract(address,block):
    web3 = Web3(Web3.WebsocketProvider(infura_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
    abi = json.load(open('erc721.json','r'))
    contract = web3.eth.contract(address=address,abi=abi)
    latest = web3.eth.blockNumber
    firstBlock = block
    totalResult = latest - firstBlock
    initial = firstBlock
    if totalResult >2000:
        while totalResult>=2000:
            fromBlock = initial
            toBlock = initial +2000
            #holdersEvent.remote(fromBlock,toBlock,contract
            mongoDb(address, fromBlock, toBlock)
            totalResult = totalResult -2000
            initial = toBlock
        if totalResult != 0:
            fromBlock = initial
            toBlock = initial + totalResult
            #holdersEvent.remote(fromBlock,toBlock,contract)
            mongoDb(address, fromBlock, toBlock)
    else:
        mongoDb(address, firstBlock, latest)
        #c.holdersEvent.remote(firstBlock,latest,contract)
    return 0

inspect_serializability(holdersContract, name="contract")   

if __name__== "__main__":
    ray.init()
    futures=[]
    Database = client.get_database('myFirstDB')
    SampleTable = Database.SampleTable
    query = SampleTable.find()
    output={}
    i=0
    details =[]
    for x in query:
        details.append(x)
    for l in details:
        block = web3.eth.getTransactionReceipt(l['tx_hash']).blockNumber
        futures.append(holdersContract.remote(l['contractAddress'], block))
    ray.get(futures)