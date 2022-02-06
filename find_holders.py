from asyncio import futures
import json
import eth_abi
from web3 import Web3
from web3.middleware import geth_poa_middleware
import asyncio
import ray
import os
import pymongo
from ray.util import inspect_serializability
from web3.logs import STRICT, IGNORE, DISCARD, WARN
infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
import ray

connection_url = 'mongodb+srv://sourabh:sourabh@cluster0.il3sa.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
client = pymongo.MongoClient(connection_url)
Database = client.get_database('myFirstDB')


def mongo(token_id, holder):
    
    connection_url = 'mongodb+srv://sourabh:sourabh@cluster0.il3sa.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
    client = pymongo.MongoClient(connection_url)
    Database = client.get_database('myFirstDB')
    holdersTable = Database.holdersTable
    queryObject = {
    'token_id': token_id,
    'holders': holder,    
    }
    #print(holders,token_id)
    holdersTable.insert_one(queryObject)

@ray.remote
def holdersEvent(_fromBlock, _toBlock,address):
    try:
        web3 = Web3(Web3.WebsocketProvider(infura_url))
        web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
        abi = json.load(open('erc721.json','r'))
        contract = web3.eth.contract(address=address,abi=abi)
    
        transferEvents = contract.events.Transfer.createFilter(fromBlock=_fromBlock, toBlock=_toBlock)
        for i in range(len(transferEvents.get_all_entries())):
            try:
                token_id =transferEvents.get_all_entries()[i].args.tokenId
                holder = transferEvents.get_all_entries()[i].args.to
                print(holder)
                mongo(token_id, holder)
                
            except eth_abi.exceptions.InsufficientDataBytes:
                pass           
    except asyncio.TimeoutError: 
        pass
inspect_serializability(holdersEvent, name="contract")   

ray.init()
contractTable = Database.contractTable
query = contractTable.find()
details =[]
futures = []
for i in query:
    details.append(i)
for l in details:
        futures.append(holdersEvent.remote(l['from'],l['to'], l['contract']))
ray.get(futures)

