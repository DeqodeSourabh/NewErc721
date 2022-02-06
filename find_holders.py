import json
import eth_abi
from web3 import Web3
from web3.middleware import geth_poa_middleware
import asyncio
import ray
import os

infura_url= "wss://mainnet.infura.io/ws/v3/57d8e5ec16764a3e86ce18fc505e640e"
web3 = Web3(Web3.WebsocketProvider(infura_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0) 
import ray

connection_url = 'mongodb+srv://sourabh:sourabh@cluster0.il3sa.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'
client = pymongo.MongoClient(connection_url)
Database = client.get_database('myFirstDB')

@ray.remote
def holdersEvent(_fromBlock, _toBlock,contract):
    try:
        transferEvents = contract.events.Transfer.createFilter(fromBlock=_fromBlock, toBlock=_toBlock)
        for i in range(len(transferEvents.get_all_entries())):
            try:
                print(transferEvents.get_all_entries()[i].args.tokenID)
                addressTo = transferEvents.get_all_entries()[i].args.to
                print(addressTo)
                #collection.insert_one({"Contractaddress":address,"holderAddress": addressTo})
            except eth_abi.exceptions.InsufficientDataBytes:
                pass           
    except asyncio.TimeoutError: 
        pass

