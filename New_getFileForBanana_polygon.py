from web3 import Web3
import pandas as pd
import json
import numpy as np
from hexbytes import HexBytes
import requests
import asyncio
import os
import sqlite3
from web3._utils.events import get_event_data
import time

url_polygon = 'wss://polygon-mumbai.g.alchemy.com/v2/DJtnV9hEqeQ-2VuKFCAT2rlW7XbnMohV'
SCL_ADDRESS ='0xD200F64cEcc8bBc1292c0187F5ee6cD7bDf1eeac'


def get_logs(SCL, from_block, to_block, topics):
    max_block_range = 10000
    logs_list = []
    while to_block>from_block:
        step_block = from_block+max_block_range
        print(f"Checking blocks from {from_block} to {step_block}")
        logs_list.append(SCL.w3.eth.get_logs({'fromBlock': from_block,
                                                'toBlock': step_block,
                                                'topics': [topics]}))
        from_block = step_block
    
    from_block = from_block - max_block_range
    logs_list.append(SCL.w3.eth.get_logs({'fromBlock': from_block,
                                            'toBlock': to_block,
                                            'topics': [topics]}))
    return [log for logs in logs_list for log in logs]

def getnewOrderEvents(SCL, w3, minBlock, maxBlock):
    # create filter
    new_order_event = SCL.events.newOrder
    event_signature_hash = w3.keccak(text="newOrder(address,uint32,int64,string,uint32,uint40,uint64,address)").hex()
    list_orders_events = get_logs(SCL, minBlock, maxBlock, event_signature_hash)
    order_events = [
                            get_event_data(
                                new_order_event.w3.codec,
                                new_order_event._get_event_abi(),
                                event
                                )
                            for event in list_orders_events
                            ]
    return [event for event in order_events if event['address']==SCL_ADDRESS]

def getDataDeliveredEvents(SCL,w3, minBlock,maxBlock):
    # create filter
    data_delivered_event = SCL.events.dataDelivered
    event_signature_hash = w3.keccak(text="dataDelivered(uint32,bool,bool)").hex()
    list_data_delivered_events = get_logs(SCL, minBlock, maxBlock, event_signature_hash)
    data_delivered_events = [
                            get_event_data(
                                data_delivered_event.w3.codec,
                                data_delivered_event._get_event_abi(),
                                event
                                )
                            for event in list_data_delivered_events
                            ]
    return [event for event in data_delivered_events if event['address']==SCL_ADDRESS]

def get_SCL_informations(data, tx_hash):
    #define SCL contract
    url = url_polygon
    w3 = Web3(Web3.WebsocketProvider(url))
    with open('contract_abi.json') as f:
        abi = json.load(f) 
    SCL = w3.eth.contract(address=SCL_ADDRESS, abi=abi)
    #get blockrange
    minBlock = int(np.min(data['blockNumber']))
    maxBlock = int(np.max(data['blockNumber']))
    #get all relevant SCL Orders in blockrange
    print("get new Order Events")
    Orders = getnewOrderEvents(SCL, w3, minBlock, maxBlock)
    print("get Data Delivered Events")
    Relay = getDataDeliveredEvents(SCL, w3,  minBlock, maxBlock)
    events = Orders + Relay
    print("adding orderIDs")
    for event in events:
        orderID = event['args']['orderID']
        transactionHash=event['transactionHash']
        data.loc[data['hash']==transactionHash,'orderID']=orderID
    print("adding commitmentIDs")
    for Order in Orders:
        orderID = Order['args']['orderID']
        commitmentID=Order['args']['commitmentID']
        receiverAddress = Order['args']['receiverAddress']
        sender_PIN = Order['args']['_PIN']
        gasForDelivery = Order['args']['_gasForDelivery']
        gasPrice = Order['args']['_gasPrice']
        gasCost = gasForDelivery*gasPrice
        data.loc[data['orderID']==orderID,'commitmentID']=commitmentID
        data.loc[data['orderID']==orderID,'receiverAddress']=receiverAddress
        data.loc[data['orderID']==orderID,'sender_PIN']=sender_PIN
        data.loc[data['orderID']==orderID,'gasCostForDelivery (Wei)']=gasCost
    print("adding Relay_StatusFlag")
    for event in Relay:
        transactionHash=event['transactionHash']
        statusFlag = event['args']['_statusFlag']
        data.loc[data['hash']==transactionHash,'Relay_StatusFlag']=statusFlag
    commitment_list = list(set(data['commitmentID'].tolist()))
    commitment_list = [commitment for commitment in commitment_list if commitment>=0]
    for commitment in commitment_list:
        try:
            commitment_infos = SCL.functions.commitments(commitment).call()
            senderID = commitment_infos[0]
            commitment_fee = commitment_infos[2]
            data.loc[data['commitmentID']==commitment,'senderID'] = senderID
            data.loc[data['commitmentID']==commitment,'commitment_fee'] = commitment_fee
        except:
            print(f'Commitment {commitment} not found')

    for i in range(len(data)):
        data.at[i,'hash'] = data.at[i,"hash"].hex()
    data = data.loc[~ data['hash'].isin(tx_hash), ]
    data = data.drop(data[data.commitmentID <0].index)
    return data

async def getexchangeRate(data):
    ExchangeRateList = data['DateTime'].tolist()
    ExchangeRateList = sorted(set(ExchangeRateList), key = ExchangeRateList.index)
    with requests.Session() as s:
        print("adding exchange rates")
        url = 'https://api.coingecko.com/api/v3/coins/matic-network/history'
        for i in ExchangeRateList:
            print(i)
            payload = {'date': i}
            api_response = s.get(url, params=payload)
            if api_response.status_code == 429:
                time.sleep(120) 
                api_response = s.get(url, params=payload)
                if api_response.status_code == 429:
                    raise NotImplementedError("coingecko rate limit needs additional work")
            data_response = api_response.json()
            data.loc[data['DateTime'] == i, 'ExchangeRate'] = 1/data_response['market_data']['current_price']['chf']
            await asyncio.sleep(0.3)
    data["ExchangeRate"] = data["ExchangeRate"].round(decimals=12)
    return data

def TransformInternalTransaction(internalTx):
    internalTx[['isError', 'value']] = internalTx[['isError', 'value']].apply(pd.to_numeric)
    internalTx.loc[(internalTx['from'] ==SCL_ADDRESS.lower()), 'value'] = internalTx['value'] * -1
    internalTx['value_internal'] = internalTx.groupby('hash')['value'].transform(sum)
    internalTx = internalTx.drop_duplicates(subset=['hash'])
    internalTx = internalTx[internalTx['isError'] == 0]
    columns = ['hash', 'blockNumber', 'timeStamp', 'value_internal']
    internalTx = internalTx[columns]
    return internalTx

def TransformParentTransaction(parentTx):
    parentTx[['isError','value','gasPrice', 'gasUsed']]=parentTx[['isError','value','gasPrice', 'gasUsed']].apply(pd.to_numeric)
    parentTx['Transaction_Fee (Gwei)']=parentTx['gasPrice']*10**(-9)*parentTx['gasUsed']
    parentTx = parentTx.loc[(parentTx['isError'] == 0) & (parentTx['to']==SCL_ADDRESS.lower()), ]
    parentTx = parentTx.rename(columns={'value': 'value_parent'})
    columns = ['hash', 'blockNumber', 'timeStamp', 'value_parent', 'Transaction_Fee (Gwei)']
    parentTx = parentTx[columns]
    return parentTx

def MergeTransactions(parentTx, internalTx):
    data = internalTx.merge(parentTx, how="outer", on="hash")
    data.loc[pd.isnull(data['blockNumber_x']), 'blockNumber_x'] = data['blockNumber_y']
    data.loc[pd.isnull(data['timeStamp_x']), 'timeStamp_x'] = data['timeStamp_y']
    data.loc[pd.isnull(data['value_parent']), 'value_parent'] = 0
    data.loc[pd.isnull(data['value_internal']), 'value_internal'] = 0
    data = data.rename(columns={'blockNumber_x': 'blockNumber', 'timeStamp_x': 'DateTime'})
    data['blockNumber'] = pd.to_numeric(data['blockNumber'])
    for i in range(len(data)):
        data.at[i,'hash'] = HexBytes(data.at[i,'hash'])
    data['DateTime'] = pd.to_datetime(data['DateTime'], unit='s').dt.strftime('%d-%m-%Y')
    columns = ['hash', 'blockNumber', 'DateTime', 'value_internal', 'value_parent', 'Transaction_Fee (Gwei)']
    data = data[columns]
    return data

def ask_for_VAT():
    VAT = input('Do you want to take VAT into account? (y/n): \n')
    while VAT not in ['y','n']:
        VAT = input("Please enter 'y' (yes) or 'n' (no): ")
    if VAT=='y':
        return True
    else:
        return False

def ask_for_separator():
    sep = input('Which delimiter do you want to use for the csv file? (,/;): \n')
    while sep not in [',',';']:
        sep = input("Please enter ',' or ';': ")
    return sep

def Create_SCL_Revenue_file(data, VAT_bool):
    revenue_data = data.copy()
    revenue_data['Value (Wei)'] = revenue_data['value_parent']+revenue_data['value_internal']-revenue_data['commitment_fee']
    revenue_data.loc[revenue_data['Relay_StatusFlag']==True,'Value (Wei)'] = revenue_data['commitment_fee']/2
    revenue_data.drop(revenue_data[revenue_data['Value (Wei)']==0].index, inplace = True)
    revenue_data['Value (Gwei)']=revenue_data['Value (Wei)']*10**(-9)
    revenue_data['Doc']=''
    revenue_data['ExchangeCurrency']='gwei'
    revenue_data["AccountDebit"] = 1027
    revenue_data["AccountCredit"] = 3001
    if VAT_bool:
        VAT = revenue_data.copy()
        VAT["AccountCredit"] = 2330
        revenue_data['Value (Gwei)'] = revenue_data['Value (Gwei)']*0.941
        revenue_data['Value (Gwei)'] = revenue_data['Value (Gwei)'].round(decimals=18)
        VAT['Value (Gwei)'] = VAT['Value (Gwei)']*0.059
        VAT['Value (Gwei)'] = VAT['Value (Gwei)'].round(decimals=18)
        revenue_data = pd.concat([revenue_data,VAT])
    revenue_data["Amount"] = revenue_data["Value (Gwei)"]/(revenue_data["ExchangeRate"]*10**9)
    revenue_data["Amount"] = revenue_data["Amount"].round(decimals=5)
    revenue_data['DateTime'] = pd.to_datetime(revenue_data['DateTime'],format="%d-%m-%Y")
    revenue_data["VatCode"]='F3'
    revenue_data.rename(columns={'DateTime':'Date','hash':'Description', 'Value (Gwei)': 'AmountCurrency'}, inplace = True)
    revenue_data.sort_values(by='blockNumber', inplace=True)
    revenue_columns = ['Date', 'Doc', 'Description','AccountDebit', 'AccountCredit','AmountCurrency','ExchangeCurrency','VatCode','ExchangeRate','Amount']
    revenue_data = revenue_data.loc[revenue_data['Relay_StatusFlag']!=False, ]
    revenue_data = revenue_data[revenue_columns]
    return revenue_data

def Database_MIS(data):
    data['commitment_fee (GWei)'] = data['commitment_fee']*10**(-9)
    data.loc[data['Relay_StatusFlag']!='pending','transactionType']='Relay'
    data.loc[data['Relay_StatusFlag']=='pending','transactionType']='Order'
    orders = data.loc[data['transactionType']=='Order', ].copy()
    relay = data.loc[data['transactionType']=='Relay', ].copy()
    order_list = list(set(orders['orderID'].tolist()))
    for order in order_list:
        try:
            orders.loc[orders['orderID']==order,'Relay_StatusFlag']= relay.loc[relay['orderID']==order,'Relay_StatusFlag'].tolist()[0]
        except (KeyError, IndexError):
            print(f'No Relay with orderID {order}')
            pass
    
    orders['gasCostForDelivery (GWei)']= orders['gasCostForDelivery (Wei)']*10**(-9)
    orders['Transaction_Fee (Gwei)'] = orders['Transaction_Fee (Gwei)'].fillna(0)
    orders['OrderCost (GWei)'] = orders['gasCostForDelivery (GWei)']+orders['commitment_fee (GWei)'] +orders['Transaction_Fee (Gwei)']
    orders.loc[orders['Relay_StatusFlag']==False, 'OrderCost (GWei)'] = orders['gasCostForDelivery (GWei)'] + orders['Transaction_Fee (Gwei)']
    orders["OrderCost (CHF)"] = orders["OrderCost (GWei)"]/(orders["ExchangeRate"]*10**9)
    orders_columns = ['hash', 'orderID', 'receiverAddress', 'DateTime', 'sender_PIN', 'senderID',
                    'commitmentID','Relay_StatusFlag', 'Transaction_Fee (Gwei)', 'gasCostForDelivery (GWei)', 'commitment_fee (GWei)', 
                    'OrderCost (GWei)','ExchangeRate','OrderCost (CHF)']
    orders = orders[orders_columns]
    relay_list = list(set(relay['orderID'].tolist()))
    if len(relay_list)==0:
        print('no complete Order&Delivery transaction since last scanned block')
        exit()
    else:
        for order in relay_list:
            try:
                relay.loc[relay['orderID']==order,'GasObtainedForDelivery (GWei)']= orders.loc[orders['orderID']==order,'gasCostForDelivery (GWei)'].tolist()[0]
            except (KeyError, IndexError):
                print(f'Not order for for orderID {order}?')
                relay.loc[relay['orderID']==order,'GasObtainedForDelivery (GWei)']=0
                pass
        relay['Unused_GasForDelivery (GWei)'] = relay['GasObtainedForDelivery (GWei)'] - relay['Transaction_Fee (Gwei)']
        relay['Sender_Profit (Gwei)'] = relay['Unused_GasForDelivery (GWei)'] + relay['commitment_fee (GWei)']/2
        relay.loc[relay['Relay_StatusFlag']!=True, 'Sender_Profit (Gwei)'] = relay['Unused_GasForDelivery (GWei)']
        relay['Sender_Profit (CHF)'] =  relay['Sender_Profit (Gwei)']/(relay["ExchangeRate"]*10**9)
        relay_columns = ['hash', 'orderID', 'receiverAddress', 'DateTime', 'sender_PIN', 'senderID',
                    'commitmentID','Relay_StatusFlag', 'GasObtainedForDelivery (GWei)', 'Unused_GasForDelivery (GWei)', 'commitment_fee (GWei)', 
                    'Sender_Profit (Gwei)','ExchangeRate','Sender_Profit (CHF)']
        relay = relay[relay_columns]
        return orders, relay


if __name__=='__main__':
    if os.path.isfile("checkpoint/startblock.txt"):
        with open("checkpoint/startblock.txt", "r") as f1:
            b = f1.read()
            try:
                startblock = str(b)
            except:
                startblock = '0'
    else:
        startblock = '0'

    connection = sqlite3.connect('checkpoint/sqlite_tx.db')
    cursor = connection.cursor()
    # create table in db if not yet created before
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS tx_hash (id INTEGER PRIMARY KEY, tx_hash TEXT, blockNumber INTEGER)")

    # Get normal Transaction from Polygonscan API and create DataFrame
    REQUESTS_HEADERS = {"User-Agent": "BCP/accounting"}
    API_key = '9B9QU1IK31EYVMAV1CTTPFKQC7JP2WSJEH'
    URL = 'https://api-testnet.polygonscan.com/api?module=account&action=txlist&address='+SCL_ADDRESS+'&startblock='+startblock+'&endblock=latest&sort=asc&apikey=' + API_key
    response = requests.get(URL, headers=REQUESTS_HEADERS)
    parentTx = pd.DataFrame.from_dict(response.json()['result'])

    # Get internal Transaction from Polygonscan API and create DataFrame
    REQUESTS_HEADERS = {"User-Agent": "BCP/accounting"}
    API_key = '9B9QU1IK31EYVMAV1CTTPFKQC7JP2WSJEH'
    URL = 'https://api-testnet.polygonscan.com/api?module=account&action=txlistinternal&address='+SCL_ADDRESS+'&startblock='+startblock+'&endblock=latest&sort=asc&apikey=' + API_key
    response_internal = requests.get(URL, headers=REQUESTS_HEADERS)
    internalTx = pd.DataFrame.from_dict(response_internal.json()['result'])
    if len(internalTx)== 0:
        print('No transaction since last scanned block')
        exit()

    # Tranform normal and internal Transaction
    print('normalTransaction: clean up')
    parentTx = TransformParentTransaction(parentTx)
    print('internalTransaction: clean up')
    internalTx = TransformInternalTransaction(internalTx)
   
    # Merge parent and internal Transactions
    print('merge transactions')
    data = MergeTransactions(parentTx, internalTx)
    
    #add col commitmentID, Relay_StatusFlag 
    data['commitmentID'] = -1
    data['Relay_StatusFlag'] = 'pending'

    # create list of tx hashes already considered in minBlock
    minBlock = np.min(data['blockNumber'])
    cursor.execute(f"SELECT * FROM tx_hash where blockNumber ={minBlock}")
    rows = cursor.fetchall()
    tx_hash = []
    for row in rows:
        tx_hash.append(row[1])    
    # add receiverAddress, Sender_PIN, orderID, CommitmentID, statusFlag and commitment_fee
    data = get_SCL_informations(data, tx_hash)
    if len(data) == 0:
        print('No new transaction since last scanned block')
        exit()

    # add exchange rates
    data = asyncio.run(getexchangeRate(data))
    
    # get the last scanned block
    maxBlock = np.max(data['blockNumber'])
    # create .csv files   
    MIS_database = Database_MIS(data)
    # ask if VAT should be taken into account
    VAT_bool = ask_for_VAT()
    csv_separator = ask_for_separator()
    Create_SCL_Revenue_file(data, VAT_bool).to_csv(f'SCL_block_{startblock}_to_{maxBlock}.csv',sep=csv_separator, index=False)

    if startblock =='0':
        MIS_database[0].to_csv(f'SCL_Orders_Database_up_to_block_{startblock}.csv',sep=csv_separator, index=False)
        MIS_database[1].to_csv(f'SCL_Delivery_Database_up_to_block_{startblock}.csv', sep=csv_separator, index=False)
    else:
        
        MIS_database[0].to_csv(f'SCL_Orders_Database_up_to_block_{startblock}.csv', mode ='w+', sep=csv_separator, index=False, header= True)
        MIS_database[1].to_csv(f'SCL_Delivery_Database_up_to_block_{startblock}.csv', mode = 'w+', sep=csv_separator, index=False, header= True)
    os.rename(f'SCL_Orders_Database_up_to_block_{startblock}.csv',f'SCL_Orders_Database_up_to_block_{maxBlock}.csv')
    os.rename(f'SCL_Delivery_Database_up_to_block_{startblock}.csv',f'SCL_Delivery_Database_up_to_block_{maxBlock}.csv')

    # Create Checkpoints
    # add transaction on the last scanned block to the database
    LastBlockTransactions = data.loc[data['blockNumber'] == maxBlock, ]
    LastBlockTransactions = list(set(LastBlockTransactions['hash'].tolist()))
    for hash in LastBlockTransactions:
        cursor.execute("INSERT INTO tx_hash(tx_hash, blockNumber) VALUES (?,?)", (hash, int(maxBlock),))
        connection.commit()
    # store the number of the last scanned block
    with open("checkpoint/startblock.txt", "w") as f2:
        f2.write(str(maxBlock))