#!/usr/bin/env python
# coding: utf-8

#pandas use to convert data in tabular form
import pandas as pd
import numpy as np
#lib for graph
import matplotlib.pyplot as plt
import time
import datetime as dt
from datetime import datetime
# from decimal import Decimal
import decimal
import math
import os
import csv
from exchanges.binance import BinanceAPI

from utils.sns import SNS_call

SNS_call(msg=(
    f" start basket run"
    ))

#PARAMETERS...
#token_X = {token:t, weight:w, trade_threshold:t_t, notrade_threshold:nt_t}
token_A = {'token':'BTC', 'weight': 0.25, 'tt':0.0025, 'ntt':0.001, 'price':{}, 'imp':{}, 'ma':{}, 'pos':0}
token_B = {'token':'ETH', 'weight': 0.25, 'tt':0.0025, 'ntt':0.001, 'price':{}, 'imp':{}, 'ma':{}, 'pos':0}
token_C = {'token':'LTC', 'weight': 0.25, 'tt':0.0025, 'ntt':0.001, 'price':{}, 'imp':{}, 'ma':{}, 'pos':0}
token_D = {'token':'XRP', 'weight': 0.25, 'tt':0.0025, 'ntt':0.001, 'price':{}, 'imp':{}, 'ma':{}, 'pos':0}
basket  = {'token':'basket','price':{},'imp':{}, 'ma':{}}
tokens = (token_A,token_B,token_C,token_D)

time_tick = 10 #in seconds
SMA = 60*60 #in seconds
SMA_nrows = round(SMA/time_tick)
basket_threshold = .005
trade_size = 10000 #in USDT


#    time_candle	symbol	position	action	price size
# #eg, 1596385980	    BNB	    Short	    Open	11167.26
def trade(file_name, row):

    # Open file in append mode
    with open(file_name, 'a+', newline='') as f:
        # Create a writer object from csv module
        writer = csv.writer(f)
        # Add contents of list as last row in the csv file
        writer.writerow(row)



# def write_basket_trades(token):
#     cols = (
#         'time_candle',
#         'symbol',
#         'position',
#         'action',
#         'price')
#     symbol = token['token']
#
#
#     line = (
#         self.start_time,
#         symbol,
#         None,
#         None,
#         token['price']
#     )
#     write_mode = 'a'
#     if not os.path.isfile('logs/backtesting_trades.csv'):
#         write_mode = 'w'
#     else:
#         cols = None
#     if not os.path.isdir(f'logs/plots'):
#         os.mkdir('logs/plots')
#
#     with open(f'logs/backtesting_trades.csv', write_mode, newline='') as f:
#         writer = csv.writer(f)
#         if cols:
#             writer.writerow(cols)
#         writer.writerow(line)


#create empty df
df = pd.DataFrame(columns=['timestamp',
                            token_A['token'],
                            token_B['token'],
                            token_C['token'],
                            token_D['token'],
                            'basket',
                            token_A['token'] + '-ma',
                            token_B['token'] + '-ma',
                            token_C['token'] + '-ma',
                            token_D['token'] + '-ma',
                            'basket-ma',
                            token_A['token'] + '-imp',
                            token_B['token'] + '-imp',
                            token_C['token'] + '-imp',
                            token_D['token'] + '-imp',
                            'basket-imp'
                            ]
                    )

#get Binance live data and calculate metrics per timestamp as ob comes in
#def get_book(self, symbol='BTCUSDT', asset_type='spot', depth=100):
client = BinanceAPI()
while True:
    #get prices
    for token in tokens:
        data = client.get_book(symbol=(token['token'] + 'USDT'), asset_type='futures', depth=5)
        token['best_bid']=float(data['bids'][0][0])
        token['best_ask']=float(data['asks'][0][0])
        token['price']=(token['best_bid']+token['best_ask'])/2
        #print(token['token'], token['best_bid'],token['price'],token['best_ask'])

        # print('token',token)

    basket['price'] = (
                    (token_A['price']*token_A['weight']) +
                    (token_B['price']*token_B['weight']) +
                    (token_C['price']*token_C['weight']) +
                    (token_D['price']*token_D['weight'])
                    )
    #print ('basket_price', basket_price)
    timestamp = data['T']/1000

    if len(df) <= (SMA/time_tick):
        df_latest = {'timestamp':timestamp,
                    token_A['token']:token_A['price'],
                    token_B['token']:token_B['price'],
                    token_C['token']:token_C['price'],
                    token_D['token']:token_D['price'],
                    'basket':basket['price'],
                    token_A['token'] + '-ma':"",
                    token_B['token'] + '-ma':"",
                    token_C['token'] + '-ma':"",
                    token_D['token'] + '-ma':"",
                    'basket-ma':"",
                    token_A['token'] + '-imp':"",
                    token_B['token'] + '-imp':"",
                    token_C['token'] + '-imp':"",
                    token_D['token'] + '-imp':"",
                    'basket-imp':""
                    }

        df = df.append(df_latest, ignore_index = True)

    if len(df) > (SMA/time_tick):

        token_A['ma'] = round((df[token_A['token']].tail(SMA_nrows).mean()),5)
        token_B['ma'] = round((df[token_B['token']].tail(SMA_nrows).mean()),5)
        token_C['ma'] = round((df[token_C['token']].tail(SMA_nrows).mean()),5)
        token_D['ma'] = round((df[token_D['token']].tail(SMA_nrows).mean()),5)
        basket['ma'] = round((df['basket'].tail(SMA_nrows).mean()),5)

        token_A['imp'] = round((token_A['price']/token_A['ma'] - 1),5)
        token_B['imp'] = round((token_B['price']/token_B['ma'] - 1),5)
        token_C['imp'] = round((token_C['price']/token_C['ma'] - 1),5)
        token_D['imp'] = round((token_D['price']/token_D['ma'] - 1),5)
        basket['imp'] = round((basket['price']/basket['ma'] - 1),5)

        df_latest = {'timestamp':timestamp,
                    token_A['token']:token_A['price'],
                    token_B['token']:token_B['price'],
                    token_C['token']:token_C['price'],
                    token_D['token']:token_D['price'],
                    'basket':basket['price'],
                    token_A['token'] + '-ma':token_A['ma'],
                    token_B['token'] + '-ma':token_B['ma'],
                    token_C['token'] + '-ma':token_C['ma'],
                    token_D['token'] + '-ma':token_D['ma'],
                    'basket-ma':basket['ma'],
                    token_A['token'] + '-imp':token_A['imp'],
                    token_B['token'] + '-imp':token_B['imp'],
                    token_C['token'] + '-imp':token_C['imp'],
                    token_D['token'] + '-imp':token_D['imp'],
                    'basket-imp':basket['imp']
                    }

        df = df.append(df_latest, ignore_index = True)


        #trade logic
        #close positions
        for token in tokens:
            #token['imp'] = row[(token['token'] + str('-imp'))]
            if token['pos'] > 0 and ((basket['imp'] - token['imp']) < (basket_threshold - token['tt'])):
                # token['price'] = row[token['token']]
                # print('line', index)
                print('----------------TRADE-------------')
                print ('sell to close', -token['pos'],' ', token['token'], '@', token['price'])
                SNS_call(msg=(
                    f" sell to close {-token['pos']}, {token['token']}"
                    f" @ {token['price']}"
                    ))
                row = [timestamp,
                        token['token'],
                        'Short',
                        'Close',
                        token['price'],
                        -token['pos']
                        ]
                trade('logs/basket_trades.csv', row)
                token['pos']=0
                break
            elif token['pos'] < 0 and ((token['imp']-basket['imp']) < (basket_threshold - token['tt'])):
                #token['price'] = row[token['token']]
                #print('line', index)
                print('----------------TRADE-------------')
                print ('buy to close', -token['pos'],' ', token['token'], '@', token['price'])
                SNS_call(msg=(
                    f" buy to close {-token['pos']}, {token['token']}"
                    f" @ {token['price']}"
                    ))
                row = [timestamp,
                        token['token'],
                        'Long',
                        'Close',
                        token['price'],
                        -token['pos']
                        ]
                trade('logs/basket_trades.csv', row)
                token['pos']=0
                break

        if (-basket_threshold < basket['imp'] < basket_threshold):
            print('nothing')
        else:
            #open a position
            for token in tokens:
                if token['pos'] == 0:
                    #token['price'] = row[token['token']]
                    #token['imp'] = row[(token['token'] + str('-imp'))]
                    #basket strong so open long pos
                    if basket['imp'] > basket_threshold:
                        if (-token['ntt'] < token['imp'] < token['tt']):
                            size = round(trade_size/token['price'],3)
                            token['pos'] = size
                            #print('line', index)
                            print('----------------TRADE-------------')
                            print ('buy to open', token['pos'],' ', token['token'], '@', token['price'])
                            SNS_call(msg=(
                                f" buy to open {token['pos']}, {token['token']}"
                                f" @ {token['price']}"
                                ))
                            row = [timestamp,
                                    token['token'],
                                    'Long',
                                    'Open',
                                    token['price'],
                                    token['pos']
                                    ]
                            trade('logs/basket_trades.csv', row)
                            break
                    #basket weak so open short pos
                elif basket['imp'] < -basket_threshold:
                        if (token['ntt'] > token['imp'] > -token['tt']):
                            size = round(trade_size/token['price'],3)
                            token['pos'] = -size
                            #print('line', index)
                            print('----------------TRADE-------------')
                            print ('sell to open', token['pos'],' ', token['token'], '@', token['price'])
                            SNS_call(msg=(
                                f" sell to open {token['pos']}, {token['token']}"
                                f" @ {token['price']}"
                                ))
                            row = [timestamp,
                                    token['token'],
                                    'Short',
                                    'Open',
                                    token['price'],
                                    token['pos']
                                    ]
                            trade('logs/basket_trades.csv', row)
                            break
    print ('latest', df.tail(1))
    for token in tokens:
        print('TOKEN:', token)
        #print (token['token'], token['imp'])
    #print('basket', basket['imp'])
    print('BASKET:', basket)

    time.sleep(time_tick)


#df.to_csv('output_basket_live.csv')






    # df[token['token']]=candle_data['close']
#
# df['datetime']=candle_data['datetime']
#
# #calc basket price
# df['basket'] = (
#                 (df[token_A['token']]*token_A['weight']) +
#                 (df[token_B['token']]*token_B['weight']) +
#                 (df[token_C['token']]*token_C['weight']) +
#                 (df[token_D['token']]*token_D['weight'])
#                 )
#
# for token in tokens:
#     #calc moving averages
#     df[(token['token'] + '-ma')] = df[token['token']].rolling(window=SMA).mean()
#     #calc impulses
#     df[(token['token'] + '-imp')] = (df[token['token']]/df[(token['token'] + '-ma')]-1)
#
# #calc basket ma
# df['basket-ma'] = df['basket'].rolling(window=SMA).mean()
# #calc basket impulse
# df['basket-imp'] = (df['basket']/df['basket-ma']-1)
#
# df.to_csv('output_basket.csv')
#
# for index, row in df.iterrows():
#     token_B['imp']asket = row['basket-imp']
#
#     #close positions
#     for token in tokens:
#         token['imp'] = row[(token['token'] + str('-imp'))]
#         if token['pos'] > 0 and ((token_B['imp']asket - token['imp']) < (basket_threshold - token['tt'])):
#             token['price'] = row[token['token']]
#             print('line', index)
#             print ('sell to close', -token['pos'],' ', token['token'], '@', token['price'])
#             token['pos']=0
#             time.sleep(5)
#         elif token['pos'] < 0 and ((token['imp']-token_B['imp']asket) < (basket_threshold - token['tt'])):
#             token['price'] = row[token['token']]
#             print('line', index)
#             print ('buy to close', -token['pos'],' ', token['token'], '@', token['price'])
#             token['pos']=0
#             time.sleep(5)
#
#     if (-basket_threshold < token_B['imp']asket < basket_threshold):
#         print('nothing')
#     else:
#         #open a position
#         for token in tokens:
#             if token['pos'] == 0:
#                 token['price'] = row[token['token']]
#                 token['imp'] = row[(token['token'] + str('-imp'))]
#                 #basket strong so open long pos
#                 if token_B['imp']asket > basket_threshold:
#                     if (-token['ntt'] < token['imp'] < token['tt']):
#                         size = round(trade_size/token['price'],3)
#                         token['pos'] = size
#                         print('line', index)
#                         print ('buy to open', token['pos'],' ', token['token'], '@', token['price'])
#                         time.sleep(5)
#                 #basket weak so open short pos
#                 elif token_B['imp']asket < -basket_threshold:
#                     if (token['ntt'] > token['imp'] > -token['tt']):
#                         size = round(trade_size/token['price'],3)
#                         token['pos'] = -size
#                         print('line', index)
#                         print ('sell to open', token['pos'],' ', token['token'], '@', token['price'])
#                         time.sleep(5)

