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
from decimal import Decimal
import math
import os
import csv
from exchanges.binance import BinanceAPI


#params
#token_X = {token:t, weight:w, trade_threshold:t_t, notrade_threshold:nt_t}

token_A = {'token':'BTC', 'weight': 0.25, 'tt':0.0075, 'ntt':0.005, 'price':{}, 'imp':{}, 'pos':0}
token_B = {'token':'ETH', 'weight': 0.25, 'tt':0.0075, 'ntt':0.005, 'price':{}, 'imp':{}, 'pos':0}
token_C = {'token':'LTC', 'weight': 0.25, 'tt':0.0075, 'ntt':0.005, 'price':{}, 'imp':{}, 'pos':0}
token_D = {'token':'XRP', 'weight': 0.25, 'tt':0.0075, 'ntt':0.005, 'price':{}, 'imp':{}, 'pos':0}

tokens = (token_A,token_B,token_C,token_D)

SMA = 20
basket_threshold = .01
trade_size = 10000
candle_length = 60 #in seconds

#create empty df
df = pd.DataFrame()

#get Binance candlestic data
# client.get_historical_candles('BTCUSDT', 180, dt.datetime.now() - dt.timedelta(seconds=650), dt.datetime.now(),
client = BinanceAPI()
for token in tokens:
    candle_data = client.get_historical_candles((token['token'] + 'USDT'), candle_length, dt.datetime.now() - dt.timedelta(seconds=100000), dt.datetime.now(), asset_type='futures')
    print('candle_data full', candle_data)
    df[token['token']]=candle_data['close']

df['datetime']=candle_data['datetime']

#calc basket price
df['basket'] = (
                (df[token_A['token']]*token_A['weight']) +
                (df[token_B['token']]*token_B['weight']) +
                (df[token_C['token']]*token_C['weight']) +
                (df[token_D['token']]*token_D['weight'])
                )

for token in tokens:
    #calc moving averages
    df[(token['token'] + '-ma')] = df[token['token']].rolling(window=SMA).mean()
    #calc impulses
    df[(token['token'] + '-imp')] = (df[token['token']]/df[(token['token'] + '-ma')]-1)
# df[(token_A['token'] + '-ma')] = df[token_A['token']].rolling(window=SMA).mean()
# df[(token_B['token'] + '-ma')] = df[token_B['token']].rolling(window=SMA).mean()
# df[(token_C['token'] + '-ma')] = df[token_C['token']].rolling(window=SMA).mean()
# df[(token_D['token'] + '-ma')] = df[token_D['token']].rolling(window=SMA).mean()
# df['ETH-ma'] = df['ETH'].rolling(window=SMA).mean()
# df['LTC-ma'] = df['LTC'].rolling(window=SMA).mean()
# df['XRP-ma'] = df['XRP'].rolling(window=SMA).mean()

#calc basket ma
df['basket-ma'] = df['basket'].rolling(window=SMA).mean()
#calc basket impulse
df['basket-imp'] = (df['basket']/df['basket-ma']-1)



# #output df
df.head()
df.to_csv('output_basket.csv')

for index, row in df.iterrows():
    imp_basket = row['basket-imp']

    #close positions
    for token in tokens:
        token['imp'] = row[(token['token'] + str('-imp'))]
        if token['pos'] > 0 and ((imp_basket - token['imp']) < (basket_threshold - token['tt'])):
            token['price'] = row[token['token']]
            print('line', index)
            print ('sell to close', -token['pos'],' ', token['token'], '@', token['price'])
            token['pos']=0
            time.sleep(5)
        elif token['pos'] < 0 and ((token['imp']-imp_basket) < (basket_threshold - token['tt'])):
            token['price'] = row[token['token']]
            print('line', index)
            print ('buy to close', -token['pos'],' ', token['token'], '@', token['price'])
            token['pos']=0
            time.sleep(5)

    if (-basket_threshold < imp_basket < basket_threshold):
        print('nothing')
    else:
           #open a position
        for token in tokens:
            if token['pos'] == 0:

                token['price'] = row[token['token']]
                token['imp'] = row[(token['token'] + str('-imp'))]
                #basket strong so open long pos
                if imp_basket > basket_threshold:
                    if (-token['ntt'] < token['imp'] < token['tt']):
                        size = round(trade_size/token['price'],3)
                        token['pos'] = size
                        print('line', index)
                        print ('buy to open', token['pos'],' ', token['token'], '@', token['price'])
                        time.sleep(5)
                #basket weak so open short pos
                elif imp_basket < -basket_threshold:
                    if (token['ntt'] > token['imp'] > -token['tt']):
                        size = round(trade_size/token['price'],3)
                        token['pos'] = -size
                        print('line', index)
                        print ('sell to open', token['pos'],' ', token['token'], '@', token['price'])
                        time.sleep(5)

    #
# for i in range(SMA + 1, len(df)):
#     imp_basket = df.loc[i,'basket-imp']
#     #close positions
#     for token in tokens:
#         token['imp'] = df.loc[i,token['token'] + str('-imp')]
#         if token['pos'] > 0 and ((imp_basket - token['imp']) < (basket_threshold - token['tt'])):
#             token['price'] = df.loc[i,token['token']]
#             print('line', i)
#             print ('sell to close', -token['pos'],' ', token['token'], '@', token['price'])
#             token['pos']=0
#         elif token['pos'] < 0 and ((token['imp']-imp_basket) < (basket_threshold - token['tt'])):
#             token['price'] = df.loc[i,token['token']]
#             print('line', i)
#             print ('buy to close', -token['pos'],' ', token['token'], '@', token['price'])
#             token['pos']=0
#
#     if (-basket_threshold < imp_basket < basket_threshold):
#         print('nothing')
#     else:
#         #open a position
#         for token in tokens:
#             if token['pos'] == 0:
#
#                 token['price'] = df.loc[i,token['token']]
#                 token['imp'] = df.loc[i,token['token'] + str('-imp')]
#                 #basket strong so open long pos
#                 if imp_basket > basket_threshold:
#                     if (-token['ntt'] < token['imp'] < token['tt']):
#                         size = round(trade_size/token['price'],3)
#                         token['pos'] = size
#                         print('line', i)
#                         print ('buy to open', token['pos'],' ', token['token'], '@', token['price'])
#                 #basket weak so open short pos
#                 elif imp_basket < -basket_threshold:
#                     if (token['ntt'] > token['imp'] > -token['tt']):
#                         size = round(trade_size/token['price'],3)
#                         token['pos'] = -size
#                         print('line', i)
#                         print ('sell to open', token['pos'],' ', token['token'], '@', token['price'])
