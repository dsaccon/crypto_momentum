import time
import os
import csv
import datetime as dt
import operator
import logging
import backtrader as bt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import btalib

from .willr_bband import WillRBband, LiveWillRBband

from utils.sns import SNS_call


class ApplicationStateError(Exception):
        pass


class WillRBbandEvo(WillRBband):

    MAX_PERIODS = (20, 14 + 43) # Corresponding to (3m, 60m) data series

    def preprocess_data(self):
        self.cross_buy_open_col = f"crossover:{self.col_tags['long_entry_cross'][0]}-{self.col_tags['long_entry_cross'][1]}"
        self.cross_buy_close_col = f"crossover:{self.col_tags['long_close_cross'][0]}-{self.col_tags['long_close_cross'][1]}"
        self.cross_sell_open_col = f"crossunder:{self.col_tags['short_entry_cross'][0]}-{self.col_tags['short_entry_cross'][1]}"
        self.cross_sell_close_col = f"crossunder:{self.col_tags['short_close_cross'][0]}-{self.col_tags['short_close_cross'][1]}"

        # ..load 60m data
        i = 1
        self.data[i]['willr'] = btalib.willr(self.data[i]['high'], self.data[i]['low'], self.data[i]['close'], period = 14).df
        self.data[i]['willr_ema'] = btalib.ema(self.data[i]['willr'], period = 43, _seed = 3).df
        self.data[i]['willr_ema_prev'] = self.data[i]['willr_ema'].shift(1)

        # ..load 3m data
        i = 0
        self.data[i]['bband_20_low'] = btalib.bbands(self.data[i]['close'], period = 20, devs = 2.2).bot
        self.data[i]['bband_20_low_prev'] = self.data[i]['bband_20_low'].shift(1)
        self.data[i]['bband_20_high'] = btalib.bbands(self.data[i]['close'], period = 20, devs = 2.2).top
        self.data[i]['bband_20_high_prev'] = self.data[i]['bband_20_high'].shift(1)
        self.data[i]['close_prev'] = self.data[i]['close'].shift(1)
        self.data[i]['bband_20_mid'] = (self.data[i]['bband_20_low'] + self.data[i]['bband_20_high'])/2

        # Upsample longer interval series to dataframe at index=0
        modulo = int(self.cfg['series'][1][-1])
        for _i, row in self.data[0].iterrows():
            dt_60m = int(_i - _i % modulo) - modulo
            if not dt_60m >= self.data[1].index[0]:
                # Skip first 60m row
                continue
            self.data[0].at[_i, 'willr_ema'] = self.data[1].at[dt_60m, 'willr_ema']
            self.data[0].at[_i, 'willr_ema_prev'] = self.data[1].at[dt_60m, 'willr_ema_prev']

        if self.cfg['floating_willr']:
            self._create_floating_willr()

        i = 0
        # For Long entry
        self.get_crosses('close', 'bband_20_low', i)

        # For Long close
        self.get_crosses('close', 'bband_20_high', i)

        # For Short entry
        self.get_crosses('close', 'bband_20_high', i, over=False)

        # For Short close
        self.get_crosses('close', 'bband_20_low', i, over=False)

        self.data[0].to_csv(f'logs/postprocess.csv') ### tmp
        
    def _bband_only (self, row):
        """
        Modified trade logic.
        Anytime entry. Attempts to fix issue of position entry only on the first
            ..3m tick of the 60m period
        """
        tag = ''
        if self.cfg['floating_willr']:
            tag = f"_{self.cfg['series'][1][1]}_float"

        # if row[f'willr_ema{tag}'] > row[f'willr_ema_prev{tag}'] and not self.position > 0:
        #     self.position_open_state = 'long_open'
        # elif row[f'willr_ema{tag}'] < row[f'willr_ema_prev{tag}'] and not self.position < 0:
        #     self.position_open_state = 'short_open'

        if self.position == 0:
            if row[self.cross_buy_open_col]:
                # Long entry
                self.position = 1
                self.position_open_state = False
                settings = (row['datetime'], 'Long', 'Open', row['close'])
                self._execute_trade(settings)
        elif self.position > 0 and row['close']>row['bband_20_mid']:
                # Long close
                self.position = 0
                settings = (row['datetime'], 'Long', 'Close', row['close'])
                self._execute_trade(settings)
                return True
        elif self.position == 0:
            if row[self.cross_sell_open_col]:
                # Short entry
                self.position = -1
                self.position_open_state = False
                settings = (row['datetime'], 'Short', 'Open', row['close'])
                self._execute_trade(settings)
        elif self.position < 0 and row['close']<row['bband_20_mid']:
                # Short close
                self.position = 0
                settings = (row['datetime'], 'Short', 'Close', row['close'])
                self._execute_trade(settings)
                return True
        return False

    def _execute_trade_all_params_willemastop(self, row):
        """
        Modified trade logic.
        inclues willrema_long/short entry, willr_long/short_entry, willrema_diff_threshold
        and stoploss and timestop 
        """
        tag = ''
        if self.cfg['floating_willr']:
            tag = f"_{self.cfg['series'][1][1]}_float"
        

        willr_threshold = self.cfg['willrema_diff_threshold']
        willrEMA_long_entry = self.cfg['willrema_long_entry'] #-100 will ignore threshold
        willrEMA_short_entry = self.cfg['willrema_short_entry'] #0 will ignore threshold
        willr_long_entry = self.cfg['willr_long_entry'] #-100 will ignore threshold
        willr_short_entry = self.cfg['willr_short_entry'] #0 will ignore threshold

        timestop =  self.cfg['timestop']
        stoploss =  self.cfg['stoploss']


        if ((row[f'willr_ema{tag}']  > willrEMA_long_entry) and  #overbought willrema implying bullish trend
           (row[f'willr{tag}']  > willr_long_entry) and  #overbought willr implying bullish trend
           ((row[f'willr_ema{tag}'] - willr_threshold) > row[f'willr_ema_prev{tag}']) and  #more overbought than before
           not self.position > 0):
            self.position_open_state = 'long_open'
        elif ((row[f'willr_ema{tag}'] < willrEMA_short_entry) and  #oversold willrema implying bearish trend
             (row[f'willr{tag}'] < willr_short_entry) and  #oversold willr implying bearish trend
             ((row[f'willr_ema{tag}'] + willr_threshold) < row[f'willr_ema_prev{tag}']) and  #more overshold than before
             not self.position < 0):
            self.position_open_state = 'short_open'
        else:
            self.position_open_state = 'neutral'

        if self.position == 0 and self.position_open_state == 'long_open':
            if row[self.cross_buy_open_col]:
                # Long entry
                self.open_price = row['close']
                self.time_opened = row['datetime']
                self.position = 1
                self.position_open_state = False
                settings = (row['datetime'], 'Long', 'Open', row['close'])
                self._execute_trade(settings)

        elif self.position > 0:
            if (row[self.cross_buy_close_col] or 
               (stoploss > 0 and row['close'] < self.open_price*(1-stoploss)) or  #stoploss
               ((row['datetime'] - self.time_opened) > timestop) or  #timestop
               ((row[f'willr_ema{tag}'] + willr_threshold) < row[f'willr_ema_prev{tag}'])): #willrema suggests going short
                # Long close
                self.position = 0
                settings = (row['datetime'], 'Long', 'Close', row['close'])
                self._execute_trade(settings)
                return True
        elif self.position == 0 and self.position_open_state == 'short_open':
            if row[self.cross_sell_open_col]:
                # Short entry
                self.open_price = row['close']
                self.time_opened = row['datetime']
                self.position = -1
                self.position_open_state = False
                settings = (row['datetime'], 'Short', 'Open', row['close'])
                self._execute_trade(settings)

        elif self.position < 0:
            if (row[self.cross_sell_close_col] or 
               (stoploss > 0 and row['close'] > self.open_price*(1+stoploss)) or  #stoploss
               ((row['datetime'] - self.time_opened) > timestop) or #timestop
               ((row[f'willr_ema{tag}'] - willr_threshold) > row[f'willr_ema_prev{tag}'])): #willrema suggests going long
               # Short close
                self.position = 0
                settings = (row['datetime'], 'Short', 'Close', row['close'])
                self._execute_trade(settings)
                return True
        return False


class LiveWillRBbandEvo(LiveWillRBband, WillRBbandEvo):
    pass
