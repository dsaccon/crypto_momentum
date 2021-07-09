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
import talib

from dotenv import load_dotenv
from sqlalchemy.sql import text
from sqlalchemy import create_engine

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
        self.data[i]['willr_ema'] = btalib.ema(self.data[i]['willr'], period = self.cfg['willr_ema_period'], _seed = 3).df #orig 43,3
        self.data[i]['willr_ema_prev'] = self.data[i]['willr_ema'].shift(1)

        # ..load 3m data
        i = 0
        self.data[i]['bband_20_low'] = btalib.bbands(self.data[i]['close'], period = self.cfg['bband_period'], devs = self.cfg['bband_devs']).bot #orig 20, 2.3
        self.data[i]['bband_20_low_prev'] = self.data[i]['bband_20_low'].shift(1)
        self.data[i]['bband_20_high'] = btalib.bbands(self.data[i]['close'], period = self.cfg['bband_period'], devs = self.cfg['bband_devs']).top
        self.data[i]['bband_20_high_prev'] = self.data[i]['bband_20_high'].shift(1)
        self.data[i]['close_prev'] = self.data[i]['close'].shift(1)
        self.data[i]['bband_20_mid'] = (self.data[i]['bband_20_low'] + self.data[i]['bband_20_high'])/2

        #other TA indicators
        #self.data[i]['rsi'] = btalib.rsi(self.data[i]['close'], period = 14).df
        self.data[i]['rsi'] = talib.RSI(self.data[i]['close'], timeperiod = 14)
        self.data[i]['chaikin_osc'] = talib.ADOSC(self.data[i]['high'], self.data[i]['low'], self.data[i]['close'], self.data[i]['volume'], fastperiod=3, slowperiod=10)
        self.data[i]['macd'], self.data[i]['macdsignal'], self.data[i]['macdhist'] = talib.MACD(self.data[i]['close'], fastperiod=12, slowperiod=26, signalperiod=9)

        #lookback
        self.data[i]['close_prev_lb'] = self.data[i]['close'].shift(self.cfg['lb_period'])
        self.data[i]['lb_chg'] = round((self.data[i]['close'] - self.data[i]['close_prev_lb'])/self.data[i]['close_prev_lb'],4)


        #bband range as volatility indicator, ie (bbandhigh-bbandlow)/close
        self.data[i]['bband_range_perc'] = (self.data[i]['bband_20_high'] - self.data[i]['bband_20_low'])/self.data[i]['close']

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

    def _execute_trade_all_params_bband_entry(self, row):
        """
        Modified trade logic.
        inclues willrema_long/short entry, willr_long/short_entry, willrema_diff_threshold
        and stoploss and timestop AND bband_entry

		Arguments
		---------
		row (dict):     candle update, converted from last row of self.data[0] df

		Returns
		---------
		(bool):         True if a position was closed, else False

        """
        tag = ''
        if self.cfg['floating_willr']:
            tag = f"_{self.cfg['series'][1][1]}_float"

        willr_threshold = self.cfg['willrema_diff_threshold']
        willrEMA_long_entry = self.cfg['willrema_long_entry'] #-100 will ignore threshold
        willrEMA_short_entry = self.cfg['willrema_short_entry'] #0 will ignore threshold
        willr_long_entry = self.cfg['willr_long_entry'] #-100 will ignore threshold
        willr_short_entry = self.cfg['willr_short_entry'] #0 will ignore threshold

        bband_entry = self.cfg['bband_entry']

        timestop =  self.cfg['timestop']
        stoploss =  self.cfg['stoploss']

        if ((row[f'willr_ema{tag}']  > willrEMA_long_entry) and  #overbought willrema implying bullish trend
           (row[f'willr{tag}']  > willr_long_entry) and  #overbought willr implying bullish trend
           ((row[f'willr_ema{tag}'] - willr_threshold) > row[f'willr_ema_prev{tag}']) and  #more overbought than before
           ((row['close']-row['bband_20_low']) > (bband_entry*(row['bband_20_high']-row['bband_20_low']))) and #BBAND_ENTRY
           not self.position > 0):
            self.position_open_state = 'long_open'
        elif ((row[f'willr_ema{tag}'] < willrEMA_short_entry) and  #oversold willrema implying bearish trend
             (row[f'willr{tag}'] < willr_short_entry) and  #oversold willr implying bearish trend
             ((row[f'willr_ema{tag}'] + willr_threshold) < row[f'willr_ema_prev{tag}']) and  #more overshold than before
             ((row['bband_20_high'] - row['close']) > (bband_entry*(row['bband_20_high']-row['bband_20_low']))) and #BBAND_ENTRY
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
               ((row['datetime'] - self.time_opened) > timestop)): #timestop
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
               ((row['datetime'] - self.time_opened) > timestop)): #timestop
                # Short close
                self.position = 0
                settings = (row['datetime'], 'Short', 'Close', row['close'])
                self._execute_trade(settings)
                return True
        return False

    def _execute_trade_all_params_runprofits(self, row):
        """
        Modified trade logic.
        inclues willrema_long/short entry, willr_long/short_entry, willrema_diff_threshold
        and stoploss and timestop AND CLOSE LONG WHEN PRICE CROSSES UNDER HIGH BBAND AND VV

		Arguments
		---------
		row (dict):     candle update, converted from last row of self.data[0] df

		Returns
		---------
		(bool):         True if a position was closed, else False

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
            if (row[self.cross_sell_open_col] or #RUN PROFITS
               (stoploss > 0 and row['close'] < self.open_price*(1-stoploss)) or  #stoploss
               ((row['datetime'] - self.time_opened) > timestop)): #timestop
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
            if (row[self.cross_buy_open_col] or #RUN PROFITS
               (stoploss > 0 and row['close'] > self.open_price*(1+stoploss)) or  #stoploss
               ((row['datetime'] - self.time_opened) > timestop)): #timestop
                # Short close
                self.position = 0
                settings = (row['datetime'], 'Short', 'Close', row['close'])
                self._execute_trade(settings)
                return True
        return False

    def _execute_trade_all_params_bband_entry_runprofits(self, row):
        """
        Modified trade logic.
        inclues willrema_long/short entry, willr_long/short_entry, willrema_diff_threshold
        and stoploss and timestop AND bband_entry

		Arguments
		---------
		row (dict):     candle update, converted from last row of self.data[0] df

		Returns
		---------
		(bool):         True if a position was closed, else False

        """
        tag = ''
        if self.cfg['floating_willr']:
            tag = f"_{self.cfg['series'][1][1]}_float"

        willr_threshold = self.cfg['willrema_diff_threshold']
        willrEMA_long_entry = self.cfg['willrema_long_entry'] #-100 will ignore threshold
        willrEMA_short_entry = self.cfg['willrema_short_entry'] #0 will ignore threshold
        willr_long_entry = self.cfg['willr_long_entry'] #-100 will ignore threshold
        willr_short_entry = self.cfg['willr_short_entry'] #0 will ignore threshold

        bband_entry = self.cfg['bband_entry']

        timestop =  self.cfg['timestop']
        stoploss =  self.cfg['stoploss']

        if ((row[f'willr_ema{tag}']  > willrEMA_long_entry) and  #overbought willrema implying bullish trend
           (row[f'willr{tag}']  > willr_long_entry) and  #overbought willr implying bullish trend
           ((row[f'willr_ema{tag}'] - willr_threshold) > row[f'willr_ema_prev{tag}']) and  #more overbought than before
           ((row['close']-row['bband_20_low']) > (bband_entry*(row['bband_20_high']-row['bband_20_low']))) and #BBAND_ENTRY
           not self.position > 0):
            self.position_open_state = 'long_open'
        elif ((row[f'willr_ema{tag}'] < willrEMA_short_entry) and  #oversold willrema implying bearish trend
             (row[f'willr{tag}'] < willr_short_entry) and  #oversold willr implying bearish trend
             ((row[f'willr_ema{tag}'] + willr_threshold) < row[f'willr_ema_prev{tag}']) and  #more overshold than before
             ((row['bband_20_high'] - row['close']) > (bband_entry*(row['bband_20_high']-row['bband_20_low']))) and #BBAND_ENTRY
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
            if (row[self.cross_sell_open_col] or #RUN PROFITS
               (stoploss > 0 and row['close'] < self.open_price*(1-stoploss)) or  #stoploss
               ((row['datetime'] - self.time_opened) > timestop)): #timestop
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
            if (row[self.cross_buy_open_col] or #RUN PROFITS
               (stoploss > 0 and row['close'] > self.open_price*(1+stoploss)) or  #stoploss
               ((row['datetime'] - self.time_opened) > timestop)): #timestop
                # Short close
                self.position = 0
                settings = (row['datetime'], 'Short', 'Close', row['close'])
                self._execute_trade(settings)
                return True
        return False

    def _execute_trade_all_params_takeprofits(self, row):
        """
        Modified trade logic.
        inclues willrema_long/short entry, willr_long/short_entry, willrema_diff_threshold
        and stoploss and timestop

		Arguments
		---------
		row (dict):     candle update, converted from last row of self.data[0] df

		Returns
		---------
		(bool):         True if a position was closed, else False

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
        takeprofit = self.cfg['takeprofit']

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
               (row['close'] > self.open_price*(1+takeprofit)) or
               (stoploss > 0 and row['close'] < self.open_price*(1-stoploss)) or  #stoploss
               ((row['datetime'] - self.time_opened) > timestop)): #timestop
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
               (row['close'] < self.open_price*(1-takeprofit)) or
               (stoploss > 0 and row['close'] > self.open_price*(1+stoploss)) or  #stoploss
               ((row['datetime'] - self.time_opened) > timestop)): #timestop
                # Short close
                self.position = 0
                settings = (row['datetime'], 'Short', 'Close', row['close'])
                self._execute_trade(settings)
                return True
        return False

    def _execute_trade_all_params_bbandhack(self, row):
        """
        Modified trade logic.
        inclues willrema_long/short entry, willr_long/short_entry, willrema_diff_threshold
        and stoploss and timestop

		Arguments
		---------
		row (dict):     candle update, converted from last row of self.data[0] df

		Returns
		---------
		(bool):         True if a position was closed, else False

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
               (row[self.cross_sell_close_col]) or #bbandhack
               (stoploss > 0 and row['close'] < self.open_price*(1-stoploss)) or  #stoploss
               ((row['datetime'] - self.time_opened) > timestop)): #timestop
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
               (row[self.cross_buy_close_col]) or #bbandhack
               (stoploss > 0 and row['close'] > self.open_price*(1+stoploss)) or  #stoploss
               ((row['datetime'] - self.time_opened) > timestop)): #timestop
                # Short close
                self.position = 0
                settings = (row['datetime'], 'Short', 'Close', row['close'])
                self._execute_trade(settings)
                return True
        return False

    def _execute_trade_all_params_bband_entry_runprofits_trailing(self, row):
        """
        Modified trade logic.
        inclues willrema_long/short entry, willr_long/short_entry, willrema_diff_threshold
        and stoploss and timestop AND bband_entry AND trailing stop for profits once close is OUTSIDE of bbands

		Arguments
		---------
		row (dict):     candle update, converted from last row of self.data[0] df

		Returns
		---------
		(bool):         True if a position was closed, else False

        """
        tag = ''
        if self.cfg['floating_willr']:
            tag = f"_{self.cfg['series'][1][1]}_float"

        willr_threshold = self.cfg['willrema_diff_threshold']
        willrEMA_long_entry = self.cfg['willrema_long_entry'] #-100 will ignore threshold
        willrEMA_short_entry = self.cfg['willrema_short_entry'] #0 will ignore threshold
        willr_long_entry = self.cfg['willr_long_entry'] #-100 will ignore threshold
        willr_short_entry = self.cfg['willr_short_entry'] #0 will ignore threshold

        bband_entry = self.cfg['bband_entry']
        bband_stop_profit = self.cfg['bband_stop_profit']

        timestop =  self.cfg['timestop']
        stoploss =  self.cfg['stoploss']

        if ((row[f'willr_ema{tag}']  > willrEMA_long_entry) and  #overbought willrema implying bullish trend
           (row[f'willr{tag}']  > willr_long_entry) and  #overbought willr implying bullish trend
           ((row[f'willr_ema{tag}'] - willr_threshold) > row[f'willr_ema_prev{tag}']) and  #more overbought than before
           ((row['close']-row['bband_20_low']) > (bband_entry*(row['bband_20_high']-row['bband_20_low']))) and #BBAND_ENTRY
           not self.position > 0):
            self.position_open_state = 'long_open'
        elif ((row[f'willr_ema{tag}'] < willrEMA_short_entry) and  #oversold willrema implying bearish trend
             (row[f'willr{tag}'] < willr_short_entry) and  #oversold willr implying bearish trend
             ((row[f'willr_ema{tag}'] + willr_threshold) < row[f'willr_ema_prev{tag}']) and  #more overshold than before
             ((row['bband_20_high'] - row['close']) > (bband_entry*(row['bband_20_high']-row['bband_20_low']))) and #BBAND_ENTRY
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
            #set stop_profit
            if not self.long_stop_profit:
                if row['close']> (row['bband_20_low'] + bband_stop_profit*(row['bband_20_high']-row['bband_20_low'])): #DEFINE STOPPROFIT
                    self.long_stop_profit = True
                    self.long_stop_profit_price = row['close']
            elif self.long_stop_profit:
                if row['close'] > self.long_stop_profit_price:
                    self.long_stop_profit_price = row['close']
            if (self.long_stop_profit and (row['close']< self.long_stop_profit_price) or #stop profit
               (stoploss > 0 and row['close'] < self.open_price*(1-stoploss)) or  #stoploss
               ((row['datetime'] - self.time_opened) > timestop)): #timestop
                # Long close
                self.position = 0
                settings = (row['datetime'], 'Long', 'Close', row['close'])
                self._execute_trade(settings)

                #reset self.long_stop_profit
                self.long_stop_profit = False
                self.long_stop_profit_price = 0
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
            #set stop_profit
            if not self.short_stop_profit:
                if row['close'] < (row['bband_20_low']+ (1-bband_stop_profit)*(row['bband_20_high']-row['bband_20_low'])):
                    self.short_stop_profit = True
                    self.short_stop_profit_price = row['close']
            elif self.short_stop_profit:
                if row['close'] < self.short_stop_profit_price:
                    self.short_stop_profit_price = row['close']
            if (self.short_stop_profit and (row['close'] >  self.short_stop_profit_price) or #stop profit
               (stoploss > 0 and row['close'] > self.open_price*(1+stoploss)) or  #stoploss
               ((row['datetime'] - self.time_opened) > timestop)): #timestop
                # Short close
                self.position = 0
                settings = (row['datetime'], 'Short', 'Close', row['close'])
                self._execute_trade(settings)

                #reset self.long_stop_profit
                self.short_stop_profit = False
                self.short_stop_profit_price = float('inf')
                return True
        return False

    def table_to_postgres(self):
        #get postprocess
        df_pp = pd.read_csv('logs/postprocess.csv',sep='\s*,\s*', engine = 'python')
        #keep only relevant columns
        cols_of_interest = ['datetime',
        	                'open',
                            'high',
                            'low',
                            'close',
                            'volume',
                            'bband_20_low',
                            'bband_20_low_prev',
                            'bband_20_high',
                            'bband_20_high_prev',
                            'bband_20_mid',
                            'rsi',
                            'chaikin_osc',
                            'macd',
                            'macdsignal',
                            'macdhist',
                            'lb_chg',
                            'bband_range_perc',
                            'willr_ema',
                            'willr_ema_prev',
                            'willr_' + str(self.cfg['series'][1][1]) + '_float',
                            'willr_ema_' + str(self.cfg['series'][1][1]) + '_float',
                            'willr_ema_prev_' + str(self.cfg['series'][1][1]) + '_float',
                            'crossover:close-bband_20_low',
                            'crossover:close-bband_20_high',
                            'crossunder:close-bband_20_high',
                            'crossunder:close-bband_20_low'
                            ]
        df_pp = df_pp[cols_of_interest]
        #get trades
        df_trades= pd.read_csv('logs/backtesting_trades.csv',sep='\s*,\s*', engine = 'python')
        #create column 'datetime' in df_trades to later merge with df_pp
        df_trades['datetime'] = df_trades['time_candle']
        #delete rows of no trades, ie sys startup and reindex
        df_trades = df_trades.dropna(subset=['time_candle'])
        df_trades.index =range(len(df_trades))
        #get only last backtest to df since orig file is appended
        last_index = df_trades.apply(pd.Series.last_valid_index)['symbol']
        df_trades = df_trades.loc[last_index:,:]
        df_trades.index =range(len(df_trades))

        #join postprocess and trades
        df_merged = pd.merge(df_trades, df_pp, on = 'datetime', how = 'outer')
        df_merged = df_merged.sort_values(by=['datetime'])
        df_merged = df_merged[:-1]
        df_merged = df_merged.reset_index(drop=True)

        #df_merged.to_csv('logs/df_merged_pre.csv')

        #get running token_position upl, rpl, equity
        #created all new columns with initial value 0
        df_merged['token_position'] = 0
        df_merged['equity']=self.cfg['start_capital']
        df_merged['upl'] = 0
        df_merged['rpl'] = 0
        df_merged['fees'] = 0
        open_size = 0
        last_pos = 0
        last_settled_bal = self.cfg['start_capital']
        fee = .0004 #change to cfg
        token_position_list = [0]
        upl_list=[0]
        rpl_list = [0]
        equity_list = [0]

        for i in range(1, len(df_merged)):
            position = df_merged.loc[i,'position']
            action = df_merged.loc[i,'action']
            price = df_merged.loc[i,'price']
            equity = df_merged.loc[i,'equity']
            close = df_merged.loc[i, 'close']

            if action == 'Open':
                if position == 'Long':
                    token_pos_i = round(last_settled_bal/price + last_pos,5)
                    open_size = token_pos_i
                    last_pos = open_size
                    open_price = price
                    fees_i = -abs(open_size*price*fee)
                elif position == 'Short':
                    token_pos_i = round(-last_settled_bal/price + last_pos,5)
                    open_size = token_pos_i
                    last_pos = open_size
                    open_price = price
                    fees_i = -abs(open_size*price*fee)
            elif action == 'Close':
                token_pos_i = -open_size + last_pos
                last_pos = 0
                fees_i = -abs(open_size*price*fee)
                #print ('CLOSE', open_size, price, fee, fees_i)
                #time.sleep(30)
            elif pd.isnull(action):
                token_pos_i = last_pos
                fees_i = 0

            #get upl
            if token_pos_i != 0:
                upl_i = (close - open_price)*open_size
            else:
                upl_i = 0

            #get rpl
            if action == 'Close':
                rpl_i = (close - open_price)*open_size
            else:
                rpl_i = 0

            #get equity
            equity_i = last_settled_bal + upl_i + rpl_i + fees_i
            if action == 'Close':
                last_settled_bal = equity_i
            if action == 'Open':
                last_settled_bal = equity_i

            token_position_list.append(token_pos_i)
            upl_list.append(upl_i)
            rpl_list.append(rpl_i)
            equity_list.append(equity_i)

        #input lists into columns of df
        df_merged['token_position'] = token_position_list
        df_merged['upl'] = upl_list
        df_merged['rpl'] = rpl_list
        df_merged['equity'] = equity_list
        #df_merged.to_csv('WIP.csv')

        #write to csv
        df_merged.to_csv('logs/df_merged.csv')

        #csv to postgres

    def upload_to_postgres(self):

        user = 'postgres'
        password = 'backtesting'
        host = 'ec2-44-234-65-8.us-west-2.compute.amazonaws.com'
        port ='5432'
        dbname ='backtesting'
        upload_file_name = 'logs/df_merged.csv'
        #table_name identifies BT run. timenow_token
        bt_time_ran = int(time.time())
        table_name = str(bt_time_ran) + '_' + str(self.cfg['symbol'][0])
        print ('table name', table_name)


        dbUrl = f"""postgresql://{user}:{password}@{host}:{port}/{dbname}"""
        print(dbUrl)
        engine = create_engine(dbUrl)
        dtype_dict = {'position':str,'action':str}
        data_frame = pd.read_csv(upload_file_name, dtype=dtype_dict)
        #data_frame['ts'] = pd.to_datetime(data_frame['ts'], unit='s')
        data_frame.rename(columns={'Unnamed: 0': 'index'}, inplace=True)
        data_frame.to_sql(
            table_name,
            engine,
            index=False,
            if_exists='append',
            chunksize=1000
        )
        print("Successfully uploaded the data")
        try:
            query = f"""SELECT create_hypertable('{table_name}', 'datetime' ,chunk_time_interval => INTERVAL '1 day',migrate_data => true, if_not_exists => true);"""
            with engine.connect() as connection:
                result = connection.execute(text(query).execution_options(autocommit=True))
                for row in result:
                    print(row[0])
        except:
            print("but failed to create hypertable")


class LiveWillRBbandEvo(LiveWillRBband, WillRBbandEvo):
    pass

