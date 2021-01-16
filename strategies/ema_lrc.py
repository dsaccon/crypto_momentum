import datetime as dt
import operator
import backtrader as bt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import btalib
from talib.abstract import LINEARREG, EMA

from .base import BacktestingBaseClass


class EmaLrc(BacktestingBaseClass):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.col_tags = {
            'long_cross': ('lrc', 'ema'),
            'short_cross': ('lrc', 'ema'),
        }
        self.order_size = 1
        self.pnl_mod = None

    def preprocess_data(self):

        # tags for crossover cols added to pd dataframe
        self.cross_buy_col = f"crossover:lrc-ema"
        self.cross_sell_col = f"crossunder:lrc-ema"

        # ..load 60m data (index=1)
        i = 0
        #self.data[i].reset_index(inplace=True) # Adapt df, otherwise problems running ema
        #self.data[i]['lrc'] = btalib.linearreg(self.data[i]['close'], period=self.cfg['lrc_periods']).df
        lrc_np = LINEARREG(self.data[i]['close'], timeperiod=self.cfg['num_periods'][1])
        self.data[i]['lrc'] = lrc_np
        #self.data[i]['lrc'] = LINEARREG(self.data[i]['close'], timeperiod=self.cfg['lrc_periods']).df
        ema_np = EMA(self.data[i]['close'], timeperiod=self.cfg['num_periods'][0])
        self.data[i]['ema'] = ema_np
        #self.data[i]['ema'] = btalib.ema(self.data[i]['close'], period=self.cfg['ema_periods'], _seed=3).df

        self.data[i]['lrc_prev'] = self.data[i]['lrc'].shift(1)
        self.data[i]['ema_prev'] = self.data[i]['ema'].shift(1)

        # For Long
        self.get_crosses('lrc', 'ema', i)

        # For Short
        self.get_crosses('lrc', 'ema', i, over=False)

    def run(self):
        super().run()
        #
        for i, row in self.data[0].iterrows():
            if row[self.cross_buy_col] and not self.position > 0:
                self.trades.append(('Buy', row['close']))
                self.position = 1
            elif row[self.cross_sell_col] and not self.position < 0:
                self.trades.append(('Sell', row['close']))
                self.position = -1

        self.calc_pnl()
        self.calc_pnl_mod() 
        print('--', self.trades)
        start_cap = self.cfg['start_capital']
        print(
            'pnl', self.pnl,
            f'{round((self.pnl/start_cap)*100, 2)}%',
            'num trades', len(self.trades),
            'dataframe', self.data[0].shape)
        print('pnl ($), as per live trading:', self.pnl_mod)

    def calc_pnl(self):
        fee = 1 - self.exchange.trading_fee
        position = 0
        balance = self.cfg['start_capital']
        for i, trade in enumerate(self.trades):
            if trade[0] == 'Buy':
                pass
            elif trade[0] == 'Sell' and not i == 0:
                balance = ((trade[1] - self.trades[i-1][1])/self.trades[i-1][1] + 1)*balance*fee
        self.pnl = balance - self.cfg['start_capital']

    def calc_pnl_mod(self):
        """
        To follow order sizes as per live trading settings
        """
        fee = 1 - self.exchange.trading_fee
        pnl = 0
        for i, trade in enumerate(self.trades):
            if i == 0:
                continue
            if trade[0] == 'Buy':
                pass
            elif trade[0] == 'Sell' and not i == 0:
                pnl += ((trade[1] - self.trades[i-1][1])/self.trades[i-1][1] + 1)*2*self.order_size*fee
        self.pnl_mod = pnl
