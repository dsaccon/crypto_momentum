import datetime as dt
import operator
import backtrader as bt
import pandas as pd
import numpy as np
import btalib

from .base import BacktestingBaseClass

class WillREma(BacktestingBaseClass):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.col_tags = {
            'long_entry_cross': ('willr_13_ema', 'willr_50_ema'),
            'long_close_cross': ('ema_13', 'ema_50'),
            'short_entry_cross': ('willr_13_ema', 'willr_50_ema'),
            'short_close_cross': ('ema_13', 'ema_50'),
        }
        #self.data = self.data[0] # Strat only uses one series

    def preprocess_data(self):
        super().preprocess_data()

        # For Long entry
        self.data[0]['willr'] = btalib.willr(self.data[0]['high'], self.data[0]['low'], self.data[0]['close'], period=14).df
        self.data[0]['willr_13_ema'] = btalib.ema(self.data[0]['willr'], period=13, _seed=3).df
        self.data[0]['willr_13_ema_prev'] = self.data[0]['willr_13_ema'].shift(1)
        self.data[0]['willr_50_ema'] = btalib.ema(self.data[0]['willr'], period=50, _seed=3).df
        self.data[0]['willr_50_ema_prev'] = self.data[0]['willr_50_ema'].shift(1)
        self.get_crosses('willr_13_ema', 'willr_50_ema', 0)
#        self.cross_buy_open_col = 'crossover:willr_13_ema-willr_50_ema'

        # For Long close
        self.data[0]['ema_13'] = btalib.ema(self.data[0]['close'], period=13, _seed=3).df
        self.data[0]['ema_50'] = btalib.ema(self.data[0]['close'], period=50, _seed=3).df
        self.data[0]['ema_13_prev'] = self.data[0]['ema_13'].shift(1)
        self.data[0]['ema_50_prev'] = self.data[0]['ema_50'].shift(1)
        self.get_crosses('ema_13', 'ema_50', 0, over=False)
#        self.cross_buy_close_col = 'crossunder:ema_13-ema_50'

        # For Short entry
        self.get_crosses('willr_13_ema', 'willr_50_ema', 0, over=False)
#        self.cross_sell_open_col = 'crossunder:willr_13_ema-willr_50_ema'

        # For Short close
        self.get_crosses('ema_13', 'ema_50', 0)
#        self.cross_sell_close_col = 'crossover:ema_13-ema_50'

    def run(self):
        super().run()
        #
        if not self._crosses_sanity_check():
            raise SanityCheckError
        for i, row in self.data[0].iterrows():
            if self.position == 0 and row['close'] > row['ema_13']:
                if row[self.cross_buy_open_col]:
                    # Long entry
                    self.trades.append(('Buy', 'Open', row['close']))
                    self.position = 1
            elif self.position > 0 and row[self.cross_buy_close_col]:
                    # Long close
                    self.trades.append(('Sell', 'Close', row['close']))
                    self.position = 0
            if self.position == 0 and row['close'] < row['ema_13']:
                if row[self.cross_sell_open_col]:
                    # Short entry
                    self.trades.append(('Sell', 'Open', row['close']))
                    self.position = -1
            elif self.position < 0 and row[self.cross_sell_close_col]:
                    # Short close
                    self.trades.append(('Sell', 'Close', row['close']))
                    self.position = 0
        self.calc_pnl()
        print(
            'pnl', self.pnl,
            f'{round((self.pnl/self.cfg["start_capital"])*100, 2)}%',
            'num trades', len(self.trades),
            'dataframe', self.data[0].shape)
