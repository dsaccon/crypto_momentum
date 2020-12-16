import datetime as dt
import operator
import backtrader as bt
import pandas as pd
import numpy as np
import btalib

class BacktestingBaseClass:

    operator_lookup = {
	'>': operator.gt,
	'<': operator.lt,
    }

    def __init__(self, data):
        self.data = data
        self.preprocess_data()
        self.trades = []

    def preprocess_data(self):
        # Customizations to dataset happen here
        pass

    def get_crosses(self, col_1, col_2, over=True):
        op = '>' if over else '<'
        col_name_suffix = 'over' if over else 'under'
        comparator = self.operator_lookup.get(op)
        col_name = f'cross{col_name_suffix}:{col_1}-{col_2}'
        col_idx = [col_1, f'{col_1}_prev', col_2, f'{col_2}_prev']
        self.data[col_name] = self.data[col_idx].apply(
            lambda row: True if (
                    not np.isnan(row[col_1])
                    and not np.isnan(row[f'{col_1}_prev'])
                    and not np.isnan(row[col_2])
                    and not np.isnan(row[f'{col_2}_prev'])
                    and comparator(row[col_1], row[col_2])
                    and not row[f'{col_1}_prev'] > row[f'{col_2}_prev'])
                else False, axis=1)

    def run(self):
        pass

class WilliamsRPriceEMA(BacktestingBaseClass):

    def preprocess_data(self):
        # For Buy entry
        self.data['willr_13'] = btalib.willr(self.data['high'], self.data['low'], self.data['close'], period = 13).df
        self.data['willr_13_ema'] = btalib.ema(self.data['willr_13'], period = 13).df
        self.data['willr_13_ema_prev'] = self.data['willr_13_ema'].shift(1)
        self.data['willr_50'] = btalib.willr(self.data['high'], self.data['low'], self.data['close'], period = 50).df
        self.data['willr_50_prev'] = self.data['willr_50'].shift(1)
        self.get_crosses('willr_13_ema', 'willr_50')

        # For Buy close
        self.data['ema_13'] = btalib.ema(self.data['close'], period = 13).df
        self.data['ema_50'] = btalib.ema(self.data['close'], period = 50).df
        self.data['ema_13_prev'] = self.data['ema_13'].shift(1)
        self.data['ema_50_prev'] = self.data['ema_50'].shift(1)
        self.get_crosses('ema_13', 'ema_50', over=False)

        # For Sell entry
        self.get_crosses('willr_13_ema', 'willr_50', over=False)

        # For Sell close
        self.get_crosses('ema_13', 'ema_50')

    def _create_triggers(self):
        pass

    def _crosses_sanity_check(self):
        #
        for col in self.data.columns.values.tolist():
            if col.startswith('cross'):
                _list = pd.Series(self.data[col]).tolist()
                print(f'{col}: ', (True in _list))
                print(f'{col}: ', (False in _list))
                print('')

    def run(self):
        self._crosses_sanity_check()


class MaCrossStrategy(bt.Strategy):

    def __init__(self):
        ma_fast = bt.ind.SMA(period = 10)
        ma_slow = bt.ind.SMA(period = 50)

        self.crossover = bt.ind.CrossOver(ma_fast, ma_slow)

    def next(self):
        print('---', self.datas[0]) ### tmp
        print(dir(self.datas[0])) ### tmp
        print(self.datas[0].tick_open, self.datas[0].open) ### tmp
        print(self.datas[0].tick_high, self.datas[0].high) ### tmp
        print(self.datas[0].tick_low, self.datas[0].low) ### tmp
        print(self.datas[0].tick_close, self.datas[0].close) ### tmp
        quit()### tmp
        if not self.position:
            if self.crossover > 0:
                self.buy()
        elif self.crossover < 0:
            self.close()


class PrintClose(bt.Strategy):

    def __init__(self):
        #Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('dt', dt) ### tmp
        print(f'{dt.isoformat()} {txt}') #Print date and close

    def next(self):
        self.log('Close: ', self.dataclose[0])


class TestStrategy(bt.Strategy):

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close

    def next(self):
        # Simply log the closing price of the series from the reference
        self.log('Close, %.2f' % self.dataclose[0])


class BtWilliamsRPriceEMA(bt.Strategy):

    def __init__(self):
        self.order = None

        self.ema = bt.ind.MovingAverageExponential(period=50)
        self.williams_r = bt.ind.WilliamsR(period=50)

        close_over_ema = self.datas[0].close > self.ema
        close_under_ema = self.datas[0].close < self.ema

        willr_ema_crossover = bt.ind.CrossOver(self.williams_r, self.ema)
        price_ema_crossover = bt.ind.CrossOver(self.datas[0].close, self.ema)

        self.buy_sig = bt.And(close_over_ema, willr_ema_crossover > 0)
        self.buy_close_sig = price_ema_crossover < 0
        self.sell_sig = bt.And(close_under_ema, willr_ema_crossover < 0)
        self.sell_close_sig = price_ema_crossover > 0

    def next(self):
        #print('Next---', self.buy_sig, self.buy_close_sig, self.sell_sig, self.sell_close_sig) ### tmp
        #print('') ### tmp
        #print('---', self.datas[0].close) ### tmp
        if self.order:
            # Only want to be in one position at a time
            return

        if not self.position:
            # We are not in the market, look for signals to open positions
            if self.buy_sig:
                self.buy()
            elif self.sell_sig:
                self.sell()
        else:
            # We are already in the market, look for signals to close positions
            if self.buy_close_sig or self.sell_close_sig:
                self.close()

    def log(self, txt, dt=None):
            dt = dt or self.datas[0].datetime.date(0)
            print(f'{dt.isoformat()} {txt}')

    def notify_order(self, order):
        print('--order:', order) ### tmp
        if order.status in [order.Submitted, order.Accepted]:
            # An active Buy/Sell order has been submitted/accepted - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED, {order.executed.price:.2f}')
            elif order.issell():
                self.log(f'SELL EXECUTED, {order.executed.price:.2f}')
            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Reset orders
        self.order = None
