import datetime as dt
import operator
import backtrader as bt
import pandas as pd
import numpy as np
import btalib

class SanityCheckError(Exception):
    pass

class UnknownStateError(Exception):
    pass

class BacktestingBaseClass:

    data_cfg = tuple()
    operator_lookup = {
	'>': operator.gt,
	'<': operator.lt,
    }

    def __init__(self, data, exchange=False):
        self.data = data
#        self.data = [df.reset_index(drop=True) for df in self.data]
        self.exchange = exchange
        self.trades = []
        self.start_capital = 1000000
        self.end_capital = 1000000
        self.pnl = 0
        self.position = 0
        self.cross_buy_open_col = ''
        self.cross_buy_close_col = ''
        self.cross_sell_open_col = ''
        self.cross_sell_close_col = ''
        self.col_tags = {
            'long_entry_cross': tuple(),
            'long_close_cross': tuple(),
            'short_entry_cross': tuple(),
            'short_close_cross': tuple(),
        }

    def preprocess_data(self):
        # Add new cols to dataframe necessary to do calcs in run()

        self.cross_buy_open_col = f"crossover:{self.col_tags['long_entry_cross'][0]}-{self.col_tags['long_entry_cross'][1]}"
        self.cross_buy_close_col = f"crossunder:{self.col_tags['long_close_cross'][0]}-{self.col_tags['long_close_cross'][1]}"
        self.cross_sell_open_col = f"crossunder:{self.col_tags['short_entry_cross'][0]}-{self.col_tags['short_entry_cross'][1]}"
        self.cross_sell_close_col = f"crossover:{self.col_tags['short_close_cross'][0]}-{self.col_tags['short_close_cross'][1]}"

    def get_crosses(self, col_1, col_2, i, over=True):
        op = '>' if over else '<'
        col_name_suffix = 'over' if over else 'under'
        comparator = self.operator_lookup.get(op)
        col_name = f'cross{col_name_suffix}:{col_1}-{col_2}'
        col_idx = [col_1, f'{col_1}_prev', col_2, f'{col_2}_prev']
        self.data[i][col_name] = self.data[i][col_idx].apply(
            lambda row: True if (
                    not np.isnan(row[col_1])
                    and not np.isnan(row[f'{col_1}_prev'])
                    and not np.isnan(row[col_2])
                    and not np.isnan(row[f'{col_2}_prev'])
                    and comparator(row[col_1], row[col_2])
                    and not row[f'{col_1}_prev'] > row[f'{col_2}_prev'])
                else False, axis=1)

    def run(self):
        self.preprocess_data()

    def calc_pnl(self):
        fee = 1 - self.exchange.trading_fee
        position = 0
        balance = self.start_capital
        for trade in self.trades:
            if position == 0:
                if trade[1] == 'Open':
                    if trade[0] == 'Buy':
                        position = trade[2]
                    if trade[0] == 'Sell':
                        position = -trade[2]
                else:
                    print('&&&', trade, position) ### tmp
                    raise SanityCheckError
            else:
                if trade[1] == 'Close':
                    if trade[0] == 'Sell' and position < 0:
                        # Short closing
#                        self.pnl += (position - trade[2])*balance*fee
#                        print('balance before', balance) ### tmp
                        balance = ((-position - trade[2])/-position + 1)*balance*fee
#                        print('calc', (-position - trade[2])/position) ### tmp
#                        print('balance', balance) ### tmp
#                        print('position', position) ### tmp
#                        print('trade 2', trade[2]) ### tmp
#                        print('fee', fee) ### tmp
#                        balance += self.pnl
                    elif trade[0] == 'Buy' and position > 0:
                        # Long closing
#                        self.pnl += (trade[2] - position)*balance*fee
                        balance = ((trade[2] - position)/position + 1)*balance*fee
                        #balance += self.pnl
                    else:
                        print('^^^', trade, position) ### tmp
                        raise SanityCheckError
                    #print('balance:', balance, 'position:', position, 'trade:', trade, 'fee:', fee) ### tmp
                    position = 0
                else:
                    print('***', trade, position) ### tmp
                    raise SanityCheckError
        self.end_capital = balance
        self.pnl = self.end_capital - self.start_capital
        print('end capital', self.end_capital) ### tmp
        print('start capital', self.start_capital) ### tmp
        print('fee', fee) ### tmp
        print('') ### tmp

    def _crosses_sanity_check(self):
        #
        for _, _df in enumerate(self.data):
            for col in _df.columns.values.tolist():
                if col.startswith('cross'):
                    _list = pd.Series(_df[col]).tolist()
                    if not True in _list:
                        return False
                    print(f'{col}: {sum(_ == True for _ in _list)} crosses, out of {len(_list)} rows')
        return True


class WillRBband(BacktestingBaseClass):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.col_tags = {
            'long_entry_cross': ('close', 'bband_20_low'),
            'long_close_cross': ('close', 'bband_20_high'),
            'short_entry_cross': ('close', 'bband_20_high'),
            'short_close_cross': ('close', 'bband_20_low'),
        }
        #self.data.append(pd.DataFrame()) # Add new cols to DataFrame in last list element

    def preprocess_data(self):
        super().preprocess_data()

        # ..load 60m data (index=1)
        i = 1
        self.data[i]['willr'] = btalib.willr(self.data[i]['high'], self.data[i]['low'], self.data[i]['close'], period = 14).df
        self.data[i]['willr_ema'] = btalib.ema(self.data[i]['willr'], period = 43, _seed = 3).df
        self.data[i]['willr_ema_prev'] = self.data[i]['willr_ema'].shift(1)

        # ..load 3m data (index=0)
        i = 0
        self.data[i]['bband_20_low'] = btalib.bbands(self.data[i]['close'], period = 20, devs = 2.3).bot
        self.data[i]['bband_20_low_prev'] = self.data[i]['bband_20_low'].shift(1)
        self.data[i]['bband_20_high'] = btalib.bbands(self.data[i]['close'], period = 20, devs = 2.3).top
        self.data[i]['bband_20_high_prev'] = self.data[i]['bband_20_high'].shift(1)
        self.data[i]['close_prev'] = self.data[i]['close'].shift(1)

        i = 0
        # For Long entry
        self.get_crosses('close', 'bband_20_low', i)

        # For Long close
        self.get_crosses('close', 'bband_20_high', i)

        # For Short entry
        self.get_crosses('close', 'bband_20_high', i, over=False)

        # For Short close
        self.get_crosses('close', 'bband_20_low', i, over=False)


    def run(self):
        super().run()
        #
        if not self._crosses_sanity_check():
            raise SanityCheckError
        processed_rows = 0
        skipped_rows = 0
        for i_3m, row_0 in self.data[0].iterrows(): # iterate over 3m series
            #if i_3m % 20 == 0:
            #row_1 = self.data[1].iloc[int(i_3m/20)] # corresponding 60m row
            row_1 = (self.data[1].loc[self.data[0]['datetime'] == row_0.datetime])
            if row_1.empty:
                # End of 60m series
                break
            if any([
                    row_1.isna().willr_ema.iloc[0],
                    row_1.isna().willr_ema_prev.iloc[0]]):
                # Skip first row before 60m' first candle
                print(f'Skipping {row_1.datetime.iloc[0]}')
                skipped_rows += 1
                continue
            processed_rows += 1
            # Only progress 60m series iteration when ts' line up
            if self.position == 0 and row_1['willr_ema'].iloc[0] > row_1['willr_ema_prev'].iloc[0]:
                if row_0[self.cross_buy_open_col]:
                    # Long entry
                    self.trades.append(('Buy', 'Open', row_0['close']))
                    self.position = 1
            elif self.position > 0 and row_0[self.cross_buy_close_col]:
                    # Long close
                    self.trades.append(('Buy', 'Close', row_0['close']))
                    self.position = 0
            if self.position == 0 and row_1['willr_ema'].iloc[0] < row_1['willr_ema_prev'].iloc[0]:
                if row_0[self.cross_sell_open_col]:
                    # Short entry
                    self.trades.append(('Sell', 'Open', row_0['close']))
                    self.position = -1
            elif self.position < 0 and row_0[self.cross_sell_close_col]:
                    # Short close
                    self.trades.append(('Sell', 'Close', row_0['close']))
                    self.position = 0

        print('--', self.trades)
        print('Processed rows', processed_rows)
        print('Skipped rows', skipped_rows)
        self.calc_pnl()
        print(
            'pnl', self.pnl, 'ending capital', self.end_capital,
            f'{round((self.pnl/self.start_capital)*100, 2)}%',
            'num trades', len(self.trades),
            'dataframe', self.data[0].shape)


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
        self.data[0]['willr'] = btalib.willr(self.data[0]['high'], self.data[0]['low'], self.data[0]['close'], period = 14).df
        self.data[0]['willr_13_ema'] = btalib.ema(self.data[0]['willr'], period = 13).df
        self.data[0]['willr_13_ema_prev'] = self.data[0]['willr_13_ema'].shift(1)
        self.data[0]['willr_50_ema'] = btalib.ema(self.data[0]['willr'], period = 50).df
        self.data[0]['willr_50_ema_prev'] = self.data[0]['willr_50_ema'].shift(1)
        self.get_crosses('willr_13_ema', 'willr_50_ema', 0)
#        self.cross_buy_open_col = 'crossover:willr_13_ema-willr_50_ema'

        # For Long close
        self.data[0]['ema_13'] = btalib.ema(self.data[0]['close'], period = 13).df
        self.data[0]['ema_50'] = btalib.ema(self.data[0]['close'], period = 50).df
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
            f'{round((self.pnl/self.start_capital)*100, 2)}%',
            'num trades', len(self.trades),
            'dataframe', self.data[0].shape)


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
