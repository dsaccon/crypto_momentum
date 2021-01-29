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

from .base import BacktestingBaseClass

class WillRBband(BacktestingBaseClass):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.execution_mode = 'backtesting'
        self.col_tags = {
            'long_entry_cross': ('close', 'bband_20_low'),
            'long_close_cross': ('close', 'bband_20_high'),
            'short_entry_cross': ('close', 'bband_20_high'),
            'short_close_cross': ('close', 'bband_20_low'),
        }
        self.position_open_state = False # Vals: 'long_open', 'short_open', False
        _execution_type = '_execute_trade_anytime_entry'
        self._on_new_candle = getattr(self, _execution_type)

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
        self.data[i]['bband_20_low'] = btalib.bbands(self.data[i]['close'], period = 20, devs = 2.3).bot
        self.data[i]['bband_20_low_prev'] = self.data[i]['bband_20_low'].shift(1)
        self.data[i]['bband_20_high'] = btalib.bbands(self.data[i]['close'], period = 20, devs = 2.3).top
        self.data[i]['bband_20_high_prev'] = self.data[i]['bband_20_high'].shift(1)
        self.data[i]['close_prev'] = self.data[i]['close'].shift(1)

        # Upsample 60m data to dataframe at index=0
        modulo = int(self.cfg['series'][1][-1])
        for _i, row in self.data[0].iterrows():
            dt_60m = int(_i - _i % modulo)
            self.data[0].at[_i, 'willr_ema'] = self.data[1].at[dt_60m, 'willr_ema']
            self.data[0].at[_i, 'willr_ema_prev'] = self.data[1].at[dt_60m, 'willr_ema_prev']
#        self.data[0]['willr_ema_prev'] = self.data[0]['willr_ema'].shift(1)

        i = 0
        # For Long entry
        self.get_crosses('close', 'bband_20_low', i)

        # For Long close
        self.get_crosses('close', 'bband_20_high', i)

        # For Short entry
        self.get_crosses('close', 'bband_20_high', i, over=False)

        # For Short close
        self.get_crosses('close', 'bband_20_low', i, over=False)

    def _execute_trade(self, trade_settings):
        """
        """
        if trade_settings[1] == 'Long' and trade_settings[2] == 'Open':
            side = 'BUY'
        elif trade_settings[1] == 'Short' and trade_settings[2] == 'Close':
            side = 'BUY'
        elif trade_settings[1] == 'Short' and trade_settings[2] == 'Open':
            side = 'SELL'
        elif trade_settings[1] == 'Long' and trade_settings[2] == 'Close':
            side = 'SELL'
        else:
            raise ValueError

        if self.execution_mode == 'backtesting':
            self.trades.append(trade_settings)
            self._trades[-1] += f'_{trade_settings[1].lower()}_{trade_settings[2].lower()}'
        elif self.execution_mode == 'live':
            self._place_live_order(side)
        else:
            raise ValueError

    def _execute_trade_anytime_entry(self, row):
        """
        Modified trade logic.
        Anytime entry. Attempts to fix issue of position entry only on the first
            ..3m tick of the 60m period
        """
        if row['willr_ema'] > row['willr_ema_prev'] and not self.position > 0:
            self.position_open_state = 'long_open'
        elif row['willr_ema'] < row['willr_ema_prev'] and not self.position < 0:
            self.position_open_state = 'short_open'

        if self.position == 0 and self.position_open_state == 'long_open':
            if row[self.cross_buy_open_col]:
                # Long entry
                #self.trades.append((row['datetime'], 'Long', 'Open', row['close']))
                self.position = 1
                self.position_open_state = False
                #self._trades[-1] += '_long_open' ### tmp
                settings = (row['datetime'], 'Long', 'Open', row['close'])
                self._execute_trade(settings)
        elif self.position > 0 and row[self.cross_buy_close_col]:
                # Long close
                #self.trades.append((row['datetime'], 'Long', 'Close', row['close']))
                self.position = 0
                #self._trades[-1] += '_long_close' ### tmp
                settings = (row['datetime'], 'Long', 'Close', row['close'])
                self._execute_trade(settings)
                return True
        elif self.position == 0 and self.position_open_state == 'short_open':
            if row[self.cross_sell_open_col]:
                # Short entry
                #self.trades.append((row['datetime'], 'Short', 'Open', row['close']))
                self.position = -1
                self.position_open_state = False
                #self._trades[-1] += '_short_open' ### tmp
                settings = (row['datetime'], 'Short', 'Open', row['close'])
                self._execute_trade(settings)
        elif self.position < 0 and row[self.cross_sell_close_col]:
                # Short close
                #self.trades.append((row['datetime'], 'Short', 'Close', row['close']))
                self.position = 0
                #self._trades[-1] += '_short_close' ### tmp
                settings = (row['datetime'], 'Short', 'Close', row['close'])
                self._execute_trade(settings)
                return True
        return False

    def _execute_trade_60m_entry(self, row):
        """
        Original trade logic.
        Only allows position entry on the first 3m tick of the 60m period
        """
        if self.position == 0 and row['willr_ema'] > row['willr_ema_prev']:
            if row[self.cross_buy_open_col]:
                # Long entry
                self.trades.append((row['datetime'], 'Long', 'Open', row['close']))
                self.position = 1
                self._trades[-1] += '_long_open' ### tmp
        elif self.position > 0 and row[self.cross_buy_close_col]:
                # Long close
                self.trades.append((row['datetime'], 'Long', 'Close', row['close']))
                self.position = 0
                self._trades[-1] += '_long_close' ### tmp
                return True
        elif self.position == 0 and row['willr_ema'] < row['willr_ema_prev']:
            if row[self.cross_sell_open_col]:
                # Short entry
                self.trades.append((row['datetime'], 'Short', 'Open', row['close']))
                self.position = -1
                self._trades[-1] += '_short_open' ### tmp
        elif self.position < 0 and row[self.cross_sell_close_col]:
                # Short close
                self.trades.append((row['datetime'], 'Short', 'Close', row['close']))
                self.position = 0
                self._trades[-1] += '_short_close' ### tmp
                return True
        return False

    def _execute_trade_chris(self, row):
        """
        This is an attempt to mimic Chris' logic
        """
        if row['willr_ema'] > row['willr_ema_prev'] and not self.position > 0:
            # Enable long open
            self.long_open = True
            self.short_open = False
        elif row['willr_ema'] < row['willr_ema_prev'] and not self.position < 0:
            # Enable short open
            self.long_open = False
            self.short_open = True

        if self.long_open and row['willr_ema'] > row['willr_ema_prev']:
            if row[self.cross_buy_open_col]:
                # Long entry
                self.trades.append((row['datetime'], 'Long', 'Open', row['close']))
                self.position = 1
                self._trades[-1] += '_long_open' ### tmp
                self.long_open = False
                self.long_close = True
        elif self.long_close and row[self.cross_buy_close_col]:
                # Long close
                self.trades.append((row['datetime'], 'Long', 'Close', row['close']))
                self.position = 0
                self._trades[-1] += '_long_close' ### tmp
                self.long_close = False
                return True
        elif self.short_open and row['willr_ema'] < row['willr_ema_prev']:
            if row[self.cross_sell_open_col]:
                # Short entry
                self.trades.append((row['datetime'], 'Short', 'Open', row['close']))
                self.position = -1
                self._trades[-1] += '_short_open' ### tmp
                self.short_open = False
                self.short_close = True
        elif self.short_close and row[self.cross_sell_close_col]:
                # Short close
                self.trades.append((row['datetime'], 'Short', 'Close', row['close']))
                self.position = 0
                self._trades[-1] += '_short_close' ### tmp
                self.short_close = False
                return True
        return False

    def _debug_output(self):
        self.data[0].to_csv('data/test/test_balances.csv')
        with open('data/test/davids_cross_pos_entry_anytime.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(self.balances)

    def run(self):
        super().run()
        #
        if not self._crosses_sanity_check():
            raise SanityCheckError
        processed_rows = 0
        self._trades = [] ### tmp
        self._POSs = [] ### tmp
        self.long_open = False
        self.long_close = False
        self.short_open = False
        self.short_close = False
        for i, row in self.data[0].iterrows(): # iterate over 3m series
            _row = row.append(pd.Series([i], index=['datetime']))
            self._trades.append('')
            if self._on_new_candle(_row):
                # If a position was closed in this candle, check to re-open new position
                self._on_new_candle(_row)
            self._POSs.append(self.position_open_state) ### tmp

        self.data[0]['position_open_state'] = self._POSs
        self.data[0]['trades'] = self._trades
        self.calc_pnl()
        # Add balances to dataframe
        balances_dt = [b[0] for b in self.balances]
        bals = [
            self.balances[balances_dt.index(i)][1]
            if i in balances_dt else None
            for i in self.data[0].index
        ]
        self.data[0]['balances'] = bals
        self.data[0]['date'] = [
            dt.datetime.fromtimestamp(i).strftime('%Y-%m-%d:%H-%M-%S')
            for i in self.data[0].index
        ]

        self.data[0].fillna(method='ffill').plot(x='date', y='balances')
        plt.show()

        #self._debug_output()

        self.logger.debug(f'--, {self.trades}')
        self.logger.debug(f'Processed rows: {processed_rows}')
        self.logger.debug(
            f'pnl: {self.pnl}, ending capital: {self.end_capital}'
            f' {round((self.pnl/self.cfg["start_capital"])*100, 2)}%'
            f' num trades: {len(self.trades)}'
            f' dataframe: {self.data[0].shape}')

class LiveWillRBband(WillRBband):

    MAX_PERIODS = (20, 43) # Corresponding to (3m, 60m) data series

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.execution_mode = 'live'

    def _get_latest_candle(self, i):
        """
        API call + checks to fetch new candles as they become available
        """
        now = dt.datetime.now()
        latest_data_idx = int(self.data[i].index[-1])
        period = self.cfg['series'][i][2]
        if now.timestamp() - latest_data_idx > period + 5:
            # Get updated candle from exchange
            start = dt.datetime.fromtimestamp(now.timestamp() - 2*period)
            new_candle = self.exchange.get_backtest_data(
                self.cfg['symbol'][0]+self.cfg['symbol'][1],
                period,
                start,
                now)
            idx = int(new_candle.iloc[-1]['datetime'])
            if not idx == latest_data_idx + period:
                self.logger.debug(f'now: {dt.datetime.now().timestamp()}')
                self.logger.debug(f'last candle: {new_candle.iloc[-1]["datetime"]}')
                self.logger.debug(f'{latest_data_idx}, {period}')
                raise Exception

            row = {c:None for c in self.data[i].columns}
            self.data[i].loc[idx] = row
            self.data[i].at[idx, 'open'] = new_candle.iloc[-1]['open']
            self.data[i].at[idx, 'high'] = new_candle.iloc[-1]['high']
            self.data[i].at[idx, 'low'] = new_candle.iloc[-1]['low']
            self.data[i].at[idx, 'close'] = new_candle.iloc[-1]['close']
            return True
        else:
            return False

    def _live_accounting(self, order_id):
        """
        For live trading, dump trade to csv
        """
        symbol = self.cfg['symbol'][0] + self.cfg['symbol'][1]
        trade_status = self.exchange.order_status(symbol=symbol, order_id=order_id)
        bals = self.exchange.get_balances()
        # row: (price, qty, base_bal, quote_bal)
        row = (
            trade_status['price'],
            trade_status['quantity'],
            bals[self.cfg['symbol'][0]],
            bals[self.cfg['symbol'][1]])

        ### Need to process trade_status here
        write_mode = 'a'
        if not os.path.isfile('data/live_trades.csv'):
            write_mode = 'w'
        with open(f'data/live_trades.csv', write_mode, newline='') as f:
            writer = csv.writer(f)
            writer.writerows(row)

    def _live_trade_size(self, side):
        """
        For live trading, calc max trade size allowed based on balance
        ..from API and current order book
        """
        bals = self.exchange.get_balances()
        symbol = self.cfg['symbol'][0] + self.cfg['symbol'][1]
        book = self.exchange.get_book(symbol=symbol)
        sig_digs = 5 # significant digits
        round_down = lambda x: int(x*10**sig_digs)/10**sig_digs
        adjuster = 5*round_down(1/10**sig_digs)

        if side.upper() == 'SELL':
            # Round down at sd decimals
            #size = int(bals[self.cfg['symbol'][0]]*10**sd)/10**sd
            size = round_down(bals[self.cfg['symbol'][0]])
            #size = bals[self.cfg['symbol'][0]]/float(book['bids'][0][0])
        elif side.upper() == 'BUY':
            #size = bals[self.cfg['symbol'][0]]/float(book['asks'][0][0])
            # Round down at sd decimals
            size = bals[self.cfg['symbol'][1]]/float(book['asks'][0][0])
            size = size*(1 - self.exchange.trading_fee)
            #size = int(size*10**sd)/10**sd
            size = round_down(size) - adjuster

        return size

    def _place_live_order(self, side):
        size = self._live_trade_size(side)
        symbol = self.cfg['symbol'][0] + self.cfg['symbol'][1]
        self.logger.debug(f'placing order: symbol {symbol}, side {side}, size {size}') ### tmp
        order_id = self.exchange.place_order(symbol, side, size)
        self._live_accounting(order_id)

    def run(self):
        self.preprocess_data()

        while True:
            # Periodically update candles from API
            if self._get_latest_candle(0): # Adds 3m candles
                self._get_latest_candle(1) # Adds 60m candles, hourly
                self.preprocess_data()
                #self._on_new_candle(self.data[0].iloc[-1])
                row = self.data[0].iloc[-1]
                idx = self.data[0].index[-1]
                row = row.append(pd.Series([idx], index=['datetime']))
                self._on_new_candle(row)


                self.logger.debug(self.data[0])
                self.logger.debug('')
                self.logger.debug(self.data[1])
            else:
                time.sleep(1)
                if int(str(int(dt.datetime.now().timestamp()))[-1]) % 9 == 0:
                    remaining = self.cfg['series'][0][2] - dt.datetime.now().timestamp() % self.cfg['series'][0][2]
                    self.logger.debug(f"Next candle in {remaining}s")
