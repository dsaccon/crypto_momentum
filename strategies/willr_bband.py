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

from utils.s3 import write_s3
from .base import BacktestingBaseClass

from utils.sns import SNS_call ### tmp

class ApplicationStateError(Exception):
        pass


class WillRBband(BacktestingBaseClass):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.execution_mode = 'backtesting'
        self._backtesting_tradelog_setup()
        self.col_tags = {
            'long_entry_cross': ('close', 'bband_20_low'),
            'long_close_cross': ('close', 'bband_20_high'),
            'short_entry_cross': ('close', 'bband_20_high'),
            'short_close_cross': ('close', 'bband_20_low'),
        }
        self.position_open_state = False # Vals: 'long_open', 'short_open', False
        _execution_type = '_execute_trade_anytime_entry'
        self._on_new_candle = getattr(self, _execution_type)

    def _backtesting_tradelog_setup(self):
        cols = (
            'time_candle',
            'symbol',
            'position',
            'action',
            'price',
            'balance')
        symbol = self.cfg['symbol'][0] + self.cfg['symbol'][1]
        line = (
            self.start_time,
            symbol,
            None,
            None,
            None,
            self.cfg["start_capital"]
        )
        write_mode = 'a'
        if not os.path.isfile('logs/backtesting_trades.csv'):
            write_mode = 'w'
        else:
            cols = None
        if not os.path.isdir(f'logs/plots'):
            os.mkdir('logs/plots')

        with open(f'logs/backtesting_trades.csv', write_mode, newline='') as f:
            writer = csv.writer(f)
            if cols:
                writer.writerow(cols)
            writer.writerow(line)

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

        trade_settings: (candle_time, <Long|Short>, <Open|Close>, close_price)

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
            symbol = self.cfg['symbol'][0] + self.cfg['symbol'][1]
            ob_snapshot = self.self.exchange.get_book(symbol=symbol, depth=10)
            self._place_live_order(trade_settings + (side, ob_snapshot))
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
                self.position = 1
                self.position_open_state = False
                settings = (row['datetime'], 'Long', 'Open', row['close'])
                self._execute_trade(settings)
        elif self.position > 0 and row[self.cross_buy_close_col]:
                # Long close
                self.position = 0
                settings = (row['datetime'], 'Long', 'Close', row['close'])
                self._execute_trade(settings)
                return True
        elif self.position == 0 and self.position_open_state == 'short_open':
            if row[self.cross_sell_open_col]:
                # Short entry
                self.position = -1
                self.position_open_state = False
                settings = (row['datetime'], 'Short', 'Open', row['close'])
                self._execute_trade(settings)
        elif self.position < 0 and row[self.cross_sell_close_col]:
                # Short close
                self.position = 0
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

    def run(self):
        super().run()
        #
        if not self._crosses_sanity_check():
            raise SanityCheckError
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

        # Plotting
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

        symbol = self.cfg['symbol'][0] + self.cfg['symbol'][1]
        if self.cfg['end']:
            end = self.cfg['end']
        else:
            end = dt.datetime.fromtimestamp(self.start_time)
            end = (end.year, end.month, end.day, end.hour, end.minute)
        plt_title = f'{symbol} ({self.cfg["asset_type"]}): {self.cfg["start"]} - {end}'
        self.data[0].fillna(method='ffill').plot(x='date', y='balances', title=plt_title)
        plt_file = f'logs/plots/{symbol}_{self.start_time}.pdf'
        plt.savefig(f'logs/plots/{symbol}_{self.start_time}.pdf')

        # Trades logging
        with open(f'logs/backtesting_trades.csv', 'a', newline='') as f:
            writer = csv.writer(f)
            for trade in self.trades:
                writer.writerow((trade[0], None) + (trade[1:]))

        self.logger.info(
            f'Start bal: {self.cfg["start_capital"]},'
            f' pnl: {self.pnl}'
            f' roi: {round((self.pnl/self.cfg["start_capital"])*100, 2)}%'
            f' num trades: {len(self.trades)}'
            f' dataframe: {self.data[0].shape}')

        # Upload files to S3
        write_s3('logs/backtester.log', bkt=self.s3_bkt_name)
        write_s3(plt_file, bkt=self.s3_bkt_name)

class LiveWillRBband(WillRBband):

    MAX_PERIODS = (20, 14 + 43) # Corresponding to (3m, 60m) data series

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.execution_mode = 'live'
        self._live_tradelog_setup()
        self.last_order = tuple() # (order_id, bals, size, position_action)
        self.neutral_inv = self.cfg['inv_neutral_bal']
        if self.neutral_inv == 'auto':
            bals = self.exchange.get_balances()
            self.neutral_inv = bals[self.cfg['symbol'][0]]

    def _live_tradelog_setup(self):
        bals = self.exchange.get_balances()
        cols = (
            'time_trade',
            'time_candle',
            'symbol',
            'side',
            'position_action',
            'size',
            'filled',
            'executed_price',
            'candle_close_price',
            'top_of_book_price',
            'order_id',
            'status',
            'fee',
            'fee_asset',
            'bal_base_before',
            'bal_base_after',
            'bal_quote_before',
            'bal_quote_after',
            'netliq_before',
            'netliq_after')
        netliq = self._get_netliq()
        now = int(dt.datetime.now().timestamp())
        line = (
            (now, '', self.cfg['symbol'][0] + self.cfg['symbol'][1])
                + tuple(('' for _ in cols[3:-6])) + (
                    bals[self.cfg['symbol'][0]], '',
                    bals[self.cfg['symbol'][1]], '', netliq, netliq))
        write_mode = 'a'
        if not os.path.isfile('logs/live_trades.csv'):
            write_mode = 'w'
        else:
            cols = None

        with open(f'logs/live_trades.csv', write_mode, newline='') as f:
            writer = csv.writer(f)
            if cols:
                writer.writerow(cols)
            writer.writerow(line)

    def _get_latest_candle(self, i):
        """
        API call + checks to fetch new candles as they become available
        """
        now = dt.datetime.now()
        latest_data_idx = int(self.data[i].index[-1])
        period = self.cfg['series'][i][2]
        if now.timestamp() - latest_data_idx > period + 2:
            # Get updated candle from exchange
            while True:
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
                    self.logger.debug(f'{latest_data_idx}, {idx}, {period}')
                    self.logger.debug(f'Retrying candle fetch')
                    time.sleep(1)
                else:
                    break

            row = {c:None for c in self.data[i].columns}
            self.data[i].loc[idx] = row
            self.data[i].at[idx, 'open'] = new_candle.iloc[-1]['open']
            self.data[i].at[idx, 'high'] = new_candle.iloc[-1]['high']
            self.data[i].at[idx, 'low'] = new_candle.iloc[-1]['low']
            self.data[i].at[idx, 'close'] = new_candle.iloc[-1]['close']
            return True
        else:
            return False

    def _live_accounting(self, order_id, bals_before, size, position_action, close_price, ob_snapshot):
        """
        For live trading, dump trade to csv

        """
        symbol = self.cfg['symbol'][0] + self.cfg['symbol'][1]
        trade_status = self.exchange.order_status(symbol=symbol, order_id=order_id)
        netliq_before = self._get_netliq(bals=bals_before)
        bals_after = self.exchange.get_balances()
        netliq_after = self._get_netliq(bals=bals_after)
        book_side = 'bids' if trade_status['side'] == 'BUY' else 'asks'
        ts_trade = str(trade_status['timestamp'])
        ts_trade = f'{ts_trade[:10]}.{ts_trade[10:]}'
        row = (
            ts_trade,
            self.data[0].index[-1],
            trade_status['symbol'],
            trade_status['side'],
            position_action,
            size,
            trade_status['quantity'],
            trade_status['price'],
            close_price,
            ob_snapshot[book_side][0][0],
            trade_status['order_id'],
            trade_status['status'],
            trade_status['fee'],
            trade_status['fee_asset'],
            bals_before[self.cfg['symbol'][0]],
            bals_after[self.cfg['symbol'][0]],
            bals_before[self.cfg['symbol'][1]],
            bals_after[self.cfg['symbol'][1]],
            netliq_before,
            netliq_after)

        trades_logfile = 'logs/live_trades.csv'
        with open(trades_logfile, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(row)
        write_s3(trades_logfile, bkt=self.s3_bkt_name)
        SNS_call(msg=(
            f"Trade: {ts_trade[:10]}, p: {round(trade_status['price'], 2)},"
            f" s: {round(float(trade_status['quantity']), 5)}, {position_action}")) ### tmp

    def _live_trade_size(self, params):
        """

        For live trading, calc max trade size allowed based on balance
        ..from API and current order book

        params: (time, <Long|Short>, <Open|Close>, close_price, side, ob_snapshot)

        """
        bals = self.exchange.get_balances()
        symbol = self.cfg['symbol'][0] + self.cfg['symbol'][1]
        book = self.exchange.get_book(symbol=symbol)
        sig_digs = len(
            self.exchange._symbol_info[symbol]['lot_prec'].split('.')[1])
        round_down = lambda x: int(x*10**sig_digs)/10**sig_digs
        adjuster_small = 1*round_down(1/10**sig_digs)
        adjuster_big = 0.85
        if self.cfg['asset_type'] == 'spot':
            fee = self.exchange.trade_fees['spot'][symbol]['taker']
            fee_asset = self.exchange.trade_fee_spot_asset
        else:
            raise NotImplementedError

        if params[4].upper() == 'SELL' and params[2] == 'Open':
            # Short open
            if self.cfg['asset_type'] == 'spot':
                if self.cfg['spot_short_method'] == 'inv':
                    if self.neutral_inv:
                        if bals[self.cfg['symbol'][0]] >= self.neutral_inv:
                            size = self.neutral_inv
                        else:
                            self.logger.critical(
                                f"Cfg'ed {self.cfg['symbol'][0]} neutral bal"
                                f" self.neutral_inv,"
                                f" is greater than bal"
                                f" {bals[self.cfg['symbol'][0]]}")
                            raise ApplicationStateError
                    else:
                        size = bals[self.cfg['symbol'][0]]
                elif self.cfg['spot_short_method'] == 'margin':
                    raise NotImplementedError
                # Adjust size to account for lot precision
                size = f'%.{sig_digs}f' % round_down(size)
            elif self.cfg['asset_type'] == 'futures':
                raise NotImplementedError
        elif params[4].upper() == 'SELL' and params[2] == 'Close':
            # Long close
            if self.cfg['rebal_on_close']:
                size = bals[self.cfg['symbol'][0]]/2
                size = f'%.{sig_digs}f' % round_down(size)
            elif fee_asset['BUY'] == 'base':
                # Pos open paid fees in base token, so pos is slightly smaller
                size = float(self.last_order[2])*(1 - fee)
                size = f'%.{sig_digs}f' % round_down(size)
            elif fee_asset['SELL'] == 'base':
                # Pos close fees in base token, so increase size slightly
                size = float(self.last_order[2])*(1 + fee)
                size = f'%.{sig_digs}f' % round_down(size)
            else:
                size = self.last_order[2]
        elif params[4].upper() == 'BUY' and params[2] == 'Open':
            # Long open
            if self.cfg['asset_type'] == 'spot':
                if self.cfg['spot_short_method'] == 'inv':
                    if self.neutral_inv:
                        if bals[self.cfg['symbol'][1]] >= self.neutral_inv*float(book['asks'][0][0]):
                            size = self.neutral_inv
                        else:
                            self.logger.critical(
                                f"Cfg'ed {self.cfg['symbol'][0]} neutral bal"
                                f" self.neutral_inv,"
                                f" is greater than account can buy with"
                                f" bal {bals[self.cfg['symbol'][1]]}")
                            raise ApplicationStateError
                    else:
                        size = bals[self.cfg['symbol'][1]]/float(book['asks'][0][0])
                elif self.cfg['spot_short_method'] == 'margin':
                    raise NotImplementedError
                # Adjust size to account for lot precision
                size = f'%.{sig_digs}f' % round_down(size)
            elif self.cfg['asset_type'] == 'futures':
                raise NotImplementedError
        elif params[4].upper() == 'BUY' and params[2] == 'Close':
            # Short close
            if self.cfg['spot_short_method'] == 'margin':
                raise NotImplementedError
            if self.cfg['rebal_on_close']:
                size = self.last_order[2] # No-op. Only rebal on Long close
            elif fee_asset['SELL'] == 'base':
                # Pos open paid fees in base token, so pos is slightly smaller
                # Note: most likely will never hit this case with any exchange
                size = float(self.last_order[2])*(1 - fee)
                size = f'%.{sig_digs}f' % round_down(size)
            elif fee_asset['BUY'] == 'base':
                # Pos close fees in base token, so increase size slightly
                #size = float(self.last_order[2])*(1 + fee)
                size = float(self.last_order[2])/(1 - fee)
                size = f'%.{sig_digs}f' % round_down(size)
            else:
                size = self.last_order[2]
        else:
            self.logger.critical(self.last_order, params)
            raise ApplicationStateError
        return size, bals

    def _get_netliq(self, bals=None):
        if not bals:
            bals = self.exchange.get_balances()
        symbol = self.cfg['symbol'][0] + self.cfg['symbol'][1]
        book = self.exchange.get_book(symbol=symbol)
        tkn_netliq = bals[self.cfg['symbol'][0]]*float(book['bids'][0][0])
        usdt_netliq = bals[self.cfg['symbol'][1]]
        return tkn_netliq + usdt_netliq

    def _place_live_order(self, params):
        """

        params: (time, <Long|Short>, <Open|Close>, close_price, side, ob_snapshot)

        """
        size, bals = self._live_trade_size(params)
        symbol = self.cfg['symbol'][0] + self.cfg['symbol'][1]
        self.logger.info(
            f'Placing order - symbol: {symbol}, side: {params[4]}, size: {size}')
        if self.cfg['asset_type'] == 'spot':
            order_id = self.exchange.place_order(symbol, params[4], size)
        elif self.cfg['asset_type'] == 'futures':
            pass # Placeholder
            #order_id = self.exchange.futures_place_order(symbol, params[4], size)
        else:
            raise ApplicationStateError
        position_action = f'{params[1].lower()}_{params[2].lower()}'
        self.last_order = (order_id, bals, size, position_action)
        accting_args = self.last_order + (params[3], params[5])
        #self._live_accounting(*self.last_order)
        self._live_accounting(*accting_args)

    def run(self):
        self.preprocess_data()
        write_mode = 'a'
        if not os.path.isfile('logs/live_candles.csv'):
            write_mode = 'w'
        with open(f'logs/live_candles.csv', write_mode) as f:
            writer = csv.writer(f)
            row = ['time'] + list(self.data[0])
            if write_mode == 'w':
                writer.writerow(row)
            writer.writerow(['' for _ in row])

        next_candle_secs = lambda: int(
            self.cfg['series'][0][2] - (
                dt.datetime.now().timestamp() % self.cfg['series'][0][2]))

        while True:
            # Periodically update candles from API
            if self._get_latest_candle(0): # Adds 3m candles
                self._get_latest_candle(1) # Adds 60m candles, hourly
                self.preprocess_data()
                row = self.data[0].iloc[-1]
                idx = self.data[0].index[-1]
                row = row.append(pd.Series([idx], index=['datetime']))
                with open(f'logs/live_candles.csv', 'a') as f:
                    writer = csv.writer(f)
                    writer.writerow([row[-1]] + list(row[:-1]))
                self._on_new_candle(row)
                self.logger.info(f'New candle:\n{row}')
                self.logger.info(f"Next candle in {next_candle_secs()}s")
            else:
                time.sleep(1)
                if int(str(int(dt.datetime.now().timestamp()))[-1]) % 9 == 0:
                    now = dt.datetime.now().timestamp()
                    remaining = self.cfg['series'][0][2] - now % self.cfg['series'][0][2]
                    self.logger.debug(f"Next candle in {next_candle_secs()}s")
