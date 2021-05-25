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

from utils.sns import SNS_call


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
        #_execution_type = '_execute_trade_anytime_entry'
        _execution_type = '_execute_trade_all_params'
    
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
        bt_params = ' asset_type:' + self.cfg['asset_type'] + \
                    ' strategy:'   + self.cfg['strategy'] + \
                    ' floating_willr:' + str(self.cfg['floating_willr']) + \
                    ' ENTRY PARAMS:' + \
                    ' willr_diff_threshold:' + str(self.cfg['willrema_diff_threshold']) + \
                    ' willrema_long_entry:' + str(self.cfg['willrema_long_entry']) + \
                    ' willrEMA_short_entry:' + str(self.cfg['willrema_short_entry']) + \
                    ' willr_long_entry:' + str(self.cfg['willr_long_entry']) + \
                    ' willr_short_entry:' + str(self.cfg['willr_short_entry']) + \
                    ' stoploss:' + str(self.cfg['stoploss']) + \
                    ' timestop:' + str(self.cfg['timestop'])

        symbol = symbol + bt_params
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

    def _create_floating_willr(self):
        ### Build floating willr/willr_ema indicators
        modulo = int(self.cfg['series'][1][-1])
        num_intervals = int(self.cfg['series'][1][-1]/self.cfg['series'][0][-1])
        last_idx = self.data[0].index[-1]
        offset_end = int(last_idx % modulo / self.cfg['series'][0][-1])
        willr = [None]*num_intervals
        willr_ema = [None]*num_intervals
        period_str = self.cfg['series'][1][1] # Period str in mins (e.g. '60m')
        tag = f'{period_str}_float'
        # Get resampled longer-interval series
        for _i, offset_beg in enumerate(range(num_intervals)): # offset from beginning of self.data[0]
            if offset_beg > offset_end:
                offset = num_intervals - (offset_beg - offset_end)
            else:
                offset = offset_end - offset_beg
            floating_series = self._resample_floating_candles(offset=offset)
#            floating_series.to_csv(f'logs/floating/3.floating_60m_{_i}.csv') ### tmp
            willr[_i] = btalib.willr(
                floating_series[f'high_{tag}'],
                floating_series[f'low_{tag}'],
                floating_series[f'close_{tag}'],
                period = 14).df
            willr[_i].rename(columns = {'r': 'willr'}, inplace = True)
            willr_ema[_i] = btalib.ema(willr[_i]['willr'], period = 43, _seed = 3).df
            willr_ema[_i].rename(columns = {'ema': 'willr_ema'}, inplace = True)

#        for _i in range(num_intervals): ###
#            willr[_i].to_csv(f'logs/floating/4.willr_{_i}.csv') ### tmp
#            willr_ema[_i].to_csv(f'logs/floating/4.willr_ema_{_i}.csv') ### tmp

        # Combine & interleave each longer-interval willr series to a shorter-interval df
        _willr = pd.concat([w for w in willr], sort=True)
        _willr = _willr.sort_index()
        _willr['timestamp'] = _willr.index.astype(np.int64) // 10 ** 9
        _willr.set_index('timestamp', inplace=True)
        self.data[0][f'willr_{period_str}_float'] = _willr.willr

        _willr_ema = pd.concat([w for w in willr_ema], sort=True)
        _willr_ema = _willr_ema.sort_index()
        _willr_ema['timestamp'] = _willr_ema.index.astype(np.int64) // 10 ** 9
        _willr_ema.set_index('timestamp', inplace=True)
        self.data[0][f'willr_ema_{tag}'] = _willr_ema.willr_ema
        self.data[0][f'willr_ema_prev_{tag}'] = self.data[0][f'willr_ema_{tag}'].shift(num_intervals)
#        self.data[0].to_csv(f'logs/floating/5.done.csv') ### tmp

    def _create_floating_ohlc(self):
        """
        Construct a series of OHLC floating candles for the longer series,
            offset by the current period of the shorter candle

        E.g. for 3m/60m candles, at 12:21pm we receive a new 3m candle timestamped at 12:18pm
            We then construct a 60m candle, consisting of 20 candles timestamped from
            11:21am to 12:18pm inclusively

        There are nuances here that are important to spell out, in order to properly model the data
            and avoid polluting it with information from the future

        Basically, the current row's timestamp should be considered from 3 perspectives:
            1. It represents the beginning of the current (shorter) candle that just finished
                * E.g. 12:18pm
            2. It represents the one (shorter) period of time before the end of the (longer) candle
                * The end of the 60m candle is 12:21pm
            3. It does not represent real-time for the algo. The algo lives one (shorter) period later
                * The algo lives (i.e. is making decisions) at 12:21pm
        """

        period_short = self.cfg['series'][0][2] # Period in secs
        period_long = self.cfg['series'][1][2] # Period in secs
        period_long_str = self.cfg['series'][1][1] # Period str in mins (e.g. '60m')
        tag = f'{period_long_str}_float'
        self.data[0][f'open_{tag}'] = pd.Series()
        self.data[0][f'high_{tag}'] = pd.Series()
        self.data[0][f'low_{tag}'] = pd.Series()
        self.data[0][f'close_{tag}'] = pd.Series()
        self.data[0]['Datetime'] = pd.to_datetime(self.data[0].index, unit='s')
#        self.data[0].to_csv('logs/floating/1.orig_data.csv') ### tmp
        first_row_ts = self.data[0].index[0]
        for i, row in self.data[0].iterrows():
            i_start = i - period_long + period_short
            i_end = i
            if i_start < first_row_ts:
                continue
            self.data[0].at[i, f'open_{tag}'] = self.data[0].loc[i_start]['open']
            self.data[0].at[i, f'high_{tag}'] = self.data[0].loc[i_start:i_end]['high'].max()
            self.data[0].at[i, f'low_{tag}'] = self.data[0].loc[i_start:i_end]['low'].min()
            self.data[0].at[i, f'close_{tag}'] = self.data[0].loc[i_end]['close']
#        self.data[0].to_csv('logs/floating/2.floating_ohlc.csv') ### tmp

    def _resample_floating_candles(self, offset=0):
        """
        Pick out the longer series' candles, offset to the final row of the dataFrame

        Used to enable calculating WillR/EMA on floating candles

		Arguments
		---------
		offset (int): number of (shorter period) intervals from last row

		Returns
		---------
		df (pd): resampled (longer period) dataFrame

        """
        modulo = int(self.cfg['series'][1][-1])
        period_str = self.cfg['series'][1][1] # Period str in mins (e.g. '60m')
        if not period_str.endswith('m'):
            self.logger.critical(f'Interval for the longer series {period_str} is not correct')
            raise ValueError
        _period_str = period_str + 'in'
        df_flt = self.data[0][[f'high_{period_str}_float', f'low_{period_str}_float', f'close_{period_str}_float']]
        df_flt.index = pd.to_datetime(self.data[0].index, unit='s')

        last_idx = int(df_flt.index[-1].timestamp())

        offset = int(last_idx % modulo / self.cfg['series'][0][-1]) - offset
        offset_str = str(int(self.cfg['series'][0][1][:-1])*offset) + self.cfg['series'][0][1][-1]
        df_flt_resampled = df_flt.resample(_period_str, origin='start', offset=offset_str)

        return df_flt_resampled.interpolate()

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
            ob_snapshot = self.exchange.get_book(symbol=symbol, depth=10, asset_type=self.cfg['asset_type'])
            self._place_live_order(trade_settings + (side, ob_snapshot))
        else:
            raise ValueError

    def _execute_trade_anytime_entry(self, row):
        """
        Modified trade logic.
        Anytime entry. Attempts to fix issue of position entry only on the first
            ..3m tick of the 60m period
        """
        tag = ''
        if self.cfg['floating_willr']:
            tag = f"_{self.cfg['series'][1][1]}_float"

        if row[f'willr_ema{tag}'] > row[f'willr_ema_prev{tag}'] and not self.position > 0:
            self.position_open_state = 'long_open'
        elif row[f'willr_ema{tag}'] < row[f'willr_ema_prev{tag}'] and not self.position < 0:
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
                self._trades[-1] += '_long_open'
        elif self.position > 0 and row[self.cross_buy_close_col]:
                # Long close
                self.trades.append((row['datetime'], 'Long', 'Close', row['close']))
                self.position = 0
                self._trades[-1] += '_long_close'
                return True
        elif self.position == 0 and row['willr_ema'] < row['willr_ema_prev']:
            if row[self.cross_sell_open_col]:
                # Short entry
                self.trades.append((row['datetime'], 'Short', 'Open', row['close']))
                self.position = -1
                self._trades[-1] += '_short_open'
        elif self.position < 0 and row[self.cross_sell_close_col]:
                # Short close
                self.trades.append((row['datetime'], 'Short', 'Close', row['close']))
                self.position = 0
                self._trades[-1] += '_short_close'
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
                self._trades[-1] += '_long_open'
                self.long_open = False
                self.long_close = True
        elif self.long_close and row[self.cross_buy_close_col]:
                # Long close
                self.trades.append((row['datetime'], 'Long', 'Close', row['close']))
                self.position = 0
                self._trades[-1] += '_long_close'
                self.long_close = False
                return True
        elif self.short_open and row['willr_ema'] < row['willr_ema_prev']:
            if row[self.cross_sell_open_col]:
                # Short entry
                self.trades.append((row['datetime'], 'Short', 'Open', row['close']))
                self.position = -1
                self._trades[-1] += '_short_open'
                self.short_open = False
                self.short_close = True
        elif self.short_close and row[self.cross_sell_close_col]:
                # Short close
                self.trades.append((row['datetime'], 'Short', 'Close', row['close']))
                self.position = 0
                self._trades[-1] += '_short_close'
                self.short_close = False
                return True
        return False

    def _execute_trade_all_params(self, row):
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

    def run(self):
        if self.cfg['floating_willr']:
            self._create_floating_ohlc()
        self.preprocess_data()
        #
        if not self._crosses_sanity_check():
            raise SanityCheckError
        self._trades = [] # Trade diagnostic col for writing df to csv
        self._POSs = [] # ""
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
            self._POSs.append(self.position_open_state)

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
        self.last_order = tuple() # (order_status: dict, bals: dict, size: float, position_action: str)
        self.neutral_inv = self.cfg['inv_neutral_bal']
        if self.neutral_inv == 'auto':
            bals = self.exchange.get_balances(asset_type=self.cfg['asset_type'])
            self.neutral_inv = bals[self.cfg['symbol'][0]]

    def _live_tradelog_setup(self):
        bals = self.exchange.get_balances(asset_type=self.cfg['asset_type'])
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
            'netliq_after',
            'margin_bal_before',
            'margin_bal_after')
        if self.cfg['asset_type'] == 'spot':
            netliq = self._get_netliq()
            margin_bal = ''
        elif self.cfg['asset_type'] == 'futures':
            netliq = ''
            margin_bal = self._get_netliq()
        now = int(dt.datetime.utcnow().timestamp())
        symbol = self.cfg['symbol'][0] + self.cfg['symbol'][1]
        line = (
            (now, '', symbol)
                + tuple(('' for _ in cols[3:-6])) + (
                    bals.get(self.cfg['symbol'][0]), '',
                    bals.get(self.cfg['symbol'][1]), '',
                    netliq, netliq, margin_bal, margin_bal))
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
        now = dt.datetime.utcnow()
        latest_data_idx = int(self.data[i].index[-1])
        period = self.cfg['series'][i][2]
        if now.timestamp() - latest_data_idx > 2*period:
            # Get updated candle from exchange
            while True:
                start = dt.datetime.fromtimestamp(now.timestamp() - 3*period)
                new_candle = self.exchange.get_backtest_data(
                    self.cfg['symbol'][0]+self.cfg['symbol'][1],
                    period,
                    start,
                    now,
                    asset_type=self.cfg['asset_type'])
                idx = int(new_candle.iloc[-1]['datetime'])
                if not idx == latest_data_idx + period:
                    self.logger.info(f'now: {dt.datetime.utcnow().timestamp()}')
                    self.logger.info(f'last completed candle:')
                    self.logger.info(f' {new_candle.iloc[-1]["datetime"]}')
                    self.logger.info(f'{latest_data_idx}, {idx}, {period}')
                    self.logger.info(f'Retrying candle fetch')
                    time.sleep(1)
                else:
                    break
                now = dt.datetime.utcnow()

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
        trade_status = self.exchange.order_status(symbol=symbol, order_id=order_id, asset_type=self.cfg['asset_type'])
        netliq_before = self._get_netliq(bals=bals_before)
        bals_after = self.exchange.get_balances(asset_type=self.cfg['asset_type'])
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
            bals_before.get(self.cfg['symbol'][0]),
            bals_after.get(self.cfg['symbol'][0]),
            bals_before.get(self.cfg['symbol'][1]),
            bals_after.get(self.cfg['symbol'][1]),
            netliq_before,
            netliq_after,
            '',
            '')

        trades_logfile = 'logs/live_trades.csv'
        with open(trades_logfile, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(row)
        if position_action.endswith('close'):
            fees = 2*self.exchange.trade_fees[self.cfg['asset_type']][symbol]['taker']
            op = operator.add if position_action.startswith('long') else operator.sub
            hurdle = round(op(1, fees)*self.last_order[0]['price'], 2)
            hurdle_str = f' h: {hurdle},'
        else:
            hurdle_str = ''
        write_s3(trades_logfile, bkt=self.s3_bkt_name)
        SNS_call(msg=(
            f"{ts_trade[:10]}: {symbol},"
            f" p: {round(trade_status['price'], 2)}, {hurdle_str}"
            f" s: {round(float(trade_status['quantity']), 5)}, {position_action}"
            f" nl: {round(netliq_after)}"))

    def _live_trade_size(self, params):
        """

        For live trading, calc max trade size allowed based on balance
        ..from API and current order book

        params: (time, <Long|Short>, <Open|Close>, close_price, side, ob_snapshot)

        """
        bals = self.exchange.get_balances(asset_type=self.cfg['asset_type'])
        symbol = self.cfg['symbol'][0] + self.cfg['symbol'][1]
        book = self.exchange.get_book(symbol=symbol, asset_type=self.cfg['asset_type'])
        sig_digs = len(
            self.exchange._symbol_info[self.cfg['asset_type']][symbol]['lot_prec'].split('.')[1])
        round_down = lambda x: int(x*10**sig_digs)/10**sig_digs
        if self.cfg['asset_type'] == 'spot':
            fee = self.exchange.trade_fees['spot'][symbol]['taker']
            fee_asset = self.exchange.trade_fee_spot_asset
        elif self.cfg['asset_type'] == 'futures':
            fee = self.exchange.trade_fees['futures'][symbol]['taker']
            if self.cfg['futures_margin_type'] == 'token':
                fee_asset = self.cfg['symbol'][0]
            elif self.cfg['futures_margin_type'].upper() == 'USDT':
                fee_asset = 'USDT'
        else:
            raise ValueError

        if params[4].upper() == 'SELL' and params[2] == 'Open':
            # Short open
            if self.cfg['asset_type'] == 'spot':
                if self.cfg['max_trade_size'] > 0:
                    size = self.cfg['max_trade_size']
                elif self.cfg['spot_short_method'] == 'inv':
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
            elif self.cfg['asset_type'] == 'futures':
                if self.cfg['max_trade_size'] > 0:
                    size = self.cfg['max_trade_size']*(1 - 2*fee)
                elif self.cfg['futures_margin_type'] == 'USDT':
                    index_price = self.exchange.futures_get_index_price(symbol=symbol)
                    size = bals['USDT']*(1 - 2*fee)/float(index_price)
                elif self.cfg['futures_margin_type'] == 'token':
                    raise NotImplementedError
                else:
                    raise ValueError
            # Adjust size to account for lot precision
            size = f'%.{sig_digs}f' % round_down(size)
        elif params[4].upper() == 'SELL' and params[2] == 'Close':
            # Long close
            if self.cfg['asset_type'] == 'futures':
                size = self.last_order[2]
            elif self.cfg['rebal_on_close']:
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
                if self.cfg['max_trade_size'] > 0:
                    size = self.cfg['max_trade_size']
                elif self.cfg['spot_short_method'] == 'inv':
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
            elif self.cfg['asset_type'] == 'futures':
                if self.cfg['max_trade_size'] > 0:
                    size = self.cfg['max_trade_size']*(1 - 2*fee)
                elif self.cfg['futures_margin_type'] == 'USDT':
                    index_price = self.exchange.futures_get_index_price(symbol=symbol)
                    size = bals['USDT']*(1 - 2*fee)/float(index_price)
                elif self.cfg['futures_margin_type'] == 'token':
                    raise NotImplementedError
                else:
                    raise ValueError
            # Adjust size to account for lot precision
            size = f'%.{sig_digs}f' % round_down(size)
        elif params[4].upper() == 'BUY' and params[2] == 'Close':
            # Short close
            if self.cfg['spot_short_method'] == 'margin':
                raise NotImplementedError
            if self.cfg['asset_type'] == 'futures':
                size = self.last_order[2]
            elif self.cfg['rebal_on_close']:
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
            bals = self.exchange.get_balances(asset_type=self.cfg['asset_type'])
        symbol = self.cfg['symbol'][0] + self.cfg['symbol'][1]
        book = self.exchange.get_book(symbol=symbol, asset_type=self.cfg['asset_type'])
        if self.cfg['asset_type'] == 'spot':
            tkn_netliq = bals[self.cfg['symbol'][0]]*float(book['bids'][0][0])
            usdt_netliq = bals[self.cfg['symbol'][1]]
            return tkn_netliq + usdt_netliq
        elif self.cfg['asset_type'] == 'futures':
            return self.exchange._futures_get_balances()['totalMarginBalance']

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
            if params[2] == 'Open':
                order_id = self.exchange.futures_place_order(symbol, params[4], size)
            elif params[2] == 'Close':
                order_id = self.exchange.futures_close_position(symbol=symbol)
            else:
                raise ValueError
        else:
            raise ApplicationStateError
        position_action = f'{params[1].lower()}_{params[2].lower()}'
        accting_args = (order_id, bals, size, position_action, params[3], params[5])
        self._live_accounting(*accting_args)
        order_status = self.exchange.order_status(symbol=symbol, order_id=order_id, asset_type=self.cfg['asset_type'])
        self.last_order = (order_status, bals, size, position_action)

    def run(self):
        if self.cfg['floating_willr']:
            self._create_floating_ohlc()
        self.preprocess_data()
        #
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
                dt.datetime.utcnow().timestamp() % self.cfg['series'][0][2]))

        while True:
            # Periodically update candles from API
            if self._get_latest_candle(0): # Adds 3m candles
                self._get_latest_candle(1) # Adds 60m candles, hourly

                if self.cfg['floating_willr']:
                    self._create_floating_ohlc()
                self.preprocess_data()
                row = self.data[0].iloc[-1]
                idx = self.data[0].index[-1]
                row = row.append(pd.Series([idx], index=['datetime']))
                with open(f'logs/live_candles.csv', 'a') as f:
                    writer = csv.writer(f)
                    writer.writerow([row[-1]] + list(row[:-1]))
                self._on_new_candle(row)
                self.data[0].to_csv(f'logs/live_table.csv') ### tmp
                self.logger.info(f'New candle:\n{row}')
                self.logger.info(f"Next candle in {next_candle_secs()}s")
            else:
                time.sleep(1)
                if int(str(int(dt.datetime.utcnow().timestamp()))[-1]) % 9 == 0:
                    now = dt.datetime.utcnow().timestamp()
                    remaining = self.cfg['series'][0][2] - now % self.cfg['series'][0][2]
                    self.logger.debug(f"Next candle in {next_candle_secs()}s")

