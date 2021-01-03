import datetime as dt
import operator
import backtrader as bt
import pandas as pd
import numpy as np
import btalib

from .base import BacktestingBaseClass

class WillRBband(BacktestingBaseClass):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.col_tags = {
            'long_entry_cross': ('close', 'bband_20_low'),
            'long_close_cross': ('close', 'bband_20_high'),
            'short_entry_cross': ('close', 'bband_20_high'),
            'short_close_cross': ('close', 'bband_20_low'),
        }
        self.position_open_state = False # Vals: 'long_open', 'short_open', False
        _execution_type = '_execute_trade_60m_entry'
        self._execute_trade = getattr(self, _execution_type)

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
                self.trades.append((row['datetime'], 'Long', 'Open', row['close']))
                self.position = 1
                self.position_open_state = False
                self._trades[-1] += '_long_open' ### tmp
        elif self.position > 0 and row[self.cross_buy_close_col]:
                # Long close
                self.trades.append((row['datetime'], 'Long', 'Close', row['close']))
                self.position = 0
                self._trades[-1] += '_long_close' ### tmp
                return True
        elif self.position == 0 and self.position_open_state == 'short_open':
            if row[self.cross_sell_open_col]:
                # Short entry
                self.trades.append((row['datetime'], 'Short', 'Open', row['close']))
                self.position = -1
                self.position_open_state = False
                self._trades[-1] += '_short_open' ### tmp
        elif self.position < 0 and row[self.cross_sell_close_col]:
                # Short close
                self.trades.append((row['datetime'], 'Short', 'Close', row['close']))
                self.position = 0
                self._trades[-1] += '_short_close' ### tmp
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
        print(self.data[0])
        print(self.data[0].shape)
        print('len', len(self._trades))
        self.data[0]['trades'] = self._trades
        self.data[0]['position_open_state'] = self._POSs
        self.data[0].to_csv('data/test/david_withcrosses_anytime_pos_entry.csv')
        import csv
        with open('data/test/david_trades_anytime_entry.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(self.trades)

    def run(self):
        super().run()
        #
        if not self._crosses_sanity_check():
            raise SanityCheckError
        processed_rows = 0
        self._trades = [] ### tmp
#        self._POSs = [] ### tmp
        self.long_open = False
        self.long_close = False
        self.short_open = False
        self.short_close = False
        for i, row in self.data[0].iterrows(): # iterate over 3m series
            _row = row.append(pd.Series([i], index=['datetime']))
            self._trades.append('')
            if self._execute_trade(_row):
                # If a position was closed in this candle, check to re-open new position
                self._execute_trade(_row)
#            self._POSs.append(self.position_open_state) ### tmp
        #self._debug_output()

        print('--', self.trades)
        print('Processed rows', processed_rows)
        self.calc_pnl()
        print(
            'pnl', self.pnl, 'ending capital', self.end_capital,
            f'{round((self.pnl/self.cfg["start_capital"])*100, 2)}%',
            'num trades', len(self.trades),
            'dataframe', self.data[0].shape)
