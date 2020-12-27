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

    def preprocess_data(self):
        super().preprocess_data()

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

    def _execute_trade(self, row):
        if self.position == 0 and row['willr_ema'] > row['willr_ema_prev']:
            if row[self.cross_buy_open_col]:
                # Long entry
                self.trades.append((row['datetime'], 'Long', 'Open', row['close']))
                self.position = 1
        elif self.position > 0 and row[self.cross_buy_close_col]:
                # Long close
                self.trades.append((row['datetime'], 'Long', 'Close', row['close']))
                self.position = 0
                return True
        elif self.position == 0 and row['willr_ema'] < row['willr_ema_prev']:
            if row[self.cross_sell_open_col]:
                # Short entry
                self.trades.append((row['datetime'], 'Short', 'Open', row['close']))
                self.position = -1
        elif self.position < 0 and row[self.cross_sell_close_col]:
                # Short close
                self.trades.append((row['datetime'], 'Short', 'Close', row['close']))
                self.position = 0
                return True
        return False

    def run(self):
        super().run()
        #
        if not self._crosses_sanity_check():
            raise SanityCheckError
        processed_rows = 0
        for i, row in self.data[0].iterrows(): # iterate over 3m series
            #if i_3m % 20 == 0:
            #row_1 = self.data[1].iloc[int(i_3m/20)] # corresponding 60m row
#            row_1 = (self.data[1].loc[self.data[0]['datetime'] == row_0.datetime])
#            if row_1.empty:
#                # End of 60m series
#                break
#            if any([
#                    row_1.isna().willr_ema.iloc[0],
#                    row_1.isna().willr_ema_prev.iloc[0]]):
#                # Skip first row before 60m' first candle
#                continue
#            processed_rows += 1

            _row = row.append(pd.Series([i], index=['datetime']))
            if self._execute_trade(_row):
                # If a position was closed in this candle, check to re-open new position
                self._execute_trade(_row)

#            # Only progress 60m series iteration when ts' line up
#            if self.position == 0 and row_1['willr_ema'].iloc[0] > row_1['willr_ema_prev'].iloc[0]:
#                if row_0[self.cross_buy_open_col]:
#                    # Long entry
#                    self.trades.append(('Buy', 'Open', row_0['close']))
#                    self.position = 1
#            elif self.position > 0 and row_0[self.cross_buy_close_col]:
#                    # Long close
#                    self.trades.append(('Buy', 'Close', row_0['close']))
#                    self.position = 0
#            if self.position == 0 and row_1['willr_ema'].iloc[0] < row_1['willr_ema_prev'].iloc[0]:
#                if row_0[self.cross_sell_open_col]:
#                    # Short entry
#                    self.trades.append(('Sell', 'Open', row_0['close']))
#                    self.position = -1
#            elif self.position < 0 and row_0[self.cross_sell_close_col]:
#                    # Short close
#                    self.trades.append(('Sell', 'Close', row_0['close']))
#                    self.position = 0

        print('--', self.trades)
        print('Processed rows', processed_rows)
        self.calc_pnl()
        print(
            'pnl', self.pnl, 'ending capital', self.end_capital,
            f'{round((self.pnl/self.cfg["start_capital"])*100, 2)}%',
            'num trades', len(self.trades),
            'dataframe', self.data[0].shape)
