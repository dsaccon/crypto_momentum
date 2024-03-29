import os
import datetime as dt
import operator
import logging
import pandas as pd
import numpy as np
import btalib
import backtrader as bt


class ApplicationStateError(Exception):
    pass


class BacktestingBaseClass:

    data_cfg = tuple()
    operator_lookup = {
	'>': operator.gt,
	'<': operator.lt,
    }

    def __init__(self, data, exch_obj, cfg, bt_start=None, debug=False):
        self.execution_mode = None # 'backtest' or 'live'
        self.data = data
        self.exchange = exch_obj
        self.cfg = cfg
        self.bt_start = dt.datetime(*bt_start) if not bt_start is None else bt_start
        self.trades = []
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
        self.balances = []
        self.logger = logging.getLogger(__name__)
        self.start_time = int(dt.datetime.utcnow().timestamp())
        self.s3_bkt_name = os.environ.get('S3_BUCKET_NAME')
        self.debug = debug

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
                    and not comparator(row[f'{col_1}_prev'], row[f'{col_2}_prev']))
                else False, axis=1)

    def run(self):
        self.preprocess_data()

    def calc_pnl(self):
        symbol = self.cfg['symbol'][0] + self.cfg['symbol'][1]
        fee = 1 - 2*self.exchange.trade_fees[self.cfg['asset_type']][symbol]['taker']
        position = 0
        balance = self.cfg['start_capital']
        for i, trade in enumerate(self.trades):
            if position == 0:
                if trade[2] == 'Open':
                    if trade[1] == 'Long':
                        position = trade[3]
                    if trade[1] == 'Short':
                        position = -trade[3]
                    self.trades[i] = trade + (None,) # For trade logging
                else:
                    raise ApplicationStateError
            else:
                if trade[2] == 'Close':
                    if trade[1] == 'Short' and position < 0:
                        # Short closing
                        balance = ((-position - trade[3])/-position + 1)*balance*fee
                    elif trade[1] == 'Long' and position > 0:
                        # Long closing
                        balance = ((trade[3] - position)/position + 1)*balance*fee
                    else:
                        raise ApplicationStateError
                    self.balances.append((trade[0], balance))
                    self.trades[i] = trade + (balance,) # For trade logging
                    position = 0
                else:
                    raise ApplicationStateError
        self.pnl = balance - self.cfg['start_capital']

    def _place_live_order(self, side):
        raise NotImplementedError

    def _crosses_sanity_check(self):
        #
        for _, _df in enumerate(self.data):
            for col in _df.columns.values.tolist():
                if col.startswith('cross'):
                    _list = pd.Series(_df[col]).tolist()
                    if not True in _list:
                        return False
                    #print(f'{col}: {sum(_ == True for _ in _list)} crosses, out of {len(_list)} rows')
                    self.logger.debug(f'{col}: {sum(_ == True for _ in _list)} crosses, out of {len(_list)} rows')
        return True
