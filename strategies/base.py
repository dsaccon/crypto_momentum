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

    def __init__(self, data, exch_obj, cfg):
        self.data = data
        self.cfg = cfg
        self.exchange = exch_obj
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
        fee = 1 - self.exchange.trading_fee
        position = 0
        balance = self.cfg['start_capital']
        for trade in self.trades:
            if position == 0:
                if trade[2] == 'Open':
                    if trade[1] == 'Long':
                        position = trade[3]
                    if trade[1] == 'Short':
                        position = -trade[3]
                else:
                    raise SanityCheckError
            else:
                if trade[2] == 'Close':
                    if trade[1] == 'Short' and position < 0:
                        # Short closing
                        balance = ((-position - trade[3])/-position + 1)*balance*fee
                    elif trade[1] == 'Long' and position > 0:
                        # Long closing
                        balance = ((trade[3] - position)/position + 1)*balance*fee
                    else:
                        raise SanityCheckError
                    position = 0
                else:
                    raise SanityCheckError
        self.end_capital = balance
        self.pnl = self.end_capital - self.cfg['start_capital']

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
