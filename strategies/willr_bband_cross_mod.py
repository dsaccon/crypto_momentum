import datetime as dt
import operator
import backtrader as bt
import pandas as pd
import numpy as np
import btalib

from .willr_bband import WillRBband

class WillRBbandCrossMod(WillRBband):

    def get_crosses(self, col_1, col_2, i, over=True):
        """
        col_1 -> close
        col_2 -> bband_20_<high|low>
        """
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
                    and comparator(row[col_1], row[col_2])
                    and not comparator(row[f'{col_1}_prev'], row[f'{col_2}']))
                else False, axis=1)
