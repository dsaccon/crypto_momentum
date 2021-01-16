import datetime as dt
import operator
import backtrader as bt
import pandas as pd
import numpy as np
import btalib

from .base import BacktestingBaseClass


class HA(BacktestingBaseClass):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.col_tags = {
        }
        self.order_size = 100
        self.pnl_mod = None

    def preprocess_data(self):

        self.data[0]['ha_open'] = np.nan
        self.data[0]['ha_close'] = np.nan
        self.data[0]['ha_high'] = np.nan
        self.data[0]['ha_low'] = np.nan
        self.data[0]['ha_color'] = ''

        _ha_o = []
        _ha_h = []
        _ha_l = []
        _ha_c = []
        _ha_color = []

        for i, (_, row) in enumerate(self.data[0].iterrows()):

            ohlc = (
                row.open,
                row.high,
                row.low,
                row.close
            )
            ha_c = (ohlc[0] + ohlc[1] + ohlc[2] + ohlc[3])/4

            if i == 0:
                # No prior HA candle is available, use prev raw candle open/close
                ha_o = (ohlc[0] + ohlc[3])/2
                ha_h = ohlc[1]
                ha_l = ohlc[2]
            else:
                ha_o = (last_ha_o + last_ha_c)/2
                ha_h = max(ohlc[1], ha_o, ha_c)
                ha_l = min(ohlc[2], ha_o, ha_c)

            if ha_c > ha_o:
                ha_color = 'Green'
            elif ha_c < ha_o:
                ha_color = 'Red'
            else:
                # Indecision candle
                ha_color = None

            _ha_o.append(ha_o)
            _ha_h.append(ha_h)
            _ha_l.append(ha_l)
            _ha_c.append(ha_c)
            _ha_color.append(ha_color)

            last_ha_o = ha_o
            last_ha_c = ha_c

        self.data[0]['ha_open'] = _ha_o
        self.data[0]['ha_high'] = _ha_h
        self.data[0]['ha_low'] = _ha_l
        self.data[0]['ha_close'] = _ha_c
        self.data[0]['ha_color'] = _ha_color

    def run(self):
        super().run()
        #
        first_order = True
        print('Running through data history...') ### tmp
        for i, row in self.data[0].iterrows():

            if i == 0:
                last_row = row
                continue

            _side = 'Buy'
            if row.ha_color == 'Red':
                _side = 'Sell'
            #
            size = self.order_size
            if first_order:
                self.trades.append((_side, row.close, size))
                first_order = False
                size *= 2
            elif not row.ha_color == last_row.ha_color:
                self.trades.append((_side, row.close, size))
            else:
                # Candle color same as previous. Do not place an order
                pass

            last_row = row

        self.calc_pnl()
        self.calc_pnl_mod()
        print('--', self.trades)
        start_cap = self.cfg['start_capital']
        print(
            'pnl', self.pnl,
            f'{round((self.pnl/start_cap)*100, 2)}%',
            'num trades', len(self.trades),
            'dataframe', self.data[0].shape)
        print('pnl ($), as per live trading:', self.pnl_mod)

    def calc_pnl(self):
        fee = 1 - self.exchange.trading_fee
        position = 0
        balance = self.cfg['start_capital']
        for i, trade in enumerate(self.trades):
            if trade[0] == 'Buy':
                pass 
            elif trade[0] == 'Sell' and not i == 0:
                balance = ((trade[1] - self.trades[i-1][1])/self.trades[i-1][1] + 1)*balance*fee

        self.pnl = balance - self.cfg['start_capital']

    def calc_pnl_mod(self):
        """
        To follow order sizes as per live trading settings
        """
        fee = 1 - self.exchange.trading_fee
        pnl = 0
        for i, trade in enumerate(self.trades):
            if i == 0:
                continue
            if trade[0] == 'Buy':
                pass 
            elif trade[0] == 'Sell' and not i == 0:
                pnl += ((trade[1] - self.trades[i-1][1])/self.trades[i-1][1] + 1)*2*self.order_size*fee
        self.pnl_mod = pnl
