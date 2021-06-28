import subprocess
import json 
import os
import pandas as pd
import datetime as dt
import argparse
import logging
import importlib
from dotenv import load_dotenv

import strategies
from backtester import Base


class DataCollectionError(Exception):
    pass


class DataConfigurationError(Exception):
    pass


class LiveTrader(Base):
    def __init__(self, *_args):
        super().__init__(*_args)

        # Figure out how much historical data to fetch before trading
        interval = 0
        for i, _cfg in enumerate(self.data_cfg):
            if _cfg[2]*self.strategy.MAX_PERIODS[i] > interval:
                interval = _cfg[2]*self.strategy.MAX_PERIODS[i]
        if not self.data_prefetch_period > interval:
            raise DataConfigurationError

        _end = dt.datetime.utcnow()
        self.end = (_end.year, _end.month, _end.day, _end.hour, _end.minute)
        self.end_ts = _end.timestamp()
        self.start_prefetch = dt.datetime.utcnow() - dt.timedelta(seconds=self.data_prefetch_period)

        self._align_first_row()
        self.exchange_obj = self.exchange_cls(use_testnet=args.use_testnet)

        if self.args.debug:
            if not os.path.exists(f'{self.path}/logs/debug/'):
                os.mkdir(f'{self.path}/logs/debug/')

    @staticmethod
    def parse_args():
        argp = Base.parse_args()
        argp.add_argument(
            "-l", "--live-status", type=str, default=None, nargs="*", help="Show open positions and desk netliq. Pass in list of tokens. For live trading only"
        )
        argp.add_argument(
            "-c", "--close-position", type=str, default=None, nargs="*", help=""
        )
        argp.add_argument(
            "-d", "--debug", action='store_true', help="Debug mode. Print tables to csvs in logs/debug/ folder"
        )
        args = argp.parse_args()
        return args

    def get_data(self):
        # Fetch data from exchange
        if self.csv_file is None:
            for series in self.data_cfg:
                if not self._get_data_api(series):
                    return False
        else:
            if not len(self.data_cfg) == len(self.csv_file):
                logging.info(f'Number of csv files provided should be {len(self.data_cfg)}')
                logging.info(self.data_cfg)
                logging.info(self.csv_file)
                raise ValueError
            self._get_data_csv()

        # Clean data
        if len(self.df) == 1:
            return True
        elif len(self.df) == 2:
            if not self._validate_data():
                logging.critical('There is something wrong with the source data')
                raise DataCollectionError
            #self._trim_dataframes()
            #self._check_gaps()
        else:
            raise NotSupportedError

        return True

    def show_positions_nl(self, tokens):
        print('Open positions (unrealized P&L):')
        filler = None
        for tkn in tokens:
            position = self.exchange_obj.futures_get_positions(symbol=f'{tkn}USDT')
            if not float(position['unrealizedProfit']) == 0:
                side = 'Long' if float(position['positionAmt']) > 0 else 'Short'
                filler = ' '.join(['' for _ in range(7 - len(tkn))])
                print(f"    {tkn}:{filler}${round(float(position['unrealizedProfit']), 2)} ({side})")
        if filler is None:
            print('    None')
        desk_nl = self.exchange_obj._futures_get_balances()['totalMarginBalance']
        print(f'Desk NL:   ${round(float(desk_nl), 2)}')

    def close_positions(self, tokens):
        for tkn in tokens:
            symb = f'{tkn}USDT'
            posn = self.exchange_obj.futures_get_positions(symbol=symb)
            if abs(float(posn['positionAmt'])) > 0:
                print(f'{tkn} - Closing position')
                self.exchange_obj.futures_close_position(symbol=symb)
            else:
                print(f'{tkn} - No position was open')

    def run(self):
        if not self.args.live_status is None:
            self.show_positions_nl(self.args.live_status)
            return
        if not self.args.close_position is None:
            self.close_positions(self.args.close_position)
            return
        #
        if self.get_data():
            data = self.df
        else:
            raise DataCollectionError
        self.strategy(
            self.df, self.exchange_obj,
            self.trading_cfg, debug=self.args.debug).run()


def test_setup():
    args = Object()
    args.name = 'WillRBband_BTC_3m_60m'
    args.exchange = 'binance'
    args.strategy = 'WillRBband'
    args.symbol = 'btcusdt'
    args.start = [2020, 9, 1]
    args.end = None
    args.period = '1min'

    return args


if __name__ == '__main__':
    load_dotenv()
    args = LiveTrader.parse_args()
    if args.live_status is None and args.close_position is None:
        logfile = 'logs/live_trader.log'
        print(f'Running live trader app, check logs at {logfile}')
        if not os.path.isdir('logs/'):
            os.mkdir('logs')
        logging.basicConfig(filename=logfile, level=logging.INFO)
        logging.info(f'{int(dt.datetime.utcnow().timestamp())}: Starting live trader')
    LiveTrader(args).run()
