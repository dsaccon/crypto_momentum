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
from backtester import Base, parse_args


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
#        self.start_ts = int(dt.datetime.utcnow().timestamp()) - self.data_prefetch_period
        self.start_prefetch = dt.datetime.utcnow() - dt.timedelta(seconds=self.data_prefetch_period)

        self._align_first_row()
#        longest_period = max([_[2] for _ in self.data_cfg])
#        shortest_period = min([_[2] for _ in self.data_cfg])
#        if not longest_period % shortest_period == 0:
#            raise DataConfigurationError
#
#        while True:
#            # Push back start ts until it lines up with both series' periods
#            if self.start_ts % longest_period == 0:
#                break
#            else:
#                self.start_ts -= 1
#        self.start = dt.datetime.fromtimestamp(self.start_ts)
#        self.start = (
#            self.start.year,
#            self.start.month,
#            self.start.day,
#            self.start.hour,
#            self.start.minute)
        self.exchange_obj = self.exchange_cls(use_testnet=args.use_testnet)

#    def _align_first_row(self):
#        """
#
#        Make sure starting row aligns between both the longer and shorter series
#
#        """
#
#        longest_period = max([_[2] for _ in self.data_cfg])
#        shortest_period = min([_[2] for _ in self.data_cfg])
#        if not longest_period % shortest_period == 0:
#            raise DataConfigurationError
#        
#        start_ts = int(self.start_prefetch.timestamp())
#        while True:
#            # Push back start ts until it lines up with both series' periods
#            if start_ts % longest_period == 0:
#                break
#            else:
#                start_ts -= 1
#        start = dt.datetime.fromtimestamp(start_ts)
#        self.start_prefetch = (
#            start.year,
#            start.month,
#            start.day,
#            start.hour,
#            start.minute)

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

    def live_status(self, tokens):
        for tkn in tokens:
            status = self.exchange_obj.futures_get_positions(symbol=f'{tkn}USDT')
            if not float(status['unrealizedProfit']) == 0:
                print(f"{tkn}: {status['unrealizedProfit']}")
        desk_nl = self.exchange_obj._futures_get_balances()['totalMarginBalance']
        print(f'Desk NL: {desk_nl}')

    def run(self):
        if self.args.live_status:
            self.live_status(self.args.live_status)
            return
        if self.get_data():
            data = self.df
        else:
            raise DataCollectionError
        self.strategy(self.df, self.exchange_obj, self.trading_cfg).run()


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
    args = parse_args()
    if not args.live_status:
        logfile = 'logs/live_trader.log'
        print(f'Running live trader app, check logs at {logfile}')
        if not os.path.isdir('logs/'):
            os.mkdir('logs')
        logging.basicConfig(filename=logfile, level=logging.INFO)
        logging.info(f'{int(dt.datetime.utcnow().timestamp())}: Starting live trader')
    LiveTrader(args).run()
