import subprocess
import json 
import os
import pandas as pd
import datetime as dt
import argparse
import logging
import importlib

import strategies
from backtester import Base, parse_args


class DataConfigurationError(Exception):
    pass


class LiveTrader(Base):
    def __init__(self, *_args):
        super().__init__(*_args)

        # Figure out how much historical data to fetch before trading
        interval = 0
        #num_periods = lambda _i: 2*(self.strategy.MAX_PERIODS[_i] + 1)
        for i, _cfg in enumerate(self.data_cfg):
            #if _cfg[2]*num_periods(i) > interval:
            if _cfg[2]*self.strategy.MAX_PERIODS[i] > interval:
                interval = _cfg[2]*self.strategy.MAX_PERIODS[i]
        _end = dt.datetime.now()
        self.end = (_end.year, _end.month, _end.day, _end.hour, _end.minute)
        self.end_ts = _end.timestamp()
        interval += 2*max([c[2] for c in self.data_cfg]) # Add some padding
        self.start_ts = int(dt.datetime.now().timestamp()) - interval

        longest_period = max([_[2] for _ in self.data_cfg])
        shortest_period = min([_[2] for _ in self.data_cfg])
        if not longest_period % shortest_period == 0:
            raise DataConfigurationError

        while True:
            # Push back start ts until it lines up with both series' periods
            if self.start_ts % longest_period == 0:
                break
            else:
                self.start_ts -= 1
        self.start = dt.datetime.fromtimestamp(self.start_ts)
        self.start = (
            self.start.year,
            self.start.month,
            self.start.day,
            self.start.hour,
            self.start.minute)
        self.exchange_obj = self.exchange_cls(use_testnet=args.use_testnet)

    def _align_first_row(self):
        """
        For more than one series, make sure ts of first aligns with each period
        Otherwise, this will mess up some of the series calculations
        Basically, make sure first row ts had mod 0 for each period 
        """
        quit() 
        longest_period = max([_[2] for _ in self.data_cfg])
        logging.info(self.data_cfg)
        logging.info(longest_period)
        while True:
            if all([
                    d.iloc[0].index % longest_period == 0
                    for d in self.data
                    ]):
                return

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
            pass
            #self._trim_dataframes()
            #self._check_gaps()
        else:
            raise NotSupportedError

        return True

    def run(self):
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
    logfile = 'logs/live_trader.log'
    print(f'Running live trader app, check logs at {logfile}')
    if not os.path.isdir('logs/'):
        os.mkdir('logs')
    logging.basicConfig(filename=logfile, level=logging.DEBUG)
    logging.info(f'{int(dt.datetime.now().timestamp())}: Starting live trader')
    args = parse_args()
    LiveTrader(args).run()
