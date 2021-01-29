import subprocess
import json 
import os
import pandas as pd
import datetime as dt
import argparse
import logging
import importlib
import strategies


class DataConfigurationError(Exception):
    pass


class LiveTrader:
    def __init__(self, args):

        self.run_name = args.name
        self.path = '/'.join(os.path.abspath(__file__).split('/')[:-1])

        self._load_config()

        # Overwrite config file settings from cli args
        self.csv_file = args.file
        if args.symbol:
            self.data_cfg = [[args.symbol, c[1]] for c in self.data_cfg]
        if args.period:
            self.data_cfg = [[c[0], args.period[i]] for i, c in enumerate(self.data_cfg)]
        self.trading_cfg['num_periods'] = args.num_periods if args.num_periods else self.num_periods
        self.start = tuple(args.start) if args.start else self.start
        self.start_ts = int(dt.datetime(*self.start).timestamp())
        if args.end is not False:
            self.end = tuple(args.end) if args.end else args.end
            if self.end:
                self.end_ts = str(int(dt.datetime(*self.end).timestamp()))

        for _cfg in self.data_cfg:
            i = 1
            if _cfg[1].endswith('s'):
                _mult = 1
            elif _cfg[1].endswith('m'):
                _mult = 60
            elif _cfg[1].endswith('h'):
                _mult = 60*60
            elif _cfg[1].endswith('d'):
                _mult = 60*60*24
            elif _cfg[1].endswith('w'):
                _mult = 60*60*24*7
            elif _cfg[1].endswith('mo'):
                _mult = 60*60*24*31
                i = 2
            _cfg.append(int(_cfg[1][:-i])*_mult)

        self.df_expected_cols = ['datetime', 'open', 'high', 'low', 'close']
        self.df = []

        ####
        interval = 0
        num_periods = 2*(self.strategy.MAX_PERIODS[i] + 1)
        for i, _cfg in enumerate(self.data_cfg):
            if _cfg[2]*num_periods > interval:
                interval = _cfg[2]*self.strategy.MAX_PERIODS[i]
        _end = dt.datetime.now()
        self.end = (_end.year, _end.month, _end.day, _end.hour, _end.minute)
        self.end_ts = _end.timestamp()
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

    def _load_config(self):
        """
        Load config settings from config.json
        """
        with open(f'{self.path}/config.json', 'r') as f:
            cfg = json.load(f)

        self.strategy = getattr(strategies, f"Live{cfg[self.run_name]['strategy']}")
        #self.strategy = getattr(strategies, cfg[self.run_name]['strategy'])
        _exch = cfg[self.run_name]['exchange']
        _path = f"exchanges.{_exch}"
        _exch_obj = importlib.import_module(_path)
        self.exchange_cls = getattr(_exch_obj, f'{_exch[0].upper()}{_exch[1:]}API')
        self.exchange_obj = self.exchange_cls()
        self.start = tuple(cfg[self.run_name]['start'])
        _end = cfg[self.run_name]['end']
        self.end = tuple(_end) if _end else _end
        self.start_capital = cfg[self.run_name]['start_capital']
        self.num_periods = cfg[self.run_name]['num_periods']
        if self.end:
            self.end_ts = str(int(dt.datetime(*self.end).timestamp()))
        self.data_cfg = cfg[self.run_name]['series']
        self.trading_cfg = cfg[self.run_name]

    def dump_to_csv(self):
        i = len(self.df) - 1
        filename = (
            f'{self.exchange_cls.__name__}'
            f'_{self.data_cfg[i][0]}'
            f'_{self.data_cfg[i][1]}'
            f'_{self.start_ts}_{self.end_ts}.csv')
        if not os.path.exists(f'{self.path}/data/'):
            os.mkdir('data/')
        self.df[i][self.df_expected_cols[1:]].to_csv(f'{self.path}/data/{filename}')

    def _get_data_api(self, period):
        df_list = []
        self.start = self.start + tuple([0 for i in range(len(self.start), 5)])
        start_dt = dt.datetime(*self.start)
        if self.end is None:
            end_dt = None
        else:
            self.end = self.end + tuple([0 for i in range(len(self.end), 5)])
            end_dt = dt.datetime(*self.end)
        while True:
            _end_dt = dt.datetime.utcnow() if end_dt is None else end_dt
            new_df = self.exchange_obj.get_backtest_data(
                self.data_cfg[len(self.df) - 1][0],
                period[2],
                start_dt,
                _end_dt)
            df_list.append(new_df)
            #subprocess.call("clear")
            remaining = int(
                ((_end_dt.timestamp() - start_dt.timestamp())/period[2]))
            logging.info(f'Collecting data - {len(df_list)*df_list[0].shape[0]} periods, remaining: {remaining}')
            secs_til_end = _end_dt.timestamp() - start_dt.timestamp()
            if not self.exchange_obj.max_candles_fetch or secs_til_end < period[2]*self.exchange_obj.max_candles_fetch:
                break
            start_dt = max(new_df.index) + dt.timedelta(seconds=period[2])

        #self.end = [_end_dt.year, _end_dt.month, _end_dt.day, _end_dt.hour, _end_dt.minute]
        self.end = (_end_dt.year, _end_dt.month, _end_dt.day, _end_dt.hour, _end_dt.minute)
        self.end_ts = int(dt.datetime(*self.end).timestamp())
        #subprocess.call("clear")
        self.df.append(pd.concat(df_list))
        #self.df[-1] = self.df[-1].reset_index(drop=True)
        self.df[-1] = self.df[-1].set_index(['datetime'], verify_integrity=True)
        logging.info(f'Data collection finished. Dataframe dimensions: {self.df[-1].shape}')
        self.dump_to_csv()
        return True

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

#        self._align_first_row()
        self.strategy(self.df, self.exchange_obj, self.trading_cfg).run()


def parse_args():
    argp = argparse.ArgumentParser()
    argp.add_argument(
        "-n", "--name", type=str, default='default', help="Backtest name from config file"
    )
    argp.add_argument(
        "-e", "--exchange", type=str, default=None, help="Exchange"
    )
    argp.add_argument(
        "-s", "--strategy", type=str, default=None, help="Strategy name"
    )
    argp.add_argument(
        "-i", "--symbol", "--instrument", type=str, default=None, help="Instrument symbol"
    )
    argp.add_argument(
        "--start", type=int, default=None, nargs='*', help="Start of period"
    )
    argp.add_argument(
        "--end", type=int, default=False, nargs='*', help="End of period"
    )
    argp.add_argument(
        "-p", "--period", type=str, default=None, nargs="*", help="Candle period"
    )
    argp.add_argument(
        "--num_periods", type=int, default=None, nargs="*", help="Number of periods"
    )
    argp.add_argument(
        "-f", "--file", "--files", type=str, default=None, nargs='*', help="Filename(s) within data/"
    )
    argp.add_argument(
        "-t", "--use_testnet", action='store_true', help="Set to False to run on live account"
    )

    args = argp.parse_args()
    return args

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
    print('Running live trader app, check logs at logs/live_trader.log')
    if not os.path.isdir('logs/'):
        os.mkdir('logs')
    logging.basicConfig(filename='logs/live_trader.log', level=logging.DEBUG)
    logging.info(f'{int(dt.datetime.now().timestamp())}: Starting live trader')
    args = parse_args()
    LiveTrader(args).run()
