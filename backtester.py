import subprocess
import json 
import os
import pandas as pd
import datetime as dt
import argparse
import importlib
import logging
import concurrent.futures
import matplotlib.pyplot as plt
import backtrader as bt
import backtrader.analyzers as btanalyzers
import strategies


class DataCollectionError(Exception):
    pass


class NotSupportedError(Exception):
    pass


class Object(object):
    pass


class Base:
    def __init__(self, args):

        self.run_name = args.name
        self.path = '/'.join(os.path.abspath(__file__).split('/')[:-1])

        self._load_config()

        # Overwrite config file settings from cli args
        self.csv_file = args.file
        if args.symbol:
            self.data_cfg = [[args.symbol, c[1]] for c in self.data_cfg]
            if args.symbol.endswith('USDT'):
                self.trading_cfg['symbol'] = (args.symbol[:3], args.symbol[3:])
            else:
                logging.critical(f'Improper symbol entered on cli: {args.symbol}')
                raise ValueError
        if args.period:
            self.data_cfg = [[c[0], args.period[i]] for i, c in enumerate(self.data_cfg)]
        if args.num_periods:
            self.trading_cfg['num_periods'] = args.num_periods
        if args.asset_type:
            self.trading_cfg['asset_type'] = args.asset_type
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
        self.trading_cfg['series'] = self.data_cfg

        self.df_expected_cols = ['datetime', 'open', 'high', 'low', 'close']
        self.df = []

        logging.info(f'Running {self.__class__.__name__} with params: {self.trading_cfg}')


    def _load_config(self):
        """
        Load config settings from config.json
        """
        with open(f'{self.path}/config.json', 'r') as f:
            cfg = json.load(f)

        if self.__class__.__name__ == 'LiveTrader':
            self.strategy = getattr(strategies, f"Live{cfg[self.run_name]['strategy']}")
        else:
            self.strategy = getattr(strategies, cfg[self.run_name]['strategy'])
        _exch = cfg[self.run_name]['exchange']
        _path = f"exchanges.{_exch}"
        _exch_obj = importlib.import_module(_path)
        self.exchange_cls = getattr(_exch_obj, f'{_exch[0].upper()}{_exch[1:]}API')
        self.exchange_obj = self.exchange_cls()
        self.start = tuple(cfg[self.run_name]['start'])
        _end = cfg[self.run_name]['end']
        self.end = tuple(_end) if _end else _end
        self.start_capital = cfg[self.run_name]['start_capital']
        if self.end:
            self.end_ts = str(int(dt.datetime(*self.end).timestamp()))
        self.data_cfg = cfg[self.run_name]['series']
        self.trading_cfg = cfg[self.run_name]


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
                _end_dt,
                asset_type=self.trading_cfg['asset_type'])
            df_list.append(new_df)
            remaining = int(
                ((_end_dt.timestamp() - start_dt.timestamp())/period[2]))
            logging.debug(f'Collecting data - {len(df_list)*df_list[0].shape[0]} periods, remaining: {remaining}')
            secs_til_end = _end_dt.timestamp() - start_dt.timestamp()
            if not self.exchange_obj.max_candles_fetch or secs_til_end < period[2]*self.exchange_obj.max_candles_fetch:
                break
            start_dt = max(new_df.index) + dt.timedelta(seconds=period[2])

        self.end = (_end_dt.year, _end_dt.month, _end_dt.day, _end_dt.hour, _end_dt.minute)
        self.end_ts = int(dt.datetime(*self.end).timestamp())
        self.df.append(pd.concat(df_list))
        self.df[-1] = self.df[-1].set_index(['datetime'], verify_integrity=True)
        logging.info(f'Data collection finished. Dataframe dimensions: {self.df[-1].shape}')
        self.dump_to_csv()

        return True


    def _validate_data(self):
        """
        Do extra checks on data retrived from API
            - Verify all gaps between timestamps are as expected
            - Verfiy traded volume is not zero
        """
        time_checks = [False for _ in self.df]
        vol_checks = [False for _ in self.df]
        for i, _df in enumerate(self.df):
            # Check for timestamp gaps
            _shifted_col = [list(_df.index)[0] - self.data_cfg[i][2]] + list(_df.index)[:-1]
            tmp_df = pd.DataFrame(_shifted_col, columns=['time_prv']).set_index(_df.index)
            tmp_df['time'] = list(_df.index)
            time_deltas = tmp_df.apply(
                lambda r: True
                if r.time - r.time_prv == self.data_cfg[i][2]
                else False, axis=1)
            if not time_deltas.all():
                # There are gaps
                time_checks[i] = [
                    tmp_df['time'].iloc[i]
                    for i, chk in enumerate(time_deltas)
                    if not chk
                ]

            # Check for 0 volume
            ### Not working yet ###
            if 0 in _df['volume']:
                vol_checks[i] = [
                    True
                    for vol in _df['volume']
                    if not vol == 0
                ]
        for i, check in enumerate(time_checks):
            if check:
                t = [
                    dt.datetime.fromtimestamp(_t).strftime('%y-%m-%d:%H:%M')
                    for _t in time_checks[i]
                ]
                msg = f"Gaps in {self.trading_cfg['series'][i][1]} series: {t}"
                logging.warning(msg)

        return not any(time_checks)


    def _trim_dataframes(self):
        # If more than one series, align final timestamps

        # Note this assumes shorter interval series is index 0, longer is 1
        while self.df[0].index[-1] >= self.df[1].index[-1] + self.data_cfg[1][-1]:
            self.df[0].drop(self.df[0].tail(1).index, inplace=True)


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


class Backtest(Base):

    def _get_data_csv(self):
        for f in self.csv_file:
            self.df.append(pd.read_csv(f'{self.path}/data/{f}'))
            cols = self.df[-1].columns.values.tolist()
            for i, col in enumerate(cols):
                # Normalize col names
                if not col == self.df_expected_cols[i]:
                    self.df[-1].rename(columns={col: self.df_expected_cols[i]}, inplace=True)
            # Adjust datetimes to 10 digit epoch
            if len(str(int(self.df[-1]['datetime'][0]))) == 10:
                dt_col = self.df[-1]['datetime']
            elif len(str(int(self.df[-1]['datetime'][0]))) == 13:
                dt_col = self.df[-1]['datetime']/1000
            else:
                #print('Something not right with datetime values')
                logging.info('Something not right with datetime values')
                raise ValueError
#            dt_col = self.df[-1]['datetime']/1000
            self.df[-1]['datetime'] = dt_col.astype(int)
            self.df[-1] = self.df[-1].set_index(['datetime'], verify_integrity=True)

    def get_data(self):
        # Fetch data from exchange
        if self.csv_file is None:
            for series in self.data_cfg:
                if not self._get_data_api(series):
                    return False
        else:
            if not len(self.data_cfg) == len(self.csv_file):
                #print(f'Number of csv files provided should be {len(self.data_cfg)}')
                logging.info(f'Number of csv files provided should be {len(self.data_cfg)}')
                #print(self.data_cfg)
                #print(self.csv_file)
                logging.info(self.data_cfg)
                logging.info(self.csv_file)
                raise ValueError
            self._get_data_csv()

        # Clean data
        if len(self.df) == 1:
            return True
        elif len(self.df) == 2:
            #self._trim_dataframes()
            if not self._validate_data():
                logging.critical('There is something wrong with the source data')
                raise DataCollectionError
        else:
            raise NotSupportedError

        return True

    def _get_best_csv(self, symbol, period):
        # Figures out which csv file has the biggest range of data in desired range
        filename_prefix = f'{self.exchange_cls.__name__}_{symbol.lower()}_{period}'
        files = [f for f in os.listdir('data/') if f.startswith(filename_prefix)]
        files = [f for f in files if self.start_ts >= int(f.strip('.csv').split('_')[-2])]
        best_file = files[0]
        _end = self.end if self.end else int(dt.datetime.utcnow().timestamp())
        for f in files:
            if _end > int(f.strip('.csv').split('_')[-2]):
                csv_end = int(f.strip('.csv').split('_')[-1])
            else:
                csv_end = _end
            _best = best_file.strip('.csv').split('_')
            if csv_end - self.start_ts > int(_best[-1]) - int(_best[-2]):
                best_file = f
        return best_file

    def load_csv(self):
        return False

    def run(self):
        if self.get_data():
            data = self.df
        else:
            raise DataCollectionError
        self.strategy(self.df, self.exchange_obj, self.trading_cfg).run()

    def run_backtrader(self):
        cerebro = bt.Cerebro()

        if self.load_local_data() or self.get_data():
            data = bt.feeds.PandasData(dataname=self.df)
        else:
            raise DataCollectionError
        cerebro.adddata(data)
    
        start_cash = 1000000
        cerebro.addstrategy(self.strategy)
        cerebro.broker.setcash(start_cash)
         
        cerebro.addsizer(bt.sizers.PercentSizer, percents = 50)
        cerebro.addanalyzer(btanalyzers.SharpeRatio, _name = "sharpe")
        cerebro.addanalyzer(btanalyzers.Transactions, _name = "trans")

        back = cerebro.run()

        print('P&L:', cerebro.broker.getvalue() - start_cash) # Ending balance
        print('analysis:', back[0].analyzers.sharpe.get_analysis()) # Sharpe
        print('number of trades:', len(back[0].analyzers.trans.get_analysis())) # Number of Trades
        cerebro.plot()


def parse_args():
    argp = argparse.ArgumentParser()
    argp.add_argument(
        "-n", "--name", type=str, default='default', help="Settings name from config file"
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
        "--start", type=int, default=None, nargs='*', help="Start of period. For backtesting only"
    )
    argp.add_argument(
        "--end", type=int, default=False, nargs='*', help="End of period. For backtesting only"
    )
    argp.add_argument(
        "-a", "--asset_type", type=str, default=None, help="Asset type (spot, futures)"
    )
    argp.add_argument(
        "-p", "--period", type=str, default=None, nargs="*", help="Candle period"
    )
    argp.add_argument(
        "--num_periods", type=int, default=None, nargs="*", help="Number of periods"
    )
    argp.add_argument(
        "-f", "--file", "--files", type=str, default=None, nargs='*', help="Filename(s) within data folder. For backtesting only"
    )
    argp.add_argument(
        "-t", "--use_testnet", action='store_true', help="Set to False to run on live account. For live trading only"
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
    logfile = 'logs/backtester.log'
    print(f'Running backtester, check logs at {logfile}')
    if not os.path.isdir('logs/'):
        os.mkdir('logs')
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info(f'{int(dt.datetime.now().timestamp())}: Starting backtester')
    args = parse_args()
    Backtest(args).run()
