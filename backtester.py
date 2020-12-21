import subprocess
import json 
import os
import pandas as pd
import datetime as dt
import argparse
import importlib
import concurrent.futures
import matplotlib.pyplot as plt
import backtrader as bt
import backtrader.analyzers as btanalyzers
import strategies


class DataCollectionError(Exception):
    pass


class NotSupportedError(Exception):
    pass


class Backtest:
    def __init__(self, args):
        self.run_name = args.name

        self._load_config()

        # Overwrite config file settings from cli args
        if args.symbol:
            self.symbol = args.symbol
            self.data_cfg = [[self.symbol, c[1]] for c in self.data_cfg]
        self.start = tuple(args.start) if args.start else self.start
        self.start_ts = int(dt.datetime(*self.start).timestamp())
        if args.end is not False:
            self.end = tuple(args.end) if args.end else args.end
            if self.end:
                self.end_ts = str(int(dt.datetime(*self.end).timestamp()))
        self.candle_period = args.period

        for _cfg in self.data_cfg:
            if _cfg[1].endswith('s'):
                _mult = 1
            elif _cfg[1].endswith('m'):
                _mult = 60
            elif _cfg[1].endswith('h'):
                _mult = 60*60
            elif _cfg[1].endswith('d'):
                _mult = 60*60*24
            _cfg.append(int(_cfg[1][:-1])*_mult)
            #self.candle_period_secs = int(self.candle_period[:-1])*_mult
        self.df = []

    def _load_config(self):
        with open('config.json', 'r') as f:
            cfg = json.load(f)

        self.strategy = getattr(strategies, cfg[self.run_name]['strategy'])
        _exch = cfg[self.run_name]['exchange']
        _path = f"exchanges.{_exch}"
        _exch_obj = importlib.import_module(_path)
        self.exchange = getattr(_exch_obj, f'{_exch[0].upper()}{_exch[1:]}API')()
        self.symbol = cfg[self.run_name]['symbol']
        self.start = tuple(cfg[self.run_name]['start'])
        _end = cfg[self.run_name]['end']
        self.end = tuple(_end) if _end else _end
        if self.end:
            self.end_ts = str(int(dt.datetime(*self.end).timestamp()))
        self.data_cfg = cfg[self.run_name]['series']

    def dump_to_csv(self):
        for df in self.df:
            for series in self.strategy.data_cfg:
                filename = (
                    f'{self.exchange}'
                    f'_{self.symbol.lower()}'
                    f'_{series[1]}'
                    f'_{self.start_ts}_{self.end_ts}.csv')
                df.to_csv(f'data/{filename}')


    def _get_data(self, period):
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
            new_df = self.exchange.get_backtest_data(
                self.symbol,
                period[2],
                start_dt,
                _end_dt)
#            print('min:', min(new_df.index), 'max:', max(new_df.index)) ### tmp
#            print('start_dt', start_dt, 'end_dt', _end_dt) ### tmp
            df_list.append(new_df)
            subprocess.call("clear")
            remaining = int(
                ((_end_dt.timestamp() - start_dt.timestamp())/period[2]))
            print(f'Collecting data - {len(df_list)*df_list[0].shape[0]} periods, remaining: {remaining}')
            secs_til_end = _end_dt.timestamp() - start_dt.timestamp()
            if not self.exchange.max_candles_fetch or secs_til_end < period[2]*self.exchange.max_candles_fetch:
                break
            start_dt = max(new_df.index) + dt.timedelta(seconds=period[2])

        #self.end = [_end_dt.year, _end_dt.month, _end_dt.day, _end_dt.hour, _end_dt.minute]
        self.end = (_end_dt.year, _end_dt.month, _end_dt.day, _end_dt.hour, _end_dt.minute)
        self.end_ts = int(dt.datetime(*self.end).timestamp())
        subprocess.call("clear")
        self.df.append(pd.concat(df_list))
        self.df[-1] = self.df[-1].reset_index(drop=True)
        print(f'Data collection finished. Dataframe dimensions: {self.df[-1].shape}')
        self.dump_to_csv()
        return True

    def _trim_dataframes(self):
        # If more than one series, make sure final timestamps line up

        # Note this assumes shorter interval series is index 0, longer is 1
        while self.df[0].datetime.iloc[-1] > self.df[1].datetime.iloc[-1]:
            self.df[0].drop(self.df[0].tail(1).index, inplace=True)

    def _check_gaps(self):
        """
        With multiple series, candle alignment can get thrown off with
        any gaps in the candles. This will clean the dataset to be able
        to skip over these gaps
        """

        for i, _df in enumerate(self.df):
            shifted_col = _df.datetime.shift(1)
            shifted_col.iloc[0] = shifted_col.iloc[1] - self.data_cfg[i][2]
            time_deltas = _df.apply(
                lambda r: True
                if r.datetime - shifted_col.iloc[r.name] == self.data_cfg[i][2]
                else False, axis=1)
            if time_deltas.all():
                # Data looks good
                continue
            else:
                # There are gaps
                raise DataCollectionError

    def get_data(self):
        # Fetch data from exchange
        for series in self.data_cfg:
            if not self._get_data(series):
                return False

        # Clean data
        if len(self.df) == 1:
            return True
        elif len(self.df) == 2:
            self._trim_dataframes()
            #self._check_gaps()
        else:
            raise NotSupportedError

        return True

    def load_data(self, symbol, period):
        filename_prefix = f'{self.exchange.__name__}_{symbol.lower()}_{period}'
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


    def run(self):
        if self.get_data():
            data = self.df
        else:
            raise DataCollectionError

        self.strategy(self.df, self.exchange).run()

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
        "-p", "--period", type=str, default=None, help="Candle period/interval"
    )

    args = argp.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()
    Backtest(args).run()
