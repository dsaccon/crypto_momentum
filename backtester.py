import subprocess
import json 
import os
import pandas as pd
import datetime as dt
import argparse
import matplotlib.pyplot as plt
import importlib
import backtrader as bt
import backtrader.analyzers as btanalyzers
import strategies


class DataCollectionError(Exception):
    pass


class Backtest:
    def __init__(
            self,
            exchange,
            strategy,
            symbol,
            start,
            end,
            period,
            ):

        self.strategy = getattr(strategies, strategy)
        _path = f'exchanges.{exchange}'
        _exch = importlib.import_module(_path)
        self.exchange_api = getattr(_exch, f'{exchange[0].upper()}{exchange[1:]}API')()
        self.exchange = exchange
        self.symbol = symbol
        self.start = tuple(start)
        self.start_ts = int(dt.datetime(*self.start).timestamp())
        self.end = tuple(end) if end else end
        if end:
            self.end_ts = str(int(dt.datetime(*self.end).timestamp()))
        self.period = period
        if self.period.endswith('s'):
            _mult = 1
        elif self.period.endswith('m'):
            _mult = 60
        elif self.period.endswith('h'):
            _mult = 60*60
        elif self.period.endswith('d'):
            _mult = 60*60*24
        self.period_secs = int(self.period[:-1])*_mult
        self.df = None


    def dump_to_csv(self):
        filename = (
            f'{self.exchange}_{self.symbol.lower()}_{self.period}'
            f'_{self.start_ts}_{self.end_ts}.csv')
        self.df.to_csv(f'data/{filename}')


    def get_data(self):
        df_list = []
        self.start = self.start + tuple([0 for i in range(len(self.start), 5)])
        start_dt = dt.datetime(*self.start)
        if self.end is None:
            end_dt = None
        else:
            self.end = self.end + [0 for i in range(len(self.end), 5)]
            end_dt = dt.datetime(*self.end)
        while True:
            _end_dt = dt.datetime.now() if end_dt is None else end_dt
            new_df = self.exchange_api.get_historical_candles(
                self.symbol,
                self.period,
                start_dt,
                _end_dt)
            df_list.append(new_df)
            subprocess.call("clear")
            remaining = int(
                ((_end_dt.timestamp() - start_dt.timestamp())/self.period_secs))
            print(f'Collecting data - {len(df_list)*df_list[0].shape[0]} periods, remaining: {remaining}')
            secs_til_end = _end_dt.timestamp() - start_dt.timestamp()
            if secs_til_end < self.period_secs*self.exchange_api.max_candles_fetch:
                break
            start_dt = max(new_df.index) + dt.timedelta(0, 1)

        self.end = [_end_dt.year, _end_dt.month, _end_dt.day, _end_dt.hour, _end_dt.minute]
        self.end_ts = int(dt.datetime(*self.end).timestamp())
        subprocess.call("clear")
        self.df = pd.concat(df_list)
        print(f'Data collection finished. Dataframe dimensions: {self.df.shape}')
        self.dump_to_csv()
        return True


    def load_data(self):
        filename_prefix = f'{self.exchange}_{self.symbol.lower()}_{self.period}'
        files = [f for f in os.listdir('data/') if f.startswith(filename_prefix)]
        files = [f for f in files if self.start_ts >= int(f.strip('.csv').split('_')[-2])]
        best_file = files[0]
        _end = self.end if self.end else int(dt.datetime.now().timestamp())
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
            #data = bt.feeds.PandasData(dataname=self.df)
            data = self.df
        else:
            raise DataCollectionError

        self.strategy(self.df).run()

def parse_args():
    argp = argparse.ArgumentParser()
    argp.add_argument(
        "-e", "--exchange", type=str, default='binance', help="Exchange"
    )
    argp.add_argument(
        "-s", "--strategy", type=str, default='MaCrossStrategy', help="Strategy name"
    )
    argp.add_argument(
        "-i", "--instrument", "--symbol", type=str, default='BTCUSDT', help="Instrument symbol"
    )
    argp.add_argument(
        "--start", type=int, default=[2020, 9, 1, 0, 0], nargs='*', help="Start of period"
    )
    argp.add_argument(
        "--end", type=int, default=None, nargs='*', help="End of period"
    )
    argp.add_argument(
        "-p", "--period", type=str, default='1m', help="Candle period"
    )

    args = argp.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()
    Backtest(
        args.exchange,
        args.strategy,
        args.instrument,
        args.start,
        args.end,
        args.period,
    ).run()
