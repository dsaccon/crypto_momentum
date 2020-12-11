import subprocess
import json 
import pandas as pd
import datetime as dt
import argparse
import matplotlib.pyplot as plt
import importlib
import backtrader as bt
import backtrader.analyzers as btanalyzers
import strategies


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
        self.symbol = symbol
        self.start = start
        self.end = end
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


    def dump_to_csv(self):
        filename = binance_btcusdt_1m_20200101_20201201.csv
        start = f'{self.start[0]}{self.start[1]}{self.start[2]}'
        end = ''
        filename = (
            f'{self.exchange}_{self.symbol.lower()}_{self.period}'
            f'{self}')

    def get_data(self):
        df_list = []
        start_dt = dt.datetime(self.start[0], self.start[1], self.start[2])
        if self.end is None:
            end_dt = None
        else:
            end_dt = dt.datetime(self.end[0], self.end[1], self.end[2])
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
         
        subprocess.call("clear")
        print('Data collection done')
        df = pd.concat(df_list)
        df.shape
        return df


    def run(self):
        cerebro = bt.Cerebro()
         
        data = bt.feeds.PandasData(dataname = self.get_data())
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
        "-e", "--exchange", type=str, default='binance', help="Exchange"
    )
    argp.add_argument(
        "-s", "--strategy", type=str, default='MaCrossStrategy', help="Strategy name"
    )
    argp.add_argument(
        "-i", "--instrument", type=str, default='BTCUSDT', help="Instrument symbol"
    )
    argp.add_argument(
        "--start", type=int, default=[2020, 9, 1], nargs=3, help="Start of period"
    )
    argp.add_argument(
        "--end", type=int, default=None, nargs=3, help="End of period"
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
