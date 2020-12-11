import subprocess
import json 
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import importlib
import backtrader as bt
import backtrader.analyzers as btanalyzers
from strategies import *


class Backtest:
    def __init__(self, exchange, strategy):
        self.strategy = strategy
        _path = f'exchanges.{exchange}'
        _exch = importlib.import_module(_path)
        self.exchange_api = getattr(_exch, f'{exchange[0].upper()}{exchange[1:]}API')()

#    def get_binance_bars(self, symbol, interval, startTime, endTime):
#     
#        url = "https://api.binance.com/api/v3/klines"
#     
#        startTime = str(int(startTime.timestamp() * 1000))
#        endTime = str(int(endTime.timestamp() * 1000))
#        limit = '1000'
#     
#        req_params = {"symbol" : symbol, 'interval' : interval, 'startTime' : startTime, 'endTime' : endTime, 'limit' : limit}
#     
#        df = pd.DataFrame(json.loads(requests.get(url, params = req_params).text))
#     
#        if (len(df.index) == 0):
#            return None
#         
#        df = df.iloc[:, 0:6]
#        df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
#     
#        df.open      = df.open.astype("float")
#        df.high      = df.high.astype("float")
#        df.low       = df.low.astype("float")
#        df.close     = df.close.astype("float")
#        df.volume    = df.volume.astype("float")
#        
#        df['adj_close'] = df['close']
#         
#        df.index = [dt.datetime.fromtimestamp(x / 1000.0) for x in df.datetime]
#     
#        return df


    def get_data(self):
        df_list = []
        last_datetime = dt.datetime(2020, 11, 1)
        while True:
            new_df = self.exchange_api.get_historical_candles(
                'ETHUSDT',
                '1m',
                last_datetime,
                dt.datetime.now())
            df_list.append(new_df)
            subprocess.call("clear")
            print(f'Collecting data: {len(df_list)*df_list[0].shape[0]} periods')
            time_delta = dt.datetime.now().timestamp() - last_datetime.timestamp() ### tmp
            if time_delta < 60*1000: ### tmp
                break
            last_datetime = max(new_df.index) + dt.timedelta(0, 1)
         
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


if __name__ == '__main__':
    Backtest('binance', MaCrossStrategy).run()
