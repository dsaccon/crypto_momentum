import threading
import datetime as dt
import time
import random
import pandas as pd

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

class IbAPI:
    def __init__(self):
        self.thread = None

    def _run(self):
        self.thread.run()

    def get_backtest_data(self, symbol, period, start, end):
        # Need to translate period
        self.thread = IBapi(symbol, '1 M', '1 min')
        self.thread.connect('127.0.0.1', 4002, random.randint(0, 999))

        #Start the socket in a thread
        api_thread = threading.Thread(target=self._run, daemon=False)
        api_thread.start()

        time.sleep(1) #Sleep interval to allow time for connection to server

        #Request historical candles
        self.thread.get_historical_candles()
        
        time.sleep(30) #Sleep interval to allow time for connection to server
        self.thread.disconnect()
        return self.thread.df

class IBapi(EWrapper, EClient):
    def __init__(self, symbol, interval, period):
        EClient.__init__(self, self)
        self.symbol = symbol
        self.interval = interval
        self.period = period
        self.security_type = 'STK'
        self._exchange = 'SMART'
        self.currency = 'USD'
        self.data_type = 'MIDPOINT'
        self.thread = None
        df_cols = {
            'time': [],
            'open': [],
            'high': [],
            'low': [],
            'close': [],
            'volume': [],
        }
        self.df = pd.DataFrame(df_cols)

    def _run(self):
        self.thread.run()

    def _run_thread(self):
        self.connect('127.0.0.1', 4002, random.randint(0, 999))

        #Start the socket in a thread
        _thread = threading.Thread(target=self._run_thread, daemon=True)
        _thread.start()
        time.sleep(1)
        self.get_historical_candles()

    def _update_df(self, data):
        self.df = self.df.append(data, ignore_index=True)

    def historicalData(self, reqId, bar):
#        print(
#            f'HistoricalData: {reqId}, Date: {bar.date},'
#            f'Open: {bar.open}, High: {bar.high},'
#            f'Low: {bar.low}, Close: {bar.close}')
        date = [int(bar.date[:4]), bar.date[4:6], bar.date[6:8]]
        date[1] = int(date[1]) if not date[1][0] == '0' else int(date[1][1:])
        date[2] = int(date[2]) if not date[2][0] == '0' else int(date[2][1:])
        self._update_df({
            'time': dt.datetime(date[0], date[1], date[2]).timestamp(),
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
            'volume': bar.volume
        })

    def get_backtest_data(self, symbol, period, start):
        pass 

    def get_historical_candles(self):
        self.reqHistoricalData(
            random.randint(0, 999),
            self._create_contract_obj(),
            '',
            '1 M',
            '1 min',
            self.data_type,
            0,
            1,
            False,
            [])

    def _create_contract_obj(self):
        contract = Contract()
        contract.symbol = self.symbol
        contract.secType = self.security_type
        contract.exchange = self._exchange
        contract.currency = self.currency
        return contract

def get_data(symbol):

    def _run_loop():
        app.run()

    app = IBapi('FB', '1 M', '1 min')
    app.connect('127.0.0.1', 4002, random.randint(0, 999))

    #Start the socket in a thread
    api_thread = threading.Thread(target=_run_loop, daemon=True)
    api_thread.start()

    time.sleep(1) #Sleep interval to allow time for connection to server

    #Request historical candles
    app.get_historical_candles()

    time.sleep(5) #sleep to allow enough time for data to be returned
    app.disconnect()
