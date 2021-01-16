import threading
import datetime as dt
import time
import random
import pandas as pd

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

from . import ExchangeAPI

class IbAPI(ExchangeAPI):
    def __init__(self):
        super().__init__()
        self.trading_fee = 0
        self.thread = None
        self.app = None

    def _run_loop(self):
        self.app.run()

    @staticmethod
    def _convert_period(period):
        """
        Convert period (secs) / bar size (s) to IB format
        """
        if period == 1:
            return '1 secs'
        elif period == 5:
            return '5 secs'
        elif period == 10:
            return '10 secs'
        elif period == 15:
            return '15 secs'
        elif period == 30:
            return '30 secs'
        elif period == 60:
            return '1 min'
        elif period == 120:
            return '2 mins'
        elif period == 180:
            return '3 mins'
        elif period == 300:
            return '5 mins'
        elif period == 600:
            return '10 mins'
        elif period == 900:
            return '15 mins'
        elif period == 1200:
            return '20 mins'
        elif period == 1800:
            return '30 mins'
        elif period == 3600:
            return '1 hour'
        elif period == 7200:
            return '2 hours'
        elif period == 10800:
            return '3 hours'
        elif period == 14400:
            return '4 hours'
        elif period == 28800:
            return '8 hours'
        elif period == 86400:
            return '1 day'
        elif period == 604800:
            return '1 week'
        elif period == 2592000 or period == 2678400:
            return '1 month'
        else:
            print('Invalid bar period size')
            return ValueError

    @staticmethod
    def _convert_start_end(start, end):
        """
        Convert 'interval' (start/end times) to IB format
        """
        if dt.datetime.now().timestamp() - end.timestamp() < 60:
            end = ''
            _end = int(dt.datetime.now().timestamp())
        else:
            _end = int(end.timestamp())
            end = f'{_end} S'

        start = _end - start.timestamp()
        if start > 86400*365:
            start = f'{int(start/(86400*365)) + 1} Y'
        elif start > 86400*30:
            start = f'{int(start/(86400*30)) + 1} M'
        elif start > 86400*7:
            start = f'{int(start/(86400*7)) + 1} W'
        elif start > 86400:
            start = f'{int(start/86400) + 1} D'
        else:
            start = f'{int(start)} S'

        return start, end

    def get_backtest_data(self, symbol, period, start, end):

        self.period = IbAPI._convert_period(period)
        self.start, self.end = IbAPI._convert_start_end(start, end)

        self.app = IBapi(symbol, self.period, self.start, self.end)
        self.app.connect('127.0.0.1', 4002, random.randint(0, 999))

        #Start the socket in a thread
        api_thread = threading.Thread(target=self._run_loop, daemon=True)
        api_thread.start()

        time.sleep(10) #Sleep interval to allow time for connection to server

        #Request historical candles
        self.app.get_historical_candles()

        #time.sleep(120) #sleep to allow enough time for data to be returned
        while self.app.df.empty:
            time.sleep(1)

        time.sleep(80)
        self.app.disconnect()

        return self.app.df


class IBapi(EWrapper, EClient):
    def __init__(self, symbol, period, start, end, rth=True):
        EClient.__init__(self, self)
        self.symbol = symbol
        self.period = period
        self.start = start
        self.end = end
        self.rth = 0 if rth else 1
        self.security_type = 'STK'
        self._exchange = 'SMART'
        self.currency = 'USD'
        self.data_type = 'MIDPOINT'
        df_cols = {
            'datetime': [],
            'open': [],
            'high': [],
            'low': [],
            'close': [],
        }
        self.df = pd.DataFrame(df_cols)

    def _update_df(self, data):
        self.df = self.df.append(data, ignore_index=True)

    def historicalData(self, reqId, bar):
        if any((
                self.period.endswith('day'),
                self.period.endswith('week'),
                self.period.endswith('month'))):
            hour = 0
            minute = 0
        else:
            hour = bar.date[10:12]
            minute = bar.date[13:15]
        date = [
            int(bar.date[:4]),
            bar.date[4:6],
            bar.date[6:8],
            hour,
            minute
        ]
        date[1] = int(date[1]) if not date[1][0] == '0' else int(date[1][1:])
        date[2] = int(date[2]) if not date[2][0] == '0' else int(date[2][1:])
        if not (hour == 0 and minute == 0):
            date[3] = int(date[3]) if not date[3][0] == '0' else int(date[3][1:])
            date[4] = int(date[4]) if not date[4][0] == '0' else int(date[4][1:])
        self._update_df({
            'datetime': dt.datetime(date[0], date[1], date[2], date[3], date[4]).timestamp(),
            'open': bar.open,
            'high': bar.high,
            'low': bar.low,
            'close': bar.close,
        })

    def get_historical_candles(self):
        self.reqHistoricalData(
            random.randint(0, 999),
            self._create_contract_obj(),
            self.end,
            self.start,
            self.period,
            self.data_type,
            not self.rth,
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

    time.sleep(120) #sleep to allow enough time for data to be returned
    app.disconnect()
