import datetime as dt
import json
import pandas as pd
import requests

class ExchangeAPI:
    def __init__(self):
        self.base_uri = ''
        self.max_candles_fetch = None
        self.trading_fee = None

class BinanceAPI(ExchangeAPI):
    def __init__(self):
        self.base_uri = 'https://api.binance.com/api/v3/'
        self.max_candles_fetch = 1000
        self.trading_fee = 0.00075

    def get_historical_candles(self, symbol, interval, startTime, endTime):

        endpoint = 'klines'
        _uri = f'{self.base_uri}{endpoint}'
        startTime = str(int(startTime.timestamp() * 1000))
        endTime = str(int(endTime.timestamp() * 1000))

        req_params = {
            'symbol' : symbol,
            'interval' : interval,
            'startTime' : startTime,
            'endTime' : endTime,
            'limit' : self.max_candles_fetch
        }

        df = pd.DataFrame(json.loads(requests.get(_uri, params = req_params).text))

        if (len(df.index) == 0):
            return None

        df = df.iloc[:, 0:6]
        df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']

        df.open      = df.open.astype("float")
        df.high      = df.high.astype("float")
        df.low       = df.low.astype("float")
        df.close     = df.close.astype("float")
        df.volume    = df.volume.astype("float")

        df['adj_close'] = df['close']

        df.index = [dt.datetime.fromtimestamp(x/1000.0) for x in df.datetime]

        return df
