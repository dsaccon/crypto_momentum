import os
import datetime as dt
import time
import json
import logging
import traceback
from functools import wraps
import requests
import pandas as pd
from . import ExchangeAPI


class NotImplementedError(Exception):
    pass


def meta(wait=1):
    """
        Retry on API call failure
    """
    def deco_meta(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            x = None
            while True:
                try:
                    x = func(*args, **kwargs)
                    break
                except Exception as ex:
                    time.sleep(wait)
            return x
        return wrapper
    return deco_meta


class OkexAPI(ExchangeAPI):
    NAME = 'okex'
    API_URL = 'https://www.okex.com'
    API_URL_AWS = 'https://aws.okex.com'
    API_URL_FUTURES = API_URL
    API_URL_FUTURES_AWS = API_URL_AWS

    VALID_CANDLE_PERIODS = {
        60, 180, 300, 900, 1800, 3600, 7200,
        14400, 21600, 43200, 86400, 604800,
        2678400, 8035200, 16070400, 31536000
    }

    VALID_HISTORICAL_CANDLE_PERIODS = {
        60, 180, 300, 900, 1800, 3600, 7200,
        14400, 21600, 43200, 86400, 604800
    }

    def __init__(self, use_testnet=False):
        self.logger = logging.getLogger(__name__)
        self.max_candles_fetch = 300

        self.API_URL = OkexAPI.API_URL_AWS
        self.API_URL_FUTURES = OkexAPI.API_URL_FUTURES_AWS

        if use_testnet:
            if 'OKEX_TEST_KEY' in os.environ:
                self._API_KEY = os.environ['OKEX_TEST_KEY']
            else:
                self._API_KEY = None
            if 'OKEX_TEST_SECRET' in os.environ:
                self._API_SECRET = os.environ['OKEX_TEST_SECRET']
            else:
                self._API_SECRET = None
        else:
            if 'OKEX_KEY' in os.environ:
                self._API_KEY = os.environ['OKEX_KEY']
            else:
                self._API_KEY = None
            if 'OKEX_SECRET' in os.environ:
                self._API_SECRET = os.environ['OKEX_SECRET']
            else:
                self._API_SECRET = None
        if self._API_KEY and self._API_SECRET:
            pass ### placeholder
        else:
            pass

        if use_testnet:
            pass ### placeholder

    # Custom functions

    def get_backtest_data(self, *args, **kwargs):
        return self.get_historical_candles(*args, **kwargs)


    # PUBLIC ENDPOINTS (SPOT & FUTURES)
    @meta(wait=1)
    def get_historical_candles(
            self,
            symbol: str,
            period: int,
            startTime: dt,
            endTime: dt = dt.datetime.utcnow(),
            asset_type: str = 'spot',
            completed_only: bool = True) -> pd:

        """
            Arguments
            ---------
            symbol (str): symbol name of instrument. E.g. 'btcusdt'
            period (int): period length (secs)
            startTime (dt): series start time in datetime format
            endTime (dt): series end time in datetime format
            completed_only: do not show candles that have not been completed

            Returns
            ---------
            df (pd): pandas dataframe with collected data from API
        """
        if not period in OkexAPI.VALID_CANDLE_PERIODS:
            raise ValueError
        if not symbol.endswith('USDT'):
            raise ValueError

        if asset_type == 'spot':
            base_uri = self.API_URL
            endpoint = ''
            instrument = '' 
            raise NotImplementedError
        elif asset_type == 'futures':
            base_uri = self.API_URL_FUTURES
            instrument = f'{symbol[:-4]}-{symbol[-4:]}-SWAP' 
            endpoint_h = f'/api/swap/v3/instruments/{instrument}/history/candles'
            endpoint = f'/api/swap/v3/instruments/{instrument}/candles'
        else:
            raise ValueError
        uri = f'{base_uri}{endpoint}'
        req_params = {
            'instrument_id' : instrument,
            'start' : startTime.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'end' : endTime.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            'granularity' : period,
            'limit' : self.max_candles_fetch
        }

        time_before = dt.datetime.utcnow()
        resp = json.loads(requests.get(uri, params=req_params).text)
        print(resp) ### tmp
        #df = pd.DataFrame(json.loads(requests.get(uri, params=req_params).text))
        df = pd.DataFrame(resp)

        if (len(df.index) == 0):
            return None

        df = df.iloc[:, 0:6]
        df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']

        df.open      = df.open.astype("float")
        df.high      = df.high.astype("float")
        df.low       = df.low.astype("float")
        df.close     = df.close.astype("float")
        df.volume    = df.volume.astype("float")

        df.index = [
            dt.datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ")
            for t in df.datetime
        ]

        df['completed'] = df.datetime.apply(lambda t: True if time_before > dt.datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ") + dt.timedelta(seconds=period) else False)
        if completed_only and df['completed'].iloc[-1] == False:
            df = df.iloc[:-1]
        return df
