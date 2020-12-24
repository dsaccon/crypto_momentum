import os
import datetime as dt
import time
import json
from operator import itemgetter
import hashlib
import hmac
import requests
import pandas as pd
from binance.client import Client as BinanceClient
from . import ExchangeAPI

"""
order = client.create_test_order(symbol='BNBBTC', side='BUY', type='MARKET', quantity=1)

POST /api/v3/order/test
POST /api/v3/order (HMAC SHA256)

"""

class BinanceAPIException(Exception):

    def __init__(self, response):
        self.code = 0
        try:
            json_res = response.json()
        except ValueError:
            self.message = 'Invalid JSON error message from Binance: {}'.format(response.text)
        else:
            self.code = json_res['code']
            self.message = json_res['msg']
        self.status_code = response.status_code
        self.response = response
        self.request = getattr(response, 'request', None)

    def __str__(self):  # pragma: no cover
        return 'APIError(code=%s): %s' % (self.code, self.message)


class BinanceRequestException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'BinanceRequestException: %s' % self.message


class BinanceAPI(ExchangeAPI):
    name = 'binance'

    def __init__(self):
        self.base_uri = 'https://api.binance.com/api/v3/'
        self.max_candles_fetch = 1000
        self.trading_fee = 0.00075
        self.API_KEY = os.environ['BINANCE_KEY']
        self.API_SECRET = os.environ['BINANCE_SECRET']
        self.session = self._init_session()
        self.external_client = BinanceClient(self.API_KEY, self.API_SECRET)

        self.API_URL = 'https://api.binance.com/api'

    def _init_session(self):
        session = requests.session()
        session.headers.update({'Accept': 'application/json',
                                'User-Agent': 'binance/python',
                                'X-MBX-APIKEY': self.API_KEY})
        return session

    def _get(self, path, signed=False, version='v1', **kwargs):
        return self._request_api('get', path, signed, version, **kwargs)

    def _post(self, path, signed=False, version='v1', **kwargs):
        return self._request_api('post', path, signed, version, **kwargs)

    def _request_api(self, method, path, signed=False, version='v1', **kwargs):
        uri = self._create_api_uri(path, signed, version)
        return self._request(method, uri, signed, **kwargs)

    def _create_api_uri(self, path, signed=True, version='v1'):
        v = 'v3' if signed else version
        return self.API_URL + '/' + v + '/' + path

    def _generate_signature(self, data):
        ordered_data = self._order_params(data)
        query_string = '&'.join(["{}={}".format(d[0], d[1]) for d in ordered_data])
        m = hmac.new(self.API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
        return m.hexdigest()

    def _handle_response(self):
        """Internal helper for handling API responses from the Binance server.
        Raises the appropriate exceptions when necessary; otherwise, returns the
        response.
        """
        if not str(self.response.status_code).startswith('2'):
            raise BinanceAPIException(self.response)
        try:
            return self.response.json()
        except ValueError:
            raise BinanceRequestException('Invalid Response: %s' % self.response.text)

    def _request(self, method, uri, signed, force_params=False, **kwargs):

        # set default requests timeout
        kwargs['timeout'] = 10

        data = kwargs.get('data', None)
        if data and isinstance(data, dict):
            kwargs['data'] = data

            # find any requests params passed and apply them
            if 'requests_params' in kwargs['data']:
                # merge requests params into kwargs
                kwargs.update(kwargs['data']['requests_params'])
                del(kwargs['data']['requests_params'])

        if signed:
            # generate signature
            kwargs['data']['timestamp'] = int(time.time() * 1000)
            kwargs['data']['signature'] = self._generate_signature(kwargs['data'])

        # sort get and post params to match signature order
        if data:
            # sort post params
            kwargs['data'] = self._order_params(kwargs['data'])
            # Remove any arguments with values of None.
            null_args = [i for i, (key, value) in enumerate(kwargs['data']) if value is None]
            for i in reversed(null_args):
                del kwargs['data'][i]

        # if get request assign data array to params value for requests lib
        if data and (method == 'get' or force_params):
            kwargs['params'] = '&'.join('%s=%s' % (data[0], data[1]) for data in kwargs['data'])
            del(kwargs['data'])

        self.response = getattr(self.session, method)(uri, **kwargs)
        return self._handle_response()

    def _order_params(self, data):
        """Convert params to list with signature as last element

        :param data:
        :return:

        """
        has_signature = False
        params = []
        for key, value in data.items():
            if key == 'signature':
                has_signature = True
            else:
                params.append((key, value))
        # sort parameters by key
        params.sort(key=itemgetter(0))
        if has_signature:
            params.append(('signature', data['signature']))
        return params

    def get_account_info(self):
        return self._get('account', True, data=params)

    def create_test_order(self, **params):
        return self._post('order/test', True, data=params)

    def get_backtest_data(self, *args):
        return self.get_historical_candles(*args)

    def _convert_interval(self, secs):
        if secs >= 60*60*24:
            return f'{int(secs/(60*60*24))}d'
        elif secs >= 60*60:
            return f'{int(secs/(60*60))}h'
        elif secs >= 60:
            return f'{int(secs/60)}m'
        else:
            return f'{int(secs)}s'

    def get_historical_candles(self, symbol, interval, startTime, endTime):

        endpoint = 'klines'
        _uri = f'{self.base_uri}{endpoint}'
        startTime = str(int(startTime.timestamp() * 1000))
        endTime = str(int(endTime.timestamp() * 1000))
        _interval = self._convert_interval(interval) # Convert from secs
        req_params = {
            'symbol' : symbol.upper(),
            'interval' : _interval,
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

        df.index = [dt.datetime.utcfromtimestamp(x/1000.0) for x in df.datetime]
        df.datetime = df.datetime.apply(lambda r: int(r/1000))
        return df
