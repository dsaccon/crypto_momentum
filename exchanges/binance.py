import os
import datetime as dt
import time
import json
import logging
import traceback
from functools import wraps
#from operator import itemgetter
#import hashlib
#import hmac
import requests
import pandas as pd
from binance.client import Client as BinanceClient
from . import ExchangeAPI

#class BinanceAPIException(Exception):
#
#    def __init__(self, response):
#        self.code = 0
#        try:
#            json_res = response.json()
#        except ValueError:
#            self.message = 'Invalid JSON error message from Binance: {}'.format(response.text)
#        else:
#            self.code = json_res['code']
#            self.message = json_res['msg']
#        self.status_code = response.status_code
#        self.response = response
#        self.request = getattr(response, 'request', None)
#
#    def __str__(self):  # pragma: no cover
#        return 'APIError(code=%s): %s' % (self.code, self.message)


#class BinanceRequestException(Exception):
#    def __init__(self, message):
#        self.message = message
#
#    def __str__(self):
#        return 'BinanceRequestException: %s' % self.message


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


class BinanceAPI(ExchangeAPI):
    NAME = 'binance'
    API_URL = 'https://api.binance.com/api'
    API_URL_TESTNET = 'https://testnet.binance.vision/api'

    def __init__(self, use_testnet=False):
        self.base_uri = 'https://api.binance.com/api/v3/'
        self.max_candles_fetch = 1000
        #self.trading_fee = 0.00075
        self.trading_fee = 0.001
        if use_testnet:
            if 'BINANCE_TEST_KEY' in os.environ:
                self._API_KEY = os.environ['BINANCE_TEST_KEY']
            else:
                self._API_KEY = None
            if 'BINANCE_TEST_SECRET' in os.environ:
                self._API_SECRET = os.environ['BINANCE_TEST_SECRET']
            else:
                self._API_SECRET = None
        else:
            if 'BINANCE_KEY' in os.environ:
                self._API_KEY = os.environ['BINANCE_KEY']
            else:
                self._API_KEY = None
            if 'BINANCE_SECRET' in os.environ:
                self._API_SECRET = os.environ['BINANCE_SECRET']
            else:
                self._API_SECRET = None
#        self.session = self._init_session()
        self._external_client = BinanceClient(self._API_KEY, self._API_SECRET)

        if use_testnet:
            self._external_client.API_URL = BinanceAPI.API_URL_TESTNET

        self.logger = logging.getLogger(__name__)

        self._symbol_info = self._parse_symbol_info()


#    def _init_session(self):
#        session = requests.session()
#        session.headers.update({'Accept': 'application/json',
#                                'User-Agent': 'binance/python',
#                                'X-MBX-APIKEY': self.API_KEY})
#        return session

#    def _get(self, path, signed=False, version='v1', **kwargs):
#        return self._request_api('get', path, signed, version, **kwargs)
#
#    def _post(self, path, signed=False, version='v1', **kwargs):
#        return self._request_api('post', path, signed, version, **kwargs)
#
#    def _request_api(self, method, path, signed=False, version='v1', **kwargs):
#        uri = self._create_api_uri(path, signed, version)
#        return self._request(method, uri, signed, **kwargs)
#
#    def _create_api_uri(self, path, signed=True, version='v1'):
#        v = 'v3' if signed else version
#        return self.API_URL + '/' + v + '/' + path
#
#    def _generate_signature(self, data):
#        ordered_data = self._order_params(data)
#        query_string = '&'.join(["{}={}".format(d[0], d[1]) for d in ordered_data])
#        m = hmac.new(self.API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
#        return m.hexdigest()
#
#    def _handle_response(self):
#        """Internal helper for handling API responses from the Binance server.
#        Raises the appropriate exceptions when necessary; otherwise, returns the
#        response.
#        """
#        if not str(self.response.status_code).startswith('2'):
#            raise BinanceAPIException(self.response)
#        try:
#            return self.response.json()
#        except ValueError:
#            raise BinanceRequestException('Invalid Response: %s' % self.response.text)
#
#    def _request(self, method, uri, signed, force_params=False, **kwargs):
#
#        # set default requests timeout
#        kwargs['timeout'] = 10
#
#        data = kwargs.get('data', None)
#        if data and isinstance(data, dict):
#            kwargs['data'] = data
#
#            # find any requests params passed and apply them
#            if 'requests_params' in kwargs['data']:
#                # merge requests params into kwargs
#                kwargs.update(kwargs['data']['requests_params'])
#                del(kwargs['data']['requests_params'])
#
#        if signed:
#            # generate signature
#            kwargs['data']['timestamp'] = int(time.time() * 1000)
#            kwargs['data']['signature'] = self._generate_signature(kwargs['data'])
#
#        # sort get and post params to match signature order
#        if data:
#            # sort post params
#            kwargs['data'] = self._order_params(kwargs['data'])
#            # Remove any arguments with values of None.
#            null_args = [i for i, (key, value) in enumerate(kwargs['data']) if value is None]
#            for i in reversed(null_args):
#                del kwargs['data'][i]
#
#        # if get request assign data array to params value for requests lib
#        if data and (method == 'get' or force_params):
#            kwargs['params'] = '&'.join('%s=%s' % (data[0], data[1]) for data in kwargs['data'])
#            del(kwargs['data'])
#
#        self.response = getattr(self.session, method)(uri, **kwargs)
#        return self._handle_response()
#
#    def _order_params(self, data):
#        """Convert params to list with signature as last element
#
#        :param data:
#        :return:
#
#        """
#        has_signature = False
#        params = []
#        for key, value in data.items():
#            if key == 'signature':
#                has_signature = True
#            else:
#                params.append((key, value))
#        # sort parameters by key
#        params.sort(key=itemgetter(0))
#        if has_signature:
#            params.append(('signature', data['signature']))
#        return params
#
#    def get_account_info(self):
#        return self._get('account', True, data=params)
#
#    def create_test_order(self, **params):
#        return self._post('order/test', True, data=params)

    # Internal helper funcs

    def _convert_period(self, secs):
        if secs >= 60*60*24:
            return f'{int(secs/(60*60*24))}d'
        elif secs >= 60*60:
            return f'{int(secs/(60*60))}h'
        elif secs >= 60:
            return f'{int(secs/60)}m'
        else:
            return f'{int(secs)}s'

    def _parse_symbol_info(self):
        symbols = self._get_exchange_info()['symbols']
        info = {}
        for s in symbols:
            key = s['symbol']
            info[key] = {}
            info[key]['lot_min'] = s['filters'][2]['minQty'].rstrip('0')
            info[key]['lot_prec'] = s['filters'][2]['stepSize'].rstrip('0')
            info[key]['price_min'] = s['filters'][0]['minPrice'].rstrip('0')
            info[key]['price_prec'] = s['filters'][0]['tickSize'].rstrip('0')
        return info

    def _get_exchange_info(self):
        endpoint = 'exchangeInfo'
        _uri = f'{self.base_uri}{endpoint}'

        resp = json.loads(requests.get(_uri, params=None).text)
        return resp

    # Generic call to external client
    def external_misc(self, func, *args, **kwargs):
        _func = getattr(self._external_client, func)
        return _func(*args, **kwargs)

    # Custom functions

    def get_backtest_data(self, *args):
        return self.get_historical_candles(*args)

    def get_historical_candles(
            self,
            symbol: str,
            period: int,
            startTime: dt,
            endTime: dt) -> pd:

        """
            Arguments
            ---------
            symbol (str): symbol name of instrument. E.g. 'btcusdt'
            period (int): period length (secs)
            startTime (dt): series start time in datetime format
            endTime (dt): series end time in datetime format

            Returns
            ---------
            df (pd): pandas dataframe with collected data from API
        """

        endpoint = 'klines'
        _uri = f'{self.base_uri}{endpoint}'
        startTime = str(int(startTime.timestamp() * 1000))
        endTime = str(int(endTime.timestamp() * 1000))
        _period = self._convert_period(period) # Convert from secs
        req_params = {
            'symbol' : symbol.upper(),
            'interval' : _period,
            'startTime' : startTime,
            'endTime' : endTime,
            'limit' : self.max_candles_fetch
        }

        df = pd.DataFrame(json.loads(requests.get(_uri, params=req_params).text))

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

    def get_book(self, symbol='BTCUSDT'):
        endpoint = 'depth'
        _uri = f'{self.base_uri}{endpoint}'
        req_params = {
            'symbol' : symbol.upper(),
        }

        resp = json.loads(requests.get(_uri, params=req_params).text)
        return resp

    def get_balances(self, asset='all'):
        bals = self._external_client.get_account()

        if asset == 'all':
            return {
                b['asset']: float(b['free'])
                for b in bals['balances']
            }
        else:
            return [
                float(b['free'])
                for b in bals['balances']
                if b['asset'] == asset
            ][0]

    @meta(wait=1)
    def order_status(self, symbol='BTCUSDT', order_id=None):
        """
        Spot statuses:
            'NEW': The order has been accepted by the engine.
            'PARTIALLY_FILLED': A part of the order has been filled.
            'FILLED': The order has been completed.
            'CANCELED': The order has been canceled by the user.
            'PENDING_CANCEL': Currently unused
            'REJECTED': The order was not accepted by the engine and not proc'd
            'EXPIRED': The order was canceled according to order type's rules
                e.g. LIMIT FOK orders with no fill, LIMIT IOC or MARKET orders that partially fill
                .. or by the exchange
                e.g. orders canceled during liquidation, orders canceled during maintenance
        """
        if order_id is None:
            resp = self._external_client.get_all_orders(symbol=symbol)
        else:
            resp = self._external_client.get_order(symbol=symbol, orderId=order_id)

        self.logger.debug(resp)
        if isinstance(resp, dict):
            return {
                    'order_id': resp['orderId'],
                    'symbol': resp['symbol'],
                    'timestamp': resp['updateTime'],
                    'status': resp['status'],
                    'side': resp['side'],
                    'price': resp['price'],
                    'quantity': resp['executedQty']
            }
        elif isinstance(resp, list):
            # Return as list of (order_id, symbol, status) tuples
            return [
                {
                    'order_id': r['orderId'],
                    'symbol': r['symbol'],
                    'timestamp': r['updateTime'],
                    'status': r['status'],
                    'side': r['side'],
                    'price': r['price'],
                    'quantity': r['executedQty']
                }
                for r in resp
            ]
        else:
            raise ValueError

    def place_order(self, symbol, side, quantity, type_='MARKET', price=None):
        if side == 'BUY':
            if type_ == 'MARKET':
                resp = self._external_client.order_market_buy(
                    symbol=symbol,
                    quantity=quantity)
            elif type_ == 'LIMIT':
                resp = self._external_client.order_limit_buy(
                    symbol=symbol,
                    quantity=quantity,
                    price=price)
            else:
                raise NotImplementedError
        elif side == 'SELL':
            if type_ == 'MARKET':
                resp = self._external_client.order_market_sell(
                    symbol=symbol,
                    quantity=quantity)
            elif type_ == 'LIMIT':
                resp = self._external_client.order_limit_sell(
                    symbol=symbol,
                    quantity=quantity,
                    price=price)
            else:
                raise NotImplementedError
        else:
            raise ValueError

        self.logger.debug(resp)
        return resp['orderId']

    def cancel_order(self, symbol='BTCUSDT', order_id=None):
        if order_id == 'all':
            all_orders = self.order_status()
            for ord_ in all_orders:
                if ord_['status'] == 'NEW':
                    self._external_client.cancel_order(
                        symbol=ord_['symbol'], orderId=ord_['order_id'])
        elif order_id is None:
            raise ValueError
        else:
            return self._external_client.cancel_order(
                symbol=symbol, orderId=order_id)

    def test_order(self, symbol='BTCUSDT', side='BUY', type_='MARKET', quantity=None, price=None):
        if type_ == 'MARKET':
            return self._external_client.create_test_order(
                symbol=symbol, side=side, type=type_, quantity=quantity)
