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
    API_URL = 'https://api.binance.com/api/v3'
    API_URL_FUTURES = 'https://fapi.binance.com'
    API_URL_TESTNET = 'https://testnet.binance.vision/api'
    API_URL_FUTURES_TESTNET = 'https://testnet.binancefuture.com'

    def __init__(self, use_testnet=False):
        self.logger = logging.getLogger(__name__)
#        self.base_uri = 'https://api.binance.com/api/v3/'
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
        self._external_client = BinanceClient(self._API_KEY, self._API_SECRET)

        if use_testnet:
            # Works for spot only at the moment
            # Margin: not supported by Binance
            # Futures: supported, but had issues connecting. Maybe to do w URL
            self._external_client.API_URL = BinanceAPI.API_URL_TESTNET
            self._external_client.FUTURES_URL = BinanceAPI.API_URL_FUTURES_TESTNET

        self._symbol_info = self._parse_symbol_info()


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
        endpoint = '/exchangeInfo'
        #_uri = f'{self.base_uri}{endpoint}'
        uri = f'{self.API_URL}{endpoint}'

        resp = json.loads(requests.get(uri, params=None).text)
        return resp

    # Generic call to external client
    def external_misc(self, func, *args, **kwargs):
        _func = getattr(self._external_client, func)
        return _func(*args, **kwargs)

    # Custom functions

    def get_backtest_data(self, *args, **kwargs):
        return self.get_historical_candles(*args, **kwargs)


    # PUBLIC ENDPOINTS (SPOT & FUTURES)
    def get_historical_candles(
            self,
            symbol: str,
            period: int,
            startTime: dt,
            endTime: dt,
            asset_type='spot') -> pd:

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

        if asset_type == 'spot':
            base_uri = self.API_URL
            endpoint = '/klines'
        elif asset_type == 'futures':
            base_uri = self.API_URL_FUTURES
            endpoint = '/fapi/v1/klines'
        else:
            raise ValueError
        uri = f'{base_uri}{endpoint}'
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

        df = pd.DataFrame(json.loads(requests.get(uri, params=req_params).text))

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

    def get_book(self, symbol='BTCUSDT', asset_type='spot', levels=100):
        if asset_type == 'spot':
            base_uri = self.API_URL
            endpoint = '/depth'
        elif asset_type == 'futures':
            base_uri = self.API_URL_FUTURES
            endpoint = '/fapi/v1/depth'
        else:
            raise ValueError
        uri = f'{base_uri}{endpoint}'
        req_params = {
            'symbol' : symbol.upper(),
        }

        resp = json.loads(requests.get(uri, params=req_params).text)
        resp['bids'] = resp['bids'][:levels]
        resp['asks'] = resp['asks'][:levels]
        return resp


    ### SPOT ###

    # ACCOUNT ENDPOINTS
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
            trades = self.get_trades(symbol=symbol, order_id=order_id)
            qty = sum([float(t['quantity']) for t in trades])
            avg_pr = sum([float(t['price'])*float(t['quantity']) for t in trades])/qty
            return {
                    'order_id': resp['orderId'],
                    'symbol': resp['symbol'],
                    'timestamp': resp['updateTime'],
                    'status': resp['status'],
                    'side': resp['side'],
                    'type': resp['type'],
                    'price': avg_pr,
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
                    'type': r['type'],
                    'price': r['price'],
                    'quantity': r['executedQty']
                }
                for r in resp
            ]
        else:
            raise ValueError

    @meta(wait=1)
    def get_trades(self, symbol='BTCUSDT', order_id=None):
        resp = self._external_client.get_my_trades(symbol=symbol)
        parser = lambda x: {
            'order_id': x['orderId'],
            'trade_id': x['id'],
            'symbol': x['symbol'],
            'timestamp': x['time'],
            'side': 'BUY' if x['isBuyer'] == 'true' else 'SELL',
            'type': 'LIMIT' if x['isMaker'] == 'true' else 'MARKET',
            'price': x['price'],
            'quantity': x['qty'],
        }
        if order_id is None:
            trades = [parser(tr) for tr in resp]
        else:
            trades = [parser(tr) for tr in resp if tr['orderId'] == order_id]
        return trades

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

        self.logger.info(resp)
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


    ### FUTURES ###

    # ACCOUNT ENDPOINTS

    def futures_get_balances(self):
        resp = self._external_client.futures_account_balance()
        return {s['asset']:s['balance'] for s in resp}

    @meta(wait=1)
    def futures_order_status(self, symbol='BTCUSDT', order_id=None):
        if order_id is None:
            resp = self._external_client.futures_get_all_orders(symbol=symbol)
        else:
            resp = self._external_client.futures_get_order(symbol=symbol, orderId=order_id)

    def futures_get_positions(self, symbol=None):
        resp = self._external_client.futures_account()
        if symbol:
            return [s for s in resp['positions'] if s['symbol'] == symbol][0]
        else:
            return resp['positions']

    @meta(wait=1)
    def futures_get_trades(self, symbol='BTCUSDT', order_id=None):
        raise NotImplementedError

    def futures_place_order(self, symbol, side, quantity, type_='MARKET', price=None):
        args = {
            'symbol': symbol,
            'quantity': quantity,
            'side': side,
            'type': type_
        }
        resp = self._external_client.futures_create_order(**args)
        self.logger.info(resp)
        return resp['orderId']

        raise NotImplementedError

    def futures_cancel_order(self, symbol='BTCUSDT', order_id=None):
        raise NotImplementedError


    # ...wip
    def futures_account(self):
        bals = self._external_client.futures_account()
        print(bals) ### tmp
