import os
import datetime as dt
import time
import json
import logging
import traceback
from functools import wraps
import requests
import pandas as pd
from binance.client import Client as BinanceClient
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


class BinanceAPI(ExchangeAPI):
    NAME = 'binance'
    API_URL = 'https://api.binance.com/api/v3'
    API_URL_FUTURES = 'https://fapi.binance.com'
    API_URL_TESTNET = 'https://testnet.binance.vision/api'
    API_URL_FUTURES_TESTNET = 'https://testnet.binancefuture.com'

    def __init__(self, use_testnet=False):
        self.logger = logging.getLogger(__name__)
        self.max_candles_fetch = 1000
        self.max_trades_fetch = 1000
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
        if self._API_KEY and self._API_SECRET:
            self._external_client = BinanceClient(self._API_KEY, self._API_SECRET)
        else:
            self._external_client = None

        if use_testnet:
            # Works for spot only at the moment
            # Margin: not supported by Binance
            # Futures: supported, but had issues connecting. Maybe to do w URL
            self._external_client.API_URL = BinanceAPI.API_URL_TESTNET
            self._external_client.FUTURES_URL = BinanceAPI.API_URL_FUTURES_TESTNET

        self.trade_fees = self._get_trade_fees()
        self.trade_fee_spot_asset = {'BUY': 'base', 'SELL': 'quote'}
        self._symbol_info = self._get_symbol_info()

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

    @staticmethod
    def _symbol_parser(s):
        """
        Extract relevant info from object returned from _get_exchange_info()
        """
        info = {}
        i_lot = [
            i for i,f in enumerate(s['filters'])
            if f['filterType'] == 'LOT_SIZE'
        ]
        i_lot = i_lot[0] if not i_lot == [] else None
        i_pr = [
            i for i,f in enumerate(s['filters'])
            if f['filterType'] == 'PRICE_FILTER'
        ]
        i_pr = i_pr[0] if not i_pr == [] else None
        if not i_lot is None:
            info['lot_min'] = s['filters'][i_lot]['minQty'].rstrip('0')
            info['lot_prec'] = s['filters'][i_lot]['stepSize'].rstrip('0')
        if not i_pr is None:
            info['price_min'] = s['filters'][i_pr]['minPrice'].rstrip('0')
            info['price_prec'] = s['filters'][i_pr]['tickSize'].rstrip('0')
        return info

    def _get_symbol_info(self):
        info = {}
        for atype in ('spot', 'futures'):
            info[atype] = {}
            symbols = self._get_exchange_info(asset_type=atype)['symbols']
            for s in symbols:
                info[atype][s['symbol']] = __class__._symbol_parser(s)
            #            key = s['symbol']
            #            _info[key] = {}
            #            i_lot = [
            #                i for i,f in enumerate(s['filters'])
            #                if f['filterType'] == 'LOT_SIZE'
            #            ]
            #            i_lot = i_lot[0] if not i_lot == [] else None
            #            i_pr = [
            #                i for i,f in enumerate(s['filters'])
            #                if f['filterType'] == 'PRICE_FILTER'
            #            ]
            #            i_pr = i_pr[0] if not i_pr == [] else None
            #            if not i_lot is None:
            #                _info[key]['lot_min'] = s['filters'][i_lot]['minQty'].rstrip('0')
            #                _info[key]['lot_prec'] = s['filters'][i_lot]['stepSize'].rstrip('0')
            #            if not i_pr is None:
            #                _info[key]['price_min'] = s['filters'][i_pr]['minPrice'].rstrip('0')
            #                _info[key]['price_prec'] = s['filters'][i_pr]['tickSize'].rstrip('0')
#        info = {'spot': _info}
#        symbols = self._get_exchange_info(asset_type='futures')['symbols']
#        for s in symbols:
#            key = s['symbol']
#            _info[key] = {}
        return info

    def _get_trade_fees(self):
        futures_tiers = {
            0: {'maker': 0.00020, 'taker': 0.00040},
            1: {'maker': 0.00016, 'taker': 0.00040},
            2: {'maker': 0.00014, 'taker': 0.00035},
            3: {'maker': 0.00012, 'taker': 0.00032},
            4: {'maker': 0.00010, 'taker': 0.00030},
            5: {'maker': 0.00008, 'taker': 0.00027},
            6: {'maker': 0.00006, 'taker': 0.00025},
            7: {'maker': 0.00004, 'taker': 0.00022},
            'none': {}
        }
        spot_tiers = {
            0: {'maker': 0.0010, 'taker': 0.0010},
            1: {'maker': 0.0009, 'taker': 0.0010},
            2: {'maker': 0.0008, 'taker': 0.0010},
            3: {'maker': 0.0007, 'taker': 0.0010},
            4: {'maker': 0.0007, 'taker': 0.00090},
            5: {'maker': 0.0006, 'taker': 0.00080},
            6: {'maker': 0.0005, 'taker': 0.00070},
            7: {'maker': 0.0004, 'taker': 0.00060},
            8: {'maker': 0.0003, 'taker': 0.00050},
            9: {'maker': 0.0002, 'taker': 0.00040},
            'none': {'maker': 0.0010, 'taker': 0.0010},
        }
        spot_symbols = (
            s['symbol']
            for s in self._get_exchange_info(asset_type='spot')['symbols']
        )
        futures_symbols = (
            s['symbol']
            for s in self._get_exchange_info(asset_type='futures')['symbols']
        )
        if self._external_client:
            tier = self._external_client.futures_account().get('feeTier')
            fees = {
                'tradeFee': self._external_client.get_trade_fee()
            }
        else:
            tier = 'none'
            fees = {
                'tradeFee': [
                    dict({'symbol': s}.items() | spot_tiers[0].items())
                    for s in spot_symbols
                ]
            }
        fees = {
            'spot': {
                s['symbol']: {
                    'maker': s['makerCommission'],
                    'taker': s['takerCommission']
                }
                for s in fees['tradeFee']
            },
            'futures': {
                future: futures_tiers[tier]
                for future in futures_symbols
            }
        }
        return fees

    def _get_exchange_info(self, asset_type='spot'):
        if asset_type == 'spot':
            endpoint = '/exchangeInfo'
            uri = f'{self.API_URL}{endpoint}'
        elif asset_type == 'futures':
            endpoint = '/fapi/v1/exchangeInfo'
            uri = f'{self.API_URL_FUTURES}{endpoint}'
        else:
            raise ValueError

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
    @meta(wait=1)
    def get_historical_candles(
            self,
            symbol: str,
            period: int,
            startTime: dt,
            endTime: dt = None,
            asset_type: str = 'spot',
            completed_only: bool = True) -> pd:

        """
            Arguments
            ---------
            symbol (str): symbol name of instrument. E.g. 'btcusdt'
            period (int): period length (secs)
            startTime (dt): series start time in datetime format
            endTime (dt or NoneType): series end time in datetime format
            completed_only: do not show candles that have not been completed

            Returns
            ---------
            df (pd): pandas dataframe with collected data from API
        """
        if not period % 60 == 0:
            self.logger.info(f'Incorrect value: {period}, period must be in secs')
            return

        if endTime is None:
            endTime = dt.datetime.utcnow()

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

        df = df.iloc[:, 0:7]
        df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'end_time']

        df.open      = df.open.astype("float")
        df.high      = df.high.astype("float")
        df.low       = df.low.astype("float")
        df.close     = df.close.astype("float")
        df.volume    = df.volume.astype("float")

        df['adj_close'] = df['close']

        df.index = [dt.datetime.utcfromtimestamp(x/1000.0) for x in df.datetime]
        df.datetime = df.datetime.apply(lambda r: int(r/1000))
        df['completed'] = True
        if completed_only:
            df = df.iloc[:-1]
        else:
            df.iloc[-1, df.columns.get_loc('completed')] = False
        return df

    @meta(wait=1)
    def get_historical_trades(
            self,
            symbol: str,
            startTime: dt,
            endTime: dt = None,
            asset_type: str = 'spot',
            completed_only: bool = True) -> pd:

        """
            ###
            ### WIP: /aggTrades part is not working yet ###
            ###

            Arguments
            ---------
            symbol (str): symbol name of instrument. E.g. 'btcusdt'
            period (int): period length (secs)
            startTime (dt): series start time in datetime format
            endTime (dt or NoneType): series end time in datetime format
            completed_only: do not show candles that have not been completed

            Returns
            ---------
            df (pd): pandas dataframe with collected data from API
        """
        if endTime is None:
            endTime = dt.datetime.utcnow()

        base_uri = self.API_URL
        endpoint = '/trades'
        if asset_type == 'spot':
            pass
            #base_uri = self.API_URL
            #endpoint = '/klines'
        elif asset_type == 'futures':
            pass
            #base_uri = self.API_URL_FUTURES
            #endpoint = '/fapi/v1/klines'
        else:
            raise ValueError
        uri = f'{base_uri}{endpoint}'
        #startTime = str(int(startTime.timestamp() * 1000))
        endTime = str(int(endTime.timestamp() * 1000))
        req_params = {
            'symbol' : symbol.upper(),
            'limit' : self.max_trades_fetch
        }

        trades = []
        _trades = json.loads(requests.get(uri, params=req_params).text)
        uri = f'{base_uri}/aggTrades'
        while True:
            trades = _trades + trades
            if _trades[0]['time'] < startTime.timestamp()*1000:
                break
            start_id = _trades[0]['id'] - 1 - self.max_trades_fetch
            req_params['fromId'] = str(start_id)
            _trades = json.loads(requests.get(uri, params=req_params).text)
            quit() ### tmp

        return trades

    @meta(wait=1)
    def get_book(self, symbol='BTCUSDT', asset_type='spot', depth=100):
        if asset_type == 'spot':
            base_uri = self.API_URL
            endpoint = '/depth'
        elif asset_type == 'futures':
            base_uri = self.API_URL_FUTURES
            endpoint = '/fapi/v1/depth'
        else:
            raise ValueError
        uri = f'{base_uri}{endpoint}'

        VALID_LIMITS = [5, 10, 20, 50, 100, 500, 1000, 5000]
        if depth in VALID_LIMITS:
            limit = depth
        else:
            self.logger.info(f'{depth} not a valid OB depth, reverting to 100')
            limit = 100
        req_params = {
            'symbol' : symbol.upper(),
            'limit' : limit,
        }
        resp = json.loads(requests.get(uri, params=req_params).text)
        if not depth in VALID_LIMITS:
            resp['bids'] = resp['bids'][:depth]
            resp['asks'] = resp['asks'][:depth]
        else:
            resp['bids'] = resp['bids']
            resp['asks'] = resp['asks']
        return resp

    def futures_get_mark_price(self, symbol=None):
        ep = '/fapi/v1/premiumIndex'
        uri = f'{self.API_URL_FUTURES}{ep}'
        params = None
        if symbol:
            params = {'symbol': symbol.upper()}
        resp = json.loads(requests.get(uri, params=params).text)
        if symbol:
            return resp['markPrice']
        return {s['symbol']: s['markPrice'] for s in resp}

    def futures_get_index_price(self, symbol=None):
        ep = '/fapi/v1/premiumIndex'
        uri = f'{self.API_URL_FUTURES}{ep}'
        params = None
        if symbol:
            params = {'symbol': symbol.upper()}
        resp = json.loads(requests.get(uri, params=params).text)
        if symbol:
            return resp['indexPrice']
        return {s['symbol']: s['indexPrice'] for s in resp}

    ### SPOT ###

    # ACCOUNT ENDPOINTS
    @meta(wait=1)
    def get_balances(self, asset='all', asset_type='spot', filter_zero=False):
        if asset_type == 'spot':
            bals = self._external_client.get_account()['balances']
            bals = {b['asset']: float(b['free']) for b in bals}
        elif asset_type == 'futures':
            bals = self.futures_get_balances()
            bals = {k:float(v) for k,v in bals.items()}
        else:
            raise ValueError

        if asset == 'all':
            if filter_zero:
                balances = {
                    k:v for k, v in bals.items()
                    if not v == 0
                }
            else:
                balances = bals
        else:
            balances = [
                v for k,v in bals.items() if k == asset
            ]
            if not len(balances) > 0:
                raise ValueError
            balances = balances[0]
        return balances

    @meta(wait=1)
    def order_status(self, symbol='BTCUSDT', order_id=None, asset_type='spot'):
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
        if asset_type == 'futures':
            resp = self.futures_order_status(symbol=symbol, order_id=order_id)
        elif asset_type == 'spot':
            if order_id is None:
                resp = self._external_client.get_all_orders(symbol=symbol)
            else:
                resp = self._external_client.get_order(symbol=symbol, orderId=order_id)
        else:
            raise ValueError

        self.logger.debug(resp)
        if isinstance(resp, dict):
            trades = self.get_trades(symbol=symbol, order_id=order_id, asset_type=asset_type)
            qty = sum([float(t['quantity']) for t in trades])
            avg_pr = sum([float(t['price'])*float(t['quantity']) for t in trades])/qty
            fee = sum([float(t['fee']) for t in trades])
            fee_asset = trades[0]['fee_asset']
            return {
                    'order_id': resp['orderId'],
                    'symbol': resp['symbol'],
                    'timestamp': resp['updateTime'],
                    'status': resp['status'],
                    'side': resp['side'],
                    'type': resp['type'],
                    'price': avg_pr,
                    'fee': fee,
                    'fee_asset': fee_asset,
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
    def get_trades(self, symbol='BTCUSDT', order_id=None, asset_type='spot'):
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
            'fee': x['commission'],
            'fee_asset': x['commissionAsset'],
        }
        if asset_type == 'futures':
            trades = self.futures_get_trades(symbol=symbol, order_id=order_id)
        elif asset_type == 'spot':
            if order_id is None:
                trades = [parser(tr) for tr in resp]
            else:
                trades = [parser(tr) for tr in resp if tr['orderId'] == order_id]
        else:
            return ValueError
        return trades

    def place_order(self, symbol, side, quantity, order_type='MARKET', price=None, asset_type='spot'):
        if asset_type == 'futures':
            args = (symbol, side, quantity)
            kwargs = {'order_type': order_type, 'price': price}
            return self.futures_place_order(*args, **kwargs)
        if not asset_type == 'spot':
            return ValueError

        if side == 'BUY':
            if order_type == 'MARKET':
                resp = self._external_client.order_market_buy(
                    symbol=symbol,
                    quantity=quantity)
            elif order_type == 'LIMIT':
                resp = self._external_client.order_limit_buy(
                    symbol=symbol,
                    quantity=quantity,
                    price=price)
            else:
                raise NotImplementedError
        elif side == 'SELL':
            if order_type == 'MARKET':
                resp = self._external_client.order_market_sell(
                    symbol=symbol,
                    quantity=quantity)
            elif order_type == 'LIMIT':
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

    def test_order(self, symbol='BTCUSDT', side='BUY', order_type='MARKET', quantity=None, price=None):
        if order_type == 'MARKET':
            return self._external_client.create_test_order(
                symbol=symbol, side=side, type=order_type, quantity=quantity)


    ### MARGIN ACCOUNT ###

    def get_margin_account_info(self):
        resp_acct = self._external_client.get_margin_account()
        #resp_asset = self._external_client.get_margin_asset(asset=asset)
        #resp_symbol = self._external_client.get_margin_asset(asset=asset)
        info = {}
        info['margin_level'] = resp_acct['marginLevel']
        return info

    def margin_get_balances(self):
        return self.get_margin_assets()

    def get_margin_assets(self, asset='all'):
        resp = self._external_client.get_margin_account()
        assets = {
            a['asset']: {
                'free': a['free'],
                'locked': a['locked'],
                'borrowed': a['borrowed'],
                'interest': a['interest'],
                'net_asset': a['netAsset'],
            }
            for a in resp['userAssets']
        }
        if asset == 'all':
            return assets
        return assets[asset]

    def get_margin_symbol(self, symbol='BTCUSDT'):
        resp = self._external_client.get_margin_account()
        asset_det = [a for a in resp['userAssets'] if a['asset'] == symbol]
        if not asset_det == []:
            return asset_det[0]
        else:
            return None

    def margin_order_status(self, symbol='BTCUSDT', order_id=None):
        resp = self._external_client.get_margin_order(symbol=symbol, clientOrderId=order_id)
        if isinstance(resp, dict):
            return {
                'order_id': resp['clientOrderId'],
                'symbol': resp['symbol'],
                'timestamp': resp['time'],
                'status': resp['status'],
                'side': resp['side'],
                'type': resp['type'],
                'price': avg_pr,
                'fee': None,
                'fee_asset': None,
                'quantity': resp['executedQty']
            }
        else:
            return None

    def margin_create_loan(self, asset='BTC', quantity=None, cross_margin=True, symbol='ETHBTC'):
        resp = client._external_client.margin_create_loan(
            asset=asset, amount=quantity, isIsolated=not cross_margin, symbol=symbol)
        return resp.get('tranId')

    def margin_repay_loan(self, asset='BTC', quantity=None, cross_margin=True, symbol='ETHBTC'):
        resp = client._external_client.margin_repay_loan(
            asset=asset, amount=quantity, isIsolated=not cross_margin, symbol=symbol)
        return resp.get('tranId')

    def margin_place_order(self, symbol, side, quantity, order_type='MARKET', price=None):
        resp = self._external_client.create_margin_order(
            symbol=symbol,
            side=side,
            type=order_type,
            quantity=quantity,
            price=price)

        self.logger.info(resp)
        return resp['orderId']

    # MARGIN<->SPOT TRANSFERS

    def spot_to_margin_xfer(self, quantity='all', asset='USDT'):
        if quantity == 'all':
            quantity = str(self.get_balances()[asset])
        resp = self._external_client.transfer_spot_to_margin(asset=asset, amount=quantity)
        return resp.get('tranId')

    def margin_to_spot_xfer(self, quantity='all', asset='USDT'):
        if quantity == 'all':
            quantity = str(self.get_balances()[asset])
        resp = self._external_client.transfer_margin_to_spot(asset=asset, amount=quantity)
        return resp.get('tranId')


    ### FUTURES ACCOUNT ###

    # ACCOUNT ENDPOINTS

    @meta(wait=1)
    def futures_get_balances(self):
        resp = self._external_client.futures_account_balance()
        return {s['asset']:s['balance'] for s in resp}

    @meta(wait=1)
    def _futures_get_balances(self):
        """
        Different info than previous
        """
        resp = self._external_client.futures_account()
        return resp

    def futures_get_margin_balance(self, asset='USDT'):
        account = self._external_client.futures_account()
        asset_acct = [a for a in account['assets'] if a['asset'] == asset][0]
        return float(asset_acct['availableBalance'])

    @meta(wait=1)
    def futures_order_status(self, symbol='BTCUSDT', order_id=None):
        if order_id is None:
            resp = self._external_client.futures_get_all_orders(symbol=symbol)
        else:
            resp = self._external_client.futures_get_order(symbol=symbol, orderId=order_id)
        return resp

    def futures_close_positions(self):
        return self.futures_close_position()

    def futures_close_position(self, symbol=None):
        def close_position(sbl):
            position = self.futures_get_positions(symbol=sbl)
            side = 'BUY' if float(position['positionAmt']) < 0 else 'SELL'
            quantity = position['positionAmt']
            quantity = quantity[1:] if quantity[0] == '-' else quantity
            order_id = self.futures_place_order(sbl, side, quantity)
            return order_id
        if symbol is None:
            positions = self.futures_get_positions(filter_zero=True)
            symbols = []
            for position in positions:
                symbols.append(position['symbol'])
            order_ids = []
            for _symbol in symbols:
                order_id = close_position(_symbol)
                order_ids.append(order_id)
            return order_ids
        else:
            order_id = close_position(symbol)
            return order_id

    @meta(wait=1)
    def futures_get_positions(self, symbol=None, filter_zero=False):
        resp = self._external_client.futures_account()
        if symbol:
            return [s for s in resp['positions'] if s['symbol'] == symbol][0]
        else:
            if filter_zero:
                return [
                    r for r in resp['positions']
                    if not float(r['positionAmt']) == 0
                ]
            return resp['positions']

    @meta(wait=1)
    def _futures_get_positions(self, symbol=None, filter_zero=False):
        """
        WIP. Gives slightly different info than from futures_get_positions()
        """
        resp = self._external_client.futures_position_information()
        if symbol:
            return [s for s in resp if s['symbol'] == symbol]
        else:
            if filter_zero:
                return [
                    r for r in resp
                    if not float(r['positionAmt']) == 0
                ]
            return resp

    @meta(wait=1)
    def futures_get_trades(self, symbol='BTCUSDT', order_id=None):
        kwargs = {
            'symbol': symbol,
        }
        resp = self._external_client.futures_account_trades(**kwargs)
        parser = lambda x: {
            'order_id': x['orderId'],
            'trade_id': x['id'],
            'symbol': x['symbol'],
            'timestamp': x['time'],
            'side': x['side'],
            'type': 'LIMIT' if x['maker'] == 'true' else 'MARKET',
            'price': x['price'],
            'quantity': x['qty'],
            'realized_pnl': x['realizedPnl'],
            'fee': x['commission'],
            'fee_asset': x['commissionAsset'],
        }
        if order_id is None:
            trades = [parser(tr) for tr in resp]
        else:
            trades = [parser(tr) for tr in resp if tr['orderId'] == order_id]
        trades.sort(key=lambda x: x['timestamp'])
        return trades

    def futures_get_position_pnl(self, symbol='BTCUSDT', order_id=None):
        trades = self.futures_get_trades(symbol=symbol, order_id=order_id)
        if order_id is None:
            trades = [tr for tr in trades if tr['order_id'] == trades[-1]['order_id']]
        r_pnl = sum([float(tr['realized_pnl']) for tr in trades])
        u_pnl = float(self.futures_get_positions(symbol=symbol)['unrealizedProfit'])
        return {'realized': r_pnl, 'unrealized': u_pnl}

    def futures_place_order(self, symbol, side, quantity, order_type='MARKET', price=None):
        kwargs = {
            'symbol': symbol,
            'quantity': quantity,
            'side': side,
            'type': order_type
        }
        resp = self._external_client.futures_create_order(**kwargs)
        self.logger.info(resp)
        return resp['orderId']

    def futures_change_initial_leverage(self, symbol, leverage):
        kwargs = {
            'symbol': symbol,
            'leverage': leverage,
        }
        resp = self._external_client.futures_change_leverage(**kwargs)
        self.logger.info(resp)
        return resp

    def futures_cancel_order(self, symbol='BTCUSDT', order_id=None):
        raise NotImplementedError

    # FUTURES<->SPOT TRANSFERS

    def spot_to_futures_xfer(self, quantity='all', asset='USDT', futures_type='usdt'):
        self._futures_spot_xfer(quantity, asset, futures_type, True)

    def futures_to_spot_xfer(self, quantity='all', asset='USDT', futures_type='usdt'):
        self._futures_spot_xfer(quantity, asset, futures_type, False)

    def _futures_spot_xfer(self, quantity, asset, futures_type, from_spot):
        if from_spot:
            if futures_type == 'usdt':
                _type = 1
            elif futures_type == 'coin':
                _type = 3
            else:
                raise ValueError
        else:
            if futures_type == 'usdt':
                _type = 2
            elif futures_type == 'coin':
                _type = 4
            else:
                raise ValueError
        if quantity == 'all' and from_spot:
            quantity = str(self.get_balances()[asset])
        elif quantity == 'all' and not from_spot:
            quantity = str(self.futures_get_balances()[asset])
        kwargs = {
            'asset': asset,
            'type': _type,
            'amount': quantity
        }
        self._external_client.futures_account_transfer(**kwargs)

    # ...wip
    def futures_account(self):
        bals = self._external_client.futures_account()
        return bals
