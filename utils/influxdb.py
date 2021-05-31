import os
import decimal as d
import datetime as dt
import random
from typing import Dict, List

from influxdb import InfluxDBClient as _InfluxDBClient

def epoch_to_iso(epoch_float):
    dt_obj = dt.datetime.fromtimestamp(epoch_float)
    iso = dt_obj.isoformat()

class InfluxDBClient:
    def __init__(self, db='trading'):
        self.client = _InfluxDBClient(
            os.environ['INFLUXDB_ADDR'],
            8086,
            os.environ['INFLUXDB_USER'],
            os.environ['INFLUXDB_PW'],
            db,
        )
        # Below just an example
        self.SCHEMA = {
            'measurement': 'accounts',
            'tags': {
                'account_id': 'novopg@gmail.com',
                'account_code': 'EOS',
                'exchange': 'OKEX',
                'base': 'EOS',
                'quote': 'USDT',
                'exp_date': '07-02-2020',
            },
            'fields': {
                'base_position': 1144.11,
                'quote_position': 2910.2,
                'equity': 308.33,
            },
            'time': 1581027577.1234567
        }

    def create_db(self, db_name):
        self.client.create_database(db_name)

    def insert(self, body):
        for element in body:
            element['time'] = epoch_to_iso(float(element['time']))
        self.client.write_points(body)
        return True

    def read(self, measurement):
        result = self.client.query(f'select value from {measurement};')
        return result

    def write_trade(self, row):
    	json_body = [
			{
				"measurement": "ta_tradelog",
				"time": dt.datetime.fromtimestamp(float(row[0])),,
				"fields": {
					"...": row[1], 
					"symbol": row[2], 
					"side": row[3], 
					"side": row[3], 
				}
			}
		]
			
		} 
        row = (
            ts_trade,
            self.data[0].index[-1],
            trade_status['symbol'],
            trade_status['side'],
            position_action,
            size,
            trade_status['quantity'],
            trade_status['price'],
            close_price,
            ob_snapshot[book_side][0][0],
            trade_status['order_id'],
            trade_status['status'],
            trade_status['fee'],
            trade_status['fee_asset'],
            bals_before.get(self.cfg['symbol'][0]),
            bals_after.get(self.cfg['symbol'][0]),
            bals_before.get(self.cfg['symbol'][1]),
            bals_after.get(self.cfg['symbol'][1]),
            netliq_before,
            netliq_after,
            '',
            '')
