import os
import decimal as d
import datetime as dt
import random
import logging
from typing import Dict, List

from influxdb import InfluxDBClient as _InfluxDBClient

def epoch_to_iso(epoch_float):
    dt_obj = dt.datetime.fromtimestamp(epoch_float)
    iso = dt_obj.isoformat()

class InfluxDBClient:
    def __init__(self, db='ta_trader'):
        self.client = _InfluxDBClient(
            os.environ['INFLUXDB_ADDR'],
            8086,
            os.environ['INFLUXDB_USER'],
            os.environ['INFLUXDB_PW'],
            db,
        )
        self.logger = logging.getLogger(__name__)

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
                'measurement': 'trades',
                'time': epoch_to_iso(float(row[0])),
                'tags': {
                    'symbol': row[2],
                    'side': row[3],
                    'position_action': row[4],
                    'order_id': row[10],
                    'status': row[11],
                    'fee_asset': row[13],
                },
                'fields': {
                    'time_trade_flt': row[0],
                    'time_candle': row[1],
                    'size': row[5],
                    'filled': row[6],
                    'executed_price': row[7],
                    'candle_close_price': row[8],
                    'top_of_book_price': row[9],
                    'fee': row[12],
                    'bal_base_before': row[14],
                    'bal_base_after': row[15],
                    'bal_quote_before': row[16],
                    'bal_quote_after': row[17],
                    'netliq_before': row[18],
                    'netliq_after': row[19],
                    'margin_bal_before': row[20],
                    'margin_bal_after': row[21],
                }
            }
        ]
        try:
            self.logger.info(f'Writing influxdb measurement: {json_body}')
            self.client.write_points(json_body)
        except influxdb.exceptions.InfluxDBClientError as e:
            self.logger.critical(e)
