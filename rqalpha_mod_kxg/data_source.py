from datetime import date, datetime, timedelta
from typing import Optional, Union

from rqalpha.data.base_data_source import BaseDataSource
from rqalpha.model.instrument import Instrument

from functools import lru_cache
import numpy as np
from rqalpha.utils.datetime_func import convert_date_to_int
from rqalpha.utils.exception import RQInvalidArgument
import pandas as pd
from sqlalchemy import create_engine

PRICE_FIELDS = {
    'open', 'close', 'high', 'low', 'limit_up', 'limit_down', 'acc_net_value', 'unit_net_value'
}

FIELDS_REQUIRE_ADJUSTMENT = set(list(PRICE_FIELDS) + ['volume'])


def adjust_bars(bars, fields, adjust_type, adjust_orig):
    if len(bars) == 0:
        return bars if fields is None else bars[fields]
    # 复权
    if isinstance(fields, str):
        if fields in PRICE_FIELDS:
            if adjust_type == 'post':
                bars[fields] = bars[fields] * bars['adj_factor']
            elif adjust_type == 'pre':
                bars[fields] = bars[fields] * bars['adj_factor'] / float(bars['adj_factor'][0])

        return bars[fields]
    result = np.copy(bars if fields is None else bars[fields])
    for f in result.dtype.names:
        if f in PRICE_FIELDS:
            if adjust_type == 'post':
                result[f] = result[f] * result['adj_factor']
            elif adjust_type == 'pre':
                result[f] = result[f] * result['adj_factor'] / float(result['adj_factor'][0])
    return result


class DBDataSource(BaseDataSource):

    def __init__(self, path, db_url):
        self.db_url = db_url
        self.engine = create_engine(db_url)
        super(DBDataSource, self).__init__(path, {})

    def is_index(self, order_book_id):
        if (order_book_id.startswith("0") and order_book_id.endswith(".XSHG")) or (
                order_book_id.startswith("3") and order_book_id.endswith(".XSHE")):
            return True
        else:
            return False

    @staticmethod
    def tushare_data_convert(df):
        if 'adj_factor' in df.columns:
            return  df.assign(
                limit_up=df['pre_close'] * 1.1,
                limit_down=df['pre_close'] * 0.9,
                datetime=df['trade_date'].astype('int64') * 1000000) \
                .rename(columns={"vol": "volume", 'amount': "total_turnover"}) \
                .drop(axis=1, columns=['trade_date', 'ts_code', 'pre_close', 'change', 'pct_chg','is_st','is_suspend']) \
                .dropna() \
                .sort_values(by="datetime")\
                .reset_index(drop=True)\
                .to_records()
        else:
            return df.assign(
                limit_up=df['pre_close'] * 1.1,
                limit_down=df['pre_close'] * 0.9,
                datetime=df['trade_date'].astype('int64') * 1000000) \
                .rename(columns={"vol": "volume", 'amount': "total_turnover"}) \
                .drop(axis=1, columns=['trade_date', 'ts_code', 'pre_close', 'change', 'pct_chg']) \
                .dropna() \
                .sort_values(by="datetime") \
                .reset_index(drop=True) \
                .to_records()

    @lru_cache(None)
    def _all_day_bars_of(self, instrument):
        table = "index_daily" if self.is_index(instrument.order_book_id) else "daily"
        df = pd.read_sql(sql="select * from {table} where ts_code='{ts_code}'".format(table=table, ts_code=instrument.order_book_id),con=self.engine)
        return self.tushare_data_convert(df)

    @lru_cache(None)
    def _filtered_day_bars(self, instrument):
        bars = self._all_day_bars_of(instrument)
        return bars[bars['volume'] > 0]

    def get_bar(self, instrument, dt, frequency):
        # type: (Instrument, Union[datetime, date], str) -> Optional[np.ndarray]
        if frequency != '1d':
            raise NotImplementedError

        bars = self._all_day_bars_of(instrument)
        if len(bars) <= 0:
            return
        dt = np.uint64(convert_date_to_int(dt))
        pos = bars['datetime'].searchsorted(dt)
        if pos >= len(bars) or bars['datetime'][pos] != dt:
            return None

        return bars[pos]

    def history_bars(self, instrument, bar_count, frequency, fields, dt,
                     skip_suspended=True, include_now=False,
                     adjust_type='pre', adjust_orig=None):
        if frequency != '1d':
            raise NotImplementedError

        if skip_suspended and instrument.type == 'CS':
            bars = self._filtered_day_bars(instrument)
        else:
            bars = self._all_day_bars_of(instrument)

        if not self._are_fields_valid(fields, bars.dtype.names):
            raise RQInvalidArgument("invalid fileds: {}".format(fields))

        if len(bars) <= 0:
            return bars

        dt = np.uint64(convert_date_to_int(dt))
        i = bars['datetime'].searchsorted(dt, side='right')
        left = i - bar_count if i >= bar_count else 0
        bars = bars[left:i]
        if adjust_type == 'none' or instrument.type in {'Future', 'INDX'}:
            # 期货及指数无需复权
            return bars if fields is None else bars[fields]

        if isinstance(fields, str) and fields not in FIELDS_REQUIRE_ADJUSTMENT:
            return bars if fields is None else bars[fields]

        return adjust_bars(bars,
                           fields, adjust_type, adjust_orig)

    def available_data_range(self, frequency):
        return date(2005, 1, 1), date.today()

    def get_trading_minutes_for(self, order_book_id, trading_dt):
        pass

    def current_snapshot(self, instrument, frequency, dt):
        pass

    def get_ticks(self, order_book_id, date):
        pass

    def get_merge_ticks(self, order_book_id_list, trading_date, last_dt=None):
        pass

    def history_ticks(self, instrument, count, dt):
        pass









