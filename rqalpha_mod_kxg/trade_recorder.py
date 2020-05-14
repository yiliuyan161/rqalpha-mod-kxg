from rqalpha.const import SIDE
from sqlalchemy import *
from sqlalchemy import event
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
from collections import defaultdict
import numpy as np

TRADE_CSV_HEADER = [
    "exec_id",
    "order_id",
    "order_book_id",
    "datetime",
    "last_price",
    "last_quantity",
    "transaction_cost",
    "position_effect",
]

PORTFOLIO_CSV_HEADER = [
    "portfolio_value",
    "market_value",
    "cash",
    # "daily_pnl",
    # "daily_returns",
    # "total_returns",
]

ModelBase = declarative_base()


class Trade(ModelBase):
    __tablename__ = "strategy_trade"
    strategy_id = Column('strategy_id', String(50), primary_key=True)
    order_id = Column('order_id', String(20), primary_key=True)
    exec_id = Column('exec_id', String(20))
    order_book_id = Column('order_book_id', String(20))
    datetime = Column('datetime', DateTime())
    last_price = Column('last_price', Float())
    last_quantity = Column('last_quantity', Float())
    transaction_cost = Column('transaction_cost', Float())
    side = Column('side', INT())
    position_effect = Column('position_effect', String(30))

    def __str__(self):
        return "<order_book_id:{order_book_id},last_price:{last_price},datetime:{datetime}>".format(order_book_id=self.order_book_id,last_price=self.last_price,datetime=self.datetime)


class Portfolio(ModelBase):
    __tablename__ = "strategy_portfolio"
    strategy_id = Column('strategy_id', String(50), primary_key=True)
    datetime = Column('datetime', DateTime, primary_key=True)
    portfolio_value = Column('portfolio_value', Float())
    market_value = Column('market_value', Float())
    cash = Column('cash', Float())
    daily_pnl = Column('daily_pnl', Float())
    daily_returns = Column('daily_returns', Float())
    total_returns = Column('total_returns', Float())


class Portfolio_Benchmark(ModelBase):
    __tablename__ = "strategy_portfolio_bm"
    strategy_id = Column('strategy_id', String(50), primary_key=True)
    datetime = Column('datetime', DateTime, primary_key=True)
    portfolio_value = Column('portfolio_value', Float())
    market_value = Column('market_value', Float())
    cash = Column('cash', Float())
    daily_pnl = Column('daily_pnl', Float())
    daily_returns = Column('daily_returns', Float())
    total_returns = Column('total_returns', Float())


class Meta(ModelBase):
    __tablename__ = "strategy_meta"
    strategy_id = Column('strategy_id', String(50), primary_key=True)
    origin_start_date = Column('origin_start_date', String(20))
    start_date = Column('start_date', String(20))
    end_date = Column('end_date', String(20))
    last_run_time = Column('last_run_time', String(20))
    cash = Column('cash',Float(20))


def add_float_encoders(conn, cursor, query, *args):
    cursor.connection.encoders[np.float64] = lambda value, encoders: float(value)


class MysqlRecorder:

    def __init__(self, strategy_id, db_url):
        # type: (str,str) -> None
        self.engine = create_engine(db_url)
        self.metadata = MetaData(self.engine)
        self._strategy_id = strategy_id
        self.trade_list = []
        self._portfolios_dict = defaultdict(list)
        self.session = sessionmaker(self.engine)()
        event.listen(self.engine, "before_cursor_execute", add_float_encoders)

    def load_meta(self):
        # type:()->Meta
        return self.session.query(Meta).filter(Meta.strategy_id == self._strategy_id).first()

    def store_meta(self, meta):
        # type (dict)->None
        if self.session.query(Meta.strategy_id).filter(Meta.strategy_id == self._strategy_id).count() > 0:
            self.session.query(Meta).filter(Meta.strategy_id == self._strategy_id).update(
                {
                    'origin_start_date': meta['origin_start_date'],
                    'start_date': meta['start_date'],
                    'end_date': meta['end_date'],
                    'last_run_time': meta['last_run_time'],
                    'cash': meta['cash']
                })
        else:
            mt = Meta(strategy_id=meta['strategy_id'], origin_start_date=meta['origin_start_date'],
                      start_date=meta['start_date'], end_date=meta['end_date'], last_run_time=meta['last_run_time'],cash=meta['cash'])
            self.session.add(mt)

    def _portfolio2obj(self, dt, portfolio):
        pf = Portfolio()
        for key in PORTFOLIO_CSV_HEADER:
            setattr(pf, key, getattr(portfolio, key))
        pf.datetime = dt
        pf.strategy_id = self._strategy_id
        return pf

    def append_trade(self, trade):
        td = Trade()
        for key in TRADE_CSV_HEADER:
            setattr(td, key, getattr(trade, key))
        td.position_effect = str(td.position_effect)
        if trade.side == SIDE.BUY:
            td.side = 1
        else:
            td.side = -1
        td.strategy_id = self._strategy_id
        self.trade_list.append(td)

    def append_portfolio(self, dt, portfolio):
        self._portfolios_dict["portfolio"].append(self._portfolio2obj(dt, portfolio))

    def flush(self):
        if self.trade_list:
            self.session.add_all(self.trade_list)
        for name, p_list in self._portfolios_dict.items():
            for portfolio_dict in p_list:
                self.session.add(portfolio_dict)
        self.session.commit()
        self.session.close()

class MemoryRecorder:
    def __init__(self):
        self.trade_list=[]
        self.portfilio_list=[]

    def append_trade(self, trade):
        td = Trade()
        for key in TRADE_CSV_HEADER:
            setattr(td, key, getattr(trade, key))
        td.position_effect = str(td.position_effect)
        if trade.side == SIDE.BUY:
            td.side = 1
        else:
            td.side = -1
        self.trade_list.append(td)

    def append_portfolio(self, dt, portfolio):
        self.portfilio_list.append(portfolio)



