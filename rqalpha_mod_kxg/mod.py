from typing import Dict

from rqalpha.events import EVENT
from rqalpha.utils import RqAttrDict

from .data_source import DBDataSource
from rqalpha.interface import AbstractMod
from rqalpha.environment import Environment
import datetime
from .trade_recorder import MysqlRecorder
from .price_board import KXGPriceBoard


class KXGMod(AbstractMod):
    def __init__(self):
        pass

    def start_up(self, env, mod_config):
        # type: (Environment,RqAttrDict) -> None
        if not ('db_url' in mod_config.keys()):
            print("-" * 50)
            print(">>> Missing db_url . Please  `config db_url`")
            print("-" * 50)
            raise RuntimeError("need db_url")

        self._env = env
        self.data_source = DBDataSource(env.config.base.data_bundle_path, mod_config.db_url)
        self._inject_api()
        env.set_data_source(self.data_source)
        env.set_price_board(KXGPriceBoard())

        # 如果有strategy_id 保存交易记录到数据库，没有就不保存
        if 'strategy_id' in mod_config.keys():
            env.event_bus.add_listener(EVENT.TRADE, self.on_trade)
            env.event_bus.add_listener(EVENT.POST_SETTLEMENT, self.on_settlement)
            self._recorder = MysqlRecorder(mod_config.strategy_id, mod_config.db_url)
            self._meta = {
                "strategy_id": mod_config.strategy_id,
                "origin_start_date": env.config.base.start_date.strftime("%Y-%m-%d"),
                "start_date": env.config.base.start_date.strftime("%Y-%m-%d"),
                "end_date": env.config.base.end_date.strftime("%Y-%m-%d"),
                "last_run_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "cash": env.config.base.accounts.stock
            }
            persist_meta = self._recorder.load_meta()
            if persist_meta:
                if persist_meta.end_date >= self._meta["start_date"]:
                    raise RuntimeError(
                        (u"current start_date {} is before last end_date {}").format(self._meta["start_date"],persist_meta.end_date))
                else:
                    self._meta["origin_start_date"] = persist_meta.origin_start_date
                    self._meta['cash']= persist_meta.cash


    def on_trade(self, event):
        trade = event.trade
        self._recorder.append_trade(trade)

    def on_settlement(self, event):
        calendar_dt = self._env.calendar_dt.date()
        portfolio = self._env.portfolio
        self._recorder.append_portfolio(calendar_dt, portfolio)
        self._meta['cash'] = portfolio.cash()

    def tear_down(self, success, exception=None):
        if exception is None:
            # 仅当成功运行才写入数据
            if hasattr(self, "_recorder"):
                self._recorder.store_meta(self._meta)
                self._recorder.flush()

    def _inject_api(self):
        from rqalpha import export_as_api
        from rqalpha.execution_context import ExecutionContext
        from rqalpha.const import EXECUTION_PHASE
        import pandas as pd

        @export_as_api
        @ExecutionContext.enforce_phase(EXECUTION_PHASE.ON_INIT,
                                        EXECUTION_PHASE.BEFORE_TRADING,
                                        EXECUTION_PHASE.ON_BAR,
                                        EXECUTION_PHASE.AFTER_TRADING,
                                        EXECUTION_PHASE.SCHEDULED)
        def read_sql_query(sql):
            # type (str)->pandas.DataFrame
            df = pd.read_sql_query(sql=sql, con=self.data_source.db_url)
            return df
