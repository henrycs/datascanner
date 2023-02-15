import datetime
import logging
import pickle

import cfg4py
from coretypes import FrameType, bars_dtype
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer
from omicron.models.security import Security
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame
from omicron.models.timeframe import TimeFrame as tf

from dfs import Storage
from dfs_tools import get_trade_limit_filename
from influx_data.security_bars_1d import (
    get_security_day_bars,
    get_security_price_limits,
)

logger = logging.getLogger(__name__)


async def scan_bars_1d_for_seclist(target_date: datetime.date):
    start = datetime.datetime.combine(target_date, datetime.time(0, 0, 0))
    end = datetime.datetime.combine(target_date, datetime.time(23, 59, 59))

    all_secs_in_bars = await get_security_day_bars(start, end)
    if all_secs_in_bars is None or len(all_secs_in_bars) == 0:
        logger.error("no secs found in bars:1d, %s", target_date)
        return None

    secs_in_bars = set()
    for sec in all_secs_in_bars:
        code = sec[1]
        secs_in_bars.add(code)

    return secs_in_bars


async def scan_bars_1d_pricelimits_for_seclist(target_date: datetime.date):
    start = datetime.datetime.combine(target_date, datetime.time(0, 0, 0))
    end = datetime.datetime.combine(target_date, datetime.time(23, 59, 59))

    all_secs_in_bars = await get_security_price_limits(start, end)
    secs_in_bars = []
    for sec in all_secs_in_bars:
        code = sec[1]
        secs_in_bars.append(code)

    return secs_in_bars


def get_security_difference(secs_in_bars, all_secs, all_indexes):
    all_stock_secs = set(all_secs)
    all_index_secs = set(all_indexes)

    # 检查是否有多余的股票
    x1 = secs_in_bars.difference(all_stock_secs)
    y1 = x1.difference(all_index_secs)
    if len(y1) > 0:  # 需要删除
        for sec in y1:
            print("to be removed: ", sec)
    print("secs to be removed: ", len(y1))

    _all_secs_set = all_stock_secs.union(all_index_secs)
    to_be_added = _all_secs_set.difference(secs_in_bars)
    if len(to_be_added) > 0:
        for sec in to_be_added:
            print("to be added: ", sec)
    print("secs to be added: ", len(to_be_added))


async def validate_bars_1d(target_date: datetime.date, all_secs, all_indexes):
    logger.info("check bars:1d for open/close: %s", target_date)
    secs_list_1 = await scan_bars_1d_for_seclist(target_date)
    get_security_difference(secs_list_1, all_secs, all_indexes)

    logger.info("check bars:1d for price limits: %s", target_date)
    secs_list_2 = await scan_bars_1d_pricelimits_for_seclist(target_date)
    get_security_difference(secs_list_2, all_secs, all_indexes)
