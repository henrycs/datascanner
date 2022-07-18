import datetime
import logging

import cfg4py
from coretypes import FrameType, bars_dtype
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer
from omicron.models.security import Security
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame
from omicron.models.timeframe import TimeFrame as tf

from influx_data.security_bars_1d import (
    get_security_day_bars,
    get_security_price_limits,
)
from jq_fetcher import get_sec_bars_1d, get_sec_bars_1d_pricelimits

logger = logging.getLogger(__name__)


async def fullscan_1d_for_close_price(
    all_secs_today, target_date: datetime.date, all_secs_data
):
    start = datetime.datetime.combine(target_date, datetime.time(0, 0, 0))
    end = datetime.datetime.combine(target_date, datetime.time(23, 59, 59))
    all_secs_in_bars = await get_security_day_bars(start, end)
    secs_in_bars = set()
    for sec in all_secs_in_bars:
        code = sec[1]
        secs_in_bars.add(code)

    # 不在当日证券列表中的，需要从数据库日线中删除
    print("security in db, not in security list:")
    secs_to_be_removed = []
    for sec in secs_in_bars:
        if sec not in all_secs_today:
            secs_to_be_removed.append(sec)
            print("----- remove from bars:1d, ", sec)
    print("total secs to be removed from bars:1d, ", len(secs_to_be_removed))

    secs_in_jq = list(all_secs_data.keys())

    # 当日证券列表有，日线数据没有，说明缺失，要补录，也可能是停牌，需要判断返回的数据日期
    print("security in sec list, not in bars:1d@db")
    secs_to_be_added = set()
    secs_to_be_added_2 = set()  # 和聚宽下载的对比
    for sec in all_secs_today:
        if sec not in secs_in_bars:
            secs_to_be_added.add(sec)
            print("--------------- get and save into bars:1d, ", sec)
        if sec not in secs_in_jq:
            secs_to_be_added_2.add(sec)  # 可能是停牌
            print("--------------- not in jq, ", sec)
    print(
        "total secs to be added into bars:1d, ",
        len(secs_to_be_added),
        len(secs_to_be_added_2),
    )

    print("-------------------------------------------------")
    for sec in secs_in_bars:
        if sec not in secs_in_jq:
            print("----- remove from bars:1d, ", sec)
    for sec in secs_in_jq:
        if sec not in secs_in_bars:
            print("----- update from bars:1d, ", sec)

    # download data from jq
    if len(secs_to_be_added) > 0:
        pass
        # await get_sec_bars_1d(secs_to_be_added, target_date)

    print("finished checking bars:1d:open, ", target_date)


async def fullscan_1d_for_pricelimits(all_secs, target_date: datetime.date):
    secs_target_date = []
    for sec in all_secs:
        code = sec[0]
        secs_target_date.append(code)

    start = datetime.datetime.combine(target_date, datetime.time(0, 0, 0))
    end = datetime.datetime.combine(target_date, datetime.time(23, 59, 59))
    all_secs_in_bars = await get_security_price_limits(start, end)
    secs_in_bars = []
    for sec in all_secs_in_bars:
        code = sec[1]
        secs_in_bars.append(code)

    print("security in limits bars, not in security list: ")
    secs_to_be_removed = []
    for sec in secs_in_bars:
        if sec not in secs_target_date:
            secs_to_be_removed.append(sec)
            print("----- remove from bars:1d:limit, ", sec)
    print("total secs to be removed from bars:1d:limit, ", len(secs_to_be_removed))

    # 当日证券列表有，日线数据没有，说明缺失，要补录，也可能是停牌，需要判断返回的数据日期
    print("security in sec list, not in bars:1d:limits: ")
    secs_to_be_added = []
    for sec in secs_target_date:
        if sec not in secs_in_bars:
            secs_to_be_added.append(sec)
            print("--------------- get and save into bars:1d:limit, ", sec)
    print("total secs to be added into bars:1d:limit, ", len(secs_to_be_added))

    # download data from jq
    if len(secs_to_be_added) > 0:
        await get_sec_bars_1d_pricelimits(secs_to_be_added, target_date)

    print("finished checking bars:1d:high_limit, ", target_date)
