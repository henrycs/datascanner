import datetime
import logging
import pickle
from coretypes import FrameType, bars_dtype
import cfg4py
from omicron.models.timeframe import TimeFrame
from omicron.models.security import Security
from omicron.models.stock import Stock
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer
from omicron.models.timeframe import TimeFrame as tf
from dfs_tools import get_trade_limit_filename
from influx_data.security_bars_1d import get_security_day_bars, get_security_price_limits
from influx_tools import remove_sec_in_bars1d
from dfs import Storage


from jq_fetcher import get_sec_bars_1d, get_sec_bars_pricelimits


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


async def remove_secs_from_bars_1d(all_secs_today, target_date: datetime.date, secs_closed, secs_limits):
    # 不在当日证券列表中的，需要从数据库日线中删除
    print("security in db, not in security list:")
    secs_to_be_removed1 = set()
    for sec in secs_closed:
        if sec not in all_secs_today:
            secs_to_be_removed1.add(sec)
            #logger.info("remove from bars:1d@db: %s", sec)
    logger.info("secs to be removed from bars:1d@db, %d", len(secs_to_be_removed1))

    # 涨跌停股票不在证券列表中的，也需要一起删除
    secs_to_be_removed2 = set()
    for sec in secs_limits:
        if sec not in all_secs_today:
            secs_to_be_removed2.add(sec)
            #logger.info("remove from bars:1d:limit@db, %s", sec)
    logger.info("total secs to be removed from bars:1d:limit@db, %d", len(secs_to_be_removed2))

    # perform remove action
    secs_to_be_removed = set.union(secs_to_be_removed1, secs_to_be_removed2)
    for sec in secs_to_be_removed:
        #await remove_sec_in_bars1d(sec, target_date)
        #logger.info("from bars:1d@db, removed: %s, %s", sec, target_date)
        pass
    
    return secs_to_be_removed


async def scan_1d_for_close_price(all_secs_today, target_date: datetime.date, secs_in_bars: set):
    start = datetime.datetime.combine(target_date, datetime.time(0, 0, 0))
    end = datetime.datetime.combine(target_date, datetime.time(23, 59, 59))

    # 当日证券列表有，日线数据没有，可能的情况：
    # 缺失（要补录），可能是停牌（需要判断返回的数据日期），可能是停牌前的整理时间已到（无数据）
    print("security in sec list, not in bars:1d@db")
    secs_to_be_added = []
    for sec in all_secs_today:
        if sec not in secs_in_bars:
            secs_to_be_added.append(sec)
            #logger.info("to be retrieved from jq (bars:1d), %s", sec)
    logger.info("total secs to be retrieved from jq (bars:1d), %d", len(secs_to_be_added))

    # download data from jq
    if len(secs_to_be_added) > 0:
        #all_secs_data = []
        all_secs_data = await get_sec_bars_1d(secs_to_be_added, target_date)
        logger.info("total secs downloaded from bars:1d@jq, %d", len(all_secs_data))
        if len(all_secs_data) > 0:
            #await Stock.persist_bars(FrameType.DAY, all_secs_data)
            logger.info("secs downloaded from bars:1d@jq and saved into db, %s, %d", FrameType.DAY, len(all_secs_data))

    logger.info("finished checking bars:1d:open, %s", target_date)


async def scan_1d_for_pricelimits(all_secs_today, target_date: datetime.date, secs_in_bars: set):
    start = datetime.datetime.combine(target_date, datetime.time(0, 0, 0))
    end = datetime.datetime.combine(target_date, datetime.time(23, 59, 59))

    # 当日证券列表有，日线数据没有，可能的情况：
    # 缺失（要补录），可能是停牌（需要判断返回的数据日期），可能是停牌前的整理时间已到（无数据）
    secs_to_be_added = []
    for sec in all_secs_today:
        if sec not in secs_in_bars:
            secs_to_be_added.append(sec)
            #logger.info("to be retrieved from jq (bars:1d:limit), %s", sec)
    logger.info("total secs to be retrieved from jq (bars:1d:limit), %d", len(secs_to_be_added))

    # download data from jq
    if len(secs_to_be_added) > 0:
        #all_secs_data = []
        all_secs_data = await get_sec_bars_pricelimits(secs_to_be_added, target_date)
        logger.info("total secs downloaded from bars:1d:limits@jq, %d", len(all_secs_data))
        if len(all_secs_data) > 0:
            #await Stock.save_trade_price_limits(all_secs_data, to_cache=False)
            logger.info("secs downloaded from bars:1d:limits@jq and saved into db, %s, %d", FrameType.DAY, len(all_secs_data))

    logger.info("finished checking bars:1d:high_limit, %s", target_date)


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

