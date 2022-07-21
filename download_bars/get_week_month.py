import datetime
import logging
import pickle

import cfg4py
from coretypes import FrameType, SecurityType
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer
from omicron.models.security import Security
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame
from omicron.models.timeframe import TimeFrame as tf

from datascan.jq_fetcher import (
    get_sec_bars_1d,
    get_sec_bars_1M,
    get_sec_bars_1w,
    get_sec_bars_pricelimits,
)
from dfs import Storage
from dfs_tools import get_trade_limit_filename, write_bars_dfs, write_price_limits_dfs
from influx_data.security_bars_1d import (
    get_security_day_bars,
    get_security_price_limits,
)
from influx_data.security_list import get_security_list
from time_utils import split_securities

logger = logging.getLogger(__name__)


async def get_1w_for_price(
    all_secs_today, target_date: datetime.date, d0: datetime.date, prefix: SecurityType
):
    # d0: 本周第一天, target_date：本周最后一天

    # download all data from jq
    all_secs_data = await get_sec_bars_1w(all_secs_today, target_date, d0)
    logger.info(
        "total secs downloaded from bars:1w@jq, %d, %s", len(all_secs_data), prefix
    )

    if len(all_secs_data) == 0:
        logger.error(
            "failed to get price data from bars:1w@jq, %s (%s)", target_date, prefix
        )
        return False

    await Stock.persist_bars(FrameType.WEEK, all_secs_data)
    logger.info(
        "get from bars:1w@jq and saved into db, %s, %s, %d",
        prefix,
        FrameType.WEEK,
        len(all_secs_data),
    )

    await write_bars_dfs(target_date, FrameType.WEEK, all_secs_data, prefix)

    logger.info("finished processing bars:1w for %s (%s)", target_date, prefix)
    return True


async def retrieve_bars_1w(target_day, all_secs, all_indexes):
    all_stock_secs = all_secs
    all_index_secs = all_indexes

    # 读取本周第一天的证券列表
    if target_day == datetime.date(2005, 1, 7):
        w1d0 = datetime.date(2005, 1, 4)
    else:
        w0 = TimeFrame.week_shift(target_day, -1)
        w1d0 = TimeFrame.day_shift(w0, 1)
    logger.info("first and last day of week: %s, %s", w1d0, target_day)

    if w1d0 != target_day:
        all_secs_in_w1d = await get_security_list(w1d0)
        if all_secs_in_w1d is None:
            logger.error("no security list in date %s", w1d0)
            return False

        all_secs_0, all_indexes_0 = split_securities(all_secs_in_w1d)
        if len(all_secs_0) == 0 or len(all_indexes_0) == 0:
            logger.error("no stock or index list in date %s", w1d0)
            return False

        # 合并数据，补上可能期间退市的股票
        all_stock_secs = all_secs.union(all_secs_0)
        all_index_secs = all_indexes.union(all_indexes_0)

    print(len(all_stock_secs), len(all_index_secs))

    rc = await get_1w_for_price(all_secs, target_day, w1d0, SecurityType.STOCK)
    if not rc:
        logger.error("failed to process stock price data (week): %s", target_day)
        return False

    rc = await get_1w_for_price(all_indexes, target_day, w1d0, SecurityType.INDEX)
    if not rc:
        logger.error("failed to process index price data (week): %s", target_day)
        return False

    return True


async def get_1M_for_price(
    all_secs_today, target_date: datetime.date, d0: datetime.date, prefix: SecurityType
):
    # d0: 本月第一天, target_date：本月最后一天

    # download all data from jq
    all_secs_data = await get_sec_bars_1M(all_secs_today, target_date, d0)
    logger.info(
        "total secs downloaded from bars:1M@jq, %d, %s", len(all_secs_data), prefix
    )

    if len(all_secs_data) == 0:
        logger.error(
            "failed to get price data from bars:1M@jq, %s (%s)", target_date, prefix
        )
        return False

    await Stock.persist_bars(FrameType.MONTH, all_secs_data)
    logger.info(
        "get from bars:1M@jq and saved into db, %s, %s, %d",
        prefix,
        FrameType.MONTH,
        len(all_secs_data),
    )

    await write_bars_dfs(target_date, FrameType.MONTH, all_secs_data, prefix)

    logger.info("finished processing bars:1M for %s (%s)", target_date, prefix)
    return True


async def retrieve_bars_month(target_day, all_secs, all_indexes):
    all_stock_secs = all_secs
    all_index_secs = all_indexes

    # 读取本月第一天的证券列表
    if target_day == datetime.date(2005, 1, 31):
        m1d0 = datetime.date(2005, 1, 4)
    else:
        w0 = TimeFrame.month_shift(target_day, -1)
        m1d0 = TimeFrame.day_shift(w0, 1)
    print("first and last day of month: ", m1d0, target_day)

    if m1d0 != target_day:
        all_secs_in_w1d = await get_security_list(m1d0)
        if all_secs_in_w1d is None:
            logger.error("no security list in date %s", m1d0)
            return False

        all_secs_0, all_indexes_0 = split_securities(all_secs_in_w1d)
        if len(all_secs_0) == 0 or len(all_indexes_0) == 0:
            logger.error("no stock or index list in date %s", m1d0)
            return False

        # 合并数据，补上可能期间退市的股票
        all_stock_secs = all_secs.union(all_secs_0)
        all_index_secs = all_indexes.union(all_indexes_0)

    print(len(all_stock_secs), len(all_index_secs))

    rc = await get_1M_for_price(all_secs, target_day, m1d0, SecurityType.STOCK)
    if not rc:
        logger.error("failed to process stock price data (week): %s", target_day)
        return False

    rc = await get_1M_for_price(all_indexes, target_day, m1d0, SecurityType.INDEX)
    if not rc:
        logger.error("failed to process index price data (week): %s", target_day)
        return False

    return True
