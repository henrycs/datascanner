import datetime
import logging

import arrow
import numpy as np
from coretypes import FrameType
from omicron.dal.cache import cache
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer
from omicron.models.security import Security
from omicron.models.timeframe import TimeFrame
from omicron.models import get_influx_client

from datascan.jq_fetcher import get_sec_bars_1w
from datascan.scanner_utils import compare_bars_wM_for_openclose, get_secs_from_bars
from datascan.security_list_check import (
    get_security_list_jq,
    split_securities_by_type_nparray,
)
from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


async def get_security_week_bars(start: datetime.datetime, end: datetime.datetime):
    client = get_influx_client()
    measurement = "stock_bars_1w"

    flux = (
        Flux()
        .measurement(measurement)
        .range(start, end)
        .bucket(client._bucket)
        .fields(["open", "high", "low", "close", "volume"])
    )

    data = await client.query(flux)
    if len(data) == 2:  # \r\n
        return []

    ds = DataframeDeserializer(
        sort_values="_time",
        usecols=["_time", "code", "open", "high", "low", "close", "volume"],
        time_col="_time",
        engine="c",
    )
    actual = ds(data)
    secs = actual.to_records(index=False)
    return secs


async def scan_bars_1w_for_seclist(d0: datetime.date, d1: datetime.date):
    start = datetime.datetime.combine(d0, datetime.time(0, 0, 0))
    end = datetime.datetime.combine(d1, datetime.time(23, 59, 59))

    all_secs_in_bars = await get_security_week_bars(start, end)
    if all_secs_in_bars is None or len(all_secs_in_bars) == 0:
        logger.error("no secs found in bars:1w/open, %s", d1)
        return None

    return all_secs_in_bars


def get_security_difference(secs_in_bars, all_stock, all_index):
    # 检查是否有多余的股票
    x1 = secs_in_bars.difference(all_stock)
    y1 = x1.difference(all_index)
    if len(y1) > 0:  # 本地数据库中多余的股票或指数
        logger.error("secs in bars:1d but not in jq : %d", len(y1))
        return False

    return True


async def validate_data_bars1w(target_date: datetime.date):
    d0, d1 = TimeFrame.get_frame_scope(target_date, FrameType.WEEK)

    all_secs_in_jq1 = await get_security_list_jq(d0)
    all_stock_jq1, all_index_jq1 = split_securities_by_type_nparray(all_secs_in_jq1)
    if (all_stock_jq1 is None or len(all_stock_jq1) == 0) or (
        all_index_jq1 is None or len(all_index_jq1) == 0
    ):
        logger.error("no security list (stock or index) in date %s", d0)
        return False

    all_secs_in_jq2 = await get_security_list_jq(d1)
    all_stock_jq2, all_index_jq2 = split_securities_by_type_nparray(all_secs_in_jq2)
    if (all_stock_jq2 is None or len(all_stock_jq2) == 0) or (
        all_index_jq2 is None or len(all_index_jq2) == 0
    ):
        logger.error("no security list (stock or index) in date %s", d1)
        return False

    # 合并本周第一天和最后一天的证券列表（不用判断两天是否为同一天）
    all_stock_jq = all_stock_jq1.union(all_stock_jq2)
    all_index_jq = all_index_jq1.union(all_index_jq2)

    logger.info("check bars:1w for open/close: %s - %s", d0, d1)
    all_db_secs_data = await scan_bars_1w_for_seclist(d0, d1)
    if all_db_secs_data is None or len(all_db_secs_data) == 0:
        logger.error("failed to get sec list from db for bars:1w/open, %s, %s", d0, d1)
        return False

    # 因为有停牌的现象，所以对比证券清单的时候，只能比较本地是否有多余的票
    secs_list_db = get_secs_from_bars(all_db_secs_data)
    rc = get_security_difference(secs_list_db, all_stock_jq, all_index_jq)
    if not rc:
        return False

    # 对比详细数据
    all_secs_set_jq = all_stock_jq.union(all_index_jq)
    all_jq_secs_data = await get_sec_bars_1w(all_secs_set_jq, d1, d0)
    logger.info(
        "total secs downloaded from bars:1w/open@jq, %d, %s, %s",
        len(all_jq_secs_data),
        d0,
        d1,
    )
    rc = compare_bars_wM_for_openclose(secs_list_db, all_db_secs_data, all_jq_secs_data)
    if not rc:
        logger.error("failed to compare sec data in db and jq, %s", target_date)
        return False

    return True
