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

from datascan.jq_fetcher import get_sec_bars_1w
from datascan.scanner_utils import compare_bars_wM_for_openclose, get_secs_from_bars
from datascan.security_list_check import (
    get_security_list_jq,
    split_securities_by_type_nparray,
)
from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


async def get_security_week_bars(start: datetime.datetime, end: datetime.datetime):
    client = Security.get_influx_client()
    measurement = "stock_bars_1w"

    flux = (
        Flux()
        .measurement(measurement)
        .range(start, end)
        .bucket(client._bucket)
        .fields(["open", "close", "high", "volume"])
    )

    data = await client.query(flux)
    if len(data) == 2:  # \r\n
        return []

    ds = DataframeDeserializer(
        sort_values="_time",
        usecols=["_time", "code", "open", "close", "high", "volume"],
        time_col="_time",
        engine="c",
    )
    actual = ds(data)
    secs = actual.to_records(index=False)
    return secs


async def scan_bars_1w_for_seclist(target_date: datetime.date):
    start = datetime.datetime.combine(target_date, datetime.time(0, 0, 0))
    end = datetime.datetime.combine(target_date, datetime.time(23, 59, 59))

    all_secs_in_bars = await get_security_week_bars(start, end)
    if all_secs_in_bars is None or len(all_secs_in_bars) == 0:
        logger.error("no secs found in bars:1w/open, %s", target_date)
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
    all_secs_in_jq = await get_security_list_jq(target_date)
    all_stock_jq, all_index_jq = split_securities_by_type_nparray(all_secs_in_jq)
    if (all_stock_jq is None or len(all_stock_jq) == 0) or (
        all_index_jq is None or len(all_index_jq) == 0
    ):
        logger.error("no security list (stock or index) in date %s", target_date)
        return False

    logger.info("check bars:1w for open/close: %s", target_date)
    all_db_secs_data = await scan_bars_1w_for_seclist(target_date)
    if all_db_secs_data is None or len(all_db_secs_data) == 0:
        logger.error("failed to get sec list from db for bars:1w/open, %s", target_date)
        return False

    # 因为有停牌的现象，所以对比证券清单的时候，只能比较本地是否有多余的票
    secs_list_db = get_secs_from_bars(all_db_secs_data)
    rc = get_security_difference(secs_list_db, all_stock_jq, all_index_jq)
    if not rc:
        return False

    # 对比详细数据
    all_secs_set_jq = all_stock_jq.union(all_index_jq)
    all_jq_secs_data = await get_sec_bars_1w(all_secs_set_jq, target_date)
    logger.info(
        "total secs downloaded from bars:1w/open@jq, %d, %s",
        len(all_jq_secs_data),
        target_date,
    )
    rc = compare_bars_wM_for_openclose(secs_list_db, all_db_secs_data, all_jq_secs_data)
    if not rc:
        logger.error("failed to compare sec data in db and jq, %s", target_date)
        return False

    return True
