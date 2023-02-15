import datetime
import logging

import arrow
import numpy as np
from coretypes import FrameType
from omicron.dal.cache import cache
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer
from omicron.models import get_influx_client
from omicron.models.security import Security
from omicron.models.timeframe import TimeFrame

from datascan.jq_fetcher import get_sec_bars_min
from datascan.scanner_utils import get_secs_from_bars
from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


async def get_sec_minutes_data_db(ft: FrameType, target_date: datetime.date):
    _start = datetime.datetime.combine(target_date, datetime.time(9, 30, 0))
    _end = datetime.datetime.combine(target_date, datetime.time(15, 30, 1))
    secs = await get_security_minutes_bars(ft, _start, _end)
    return secs


async def get_seclist_from_minutes_data_db(ft: FrameType, target_date: datetime.date):
    _start = datetime.datetime.combine(target_date, datetime.time(11, 30, 0))
    _end = datetime.datetime.combine(target_date, datetime.time(11, 30, 1))
    secs = await get_security_minutes_bars(ft, _start, _end)
    return secs


my_bars_dtype = np.dtype(
    [
        # use datetime64 may improve performance/memory usage, but it's hard to talk with other modules, like TimeFrame
        ("_time", "datetime64[s]"),
        ("code", "O"),
        ("open", "f4"),
        ("high", "f4"),
        ("low", "f4"),
        ("close", "f4"),
        ("volume", "f8"),
    ]
)


async def get_security_minutes_bars_bysecs(
    sec_list, ft: FrameType, start: datetime.datetime, end: datetime.datetime
):
    client = get_influx_client()
    measurement = "stock_bars_%s" % ft.value

    flux = (
        Flux()
        .measurement(measurement)
        .range(start, end)
        .bucket(client._bucket)
        .fields(["open", "high", "low", "close", "volume"])
        .tags({"code": sec_list})
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
    secs = actual.to_records(index=False).astype(my_bars_dtype)
    return secs


async def get_security_minutes_bars(
    ft: FrameType, start: datetime.datetime, end: datetime.datetime
):
    client = get_influx_client()
    measurement = "stock_bars_%s" % ft.value

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


def get_security_difference(secs_in_bars, all_stock, all_index, ft):
    # 检查是否有多余的股票
    x1 = secs_in_bars.difference(all_stock)
    y1 = x1.difference(all_index)
    if len(y1) > 0:  # 本地数据库中多余的股票或指数
        logger.error("secs in bars:%s but not in jq : %d", ft.value, len(y1))
        return False

    return True


def compare_seclist_difference(secs_in_day, secs_in_min, ft: FrameType):
    # 检查是否有多余的股票
    x1 = secs_in_min.difference(secs_in_day)
    if len(x1) > 0:  # 分钟线有，但日线啊没有
        logger.error("secs in bars:%s but not in bars:1d, %d", ft.value, len(x1))
        return False

    y1 = secs_in_day.difference(secs_in_min)
    if len(y1) > 0:  # 日线有，但分钟线没有
        logger.error("secs in bars:1d but not in bars:%s, %d", ft.value, len(y1))
        return False

    return True


async def validate_minute_bars_simple(
    target_date: datetime.date, all_secs_in_day, ft: FrameType
):
    logger.info("check bars:%s for open/close: %s", ft.value, target_date)
    all_db_secs_min = await get_seclist_from_minutes_data_db(ft, target_date)
    if all_db_secs_min is None or len(all_db_secs_min) == 0:
        logger.error(
            "failed to get sec list from db for bars:%s/open, %s", ft.value, target_date
        )
        return False

    secs_list = get_secs_from_bars(all_db_secs_min)
    rc = compare_seclist_difference(secs_list, all_secs_in_day, ft)
    if not rc:
        return False

    return True


async def validate_minute_bars(
    target_date: datetime.date, all_stock, all_index, all_secs_in_db, ft: FrameType
):
    logger.info("check bars:%s for open/close: %s", ft.value, target_date)
    all_db_secs_data = await get_sec_minutes_data_db(ft, target_date)
    if all_db_secs_data is None or len(all_db_secs_data) == 0:
        logger.error(
            "failed to get sec list from db for bars:%s/open, %s", ft.value, target_date
        )
        return False

    secs_list = get_secs_from_bars(all_db_secs_data)
    rc = get_security_difference(secs_list, all_stock, all_index, ft)
    if not rc:
        return False

    # 针对分钟线重采样数据

    # 因为是抽选股票，所以不能和日线那样做完全比较

    logger.info(
        "total secs downloaded from bars:%s/open@jq, %d, %s",
        ft.value,
        len(secs_list),
        target_date,
    )

    return True
