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

from datascan.jq_fetcher import (
    get_sec_bars_1d,
    get_sec_bars_min,
    get_sec_bars_pricelimits,
)
from datascan.scanner_utils import (
    compare_bars_for_openclose,
    compare_bars_min_for_openclose,
    get_secs_from_bars,
)
from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


async def get_sec_minutes_data_db(ft: FrameType, target_date: datetime.date):
    _start = datetime.datetime.combine(target_date, datetime.time(9, 30, 0))
    _end = datetime.datetime.combine(target_date, datetime.time(15, 30, 1))
    secs = await get_security_minutes_bars(ft, _start, _end)
    return secs


async def get_security_minutes_bars(
    ft: FrameType, start: datetime.datetime, end: datetime.datetime
):
    client = Security.get_influx_client()
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


async def validate_minute_bars(
    target_date: datetime.date, all_stock, all_index, ft: FrameType
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

    # 对比详细数据
    tmp_secs_list = list(secs_list)
    n_bars = 0
    if ft == FrameType.MIN1:
        _tmp = np.random.choice(tmp_secs_list, 24, replace=False)
        n_bars = 240
    elif ft == FrameType.MIN5:
        _tmp = np.random.choice(tmp_secs_list, 48, replace=False)
        n_bars = 48
    elif ft == FrameType.MIN15:
        _tmp = np.random.choice(tmp_secs_list, 64, replace=False)
        n_bars = 16
    elif ft == FrameType.MIN30:
        _tmp = np.random.choice(tmp_secs_list, 64, replace=False)
        n_bars = 8
    elif ft == FrameType.MIN60:
        _tmp = np.random.choice(tmp_secs_list, 128, replace=False)
        n_bars = 4
    else:
        raise ValueError("FrameType %s not supported" % ft)

    secs_set = set(_tmp)
    if len(secs_set) == 0:
        logger.error("failed to random.choice from sec list, %s (%s)", target_date, ft)
        return False

    all_jq_secs_data = await get_sec_bars_min(secs_set, target_date, ft)
    if len(all_jq_secs_data) == 0:
        logger.info("no valid price data from bars:%s@jq, %s", ft.value, target_date)
        return True

    # 因为是抽选股票，所以不能和日线那样做完全比较
    rc = compare_bars_min_for_openclose(
        secs_set, all_db_secs_data, all_jq_secs_data, n_bars
    )
    if not rc:
        logger.error("failed to compare sec data in db and jq, %s", target_date)
        return False

    logger.info(
        "total secs downloaded from bars:%s/open@jq, %d, %s",
        ft.value,
        len(all_jq_secs_data),
        target_date,
    )

    return True
