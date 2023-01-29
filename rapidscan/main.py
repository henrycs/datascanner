import datetime
import logging
import os

import arrow
from coretypes import FrameType
from omicron.dal.cache import cache
from omicron.models.timeframe import TimeFrame

from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher
from influx_data.security_list import get_security_list
from rapidscan.fix_days import scan_bars_1d_for_seclist
from rapidscan.fix_minutes import validate_bars_min
from time_utils import (
    check_running_conditions,
    get_cache_keyname,
    get_latest_day_str,
    split_securities,
)

logger = logging.getLogger(__name__)


async def scanner_handler_minutes(ft: FrameType, reload_days: bool):
    if ft not in (
        FrameType.MIN1,
        FrameType.MIN5,
        FrameType.MIN15,
        FrameType.MIN30,
        FrameType.MIN60,
    ):
        raise TypeError("FrameType not supported!")

    instance = AbstractQuotesFetcher.get_instance()
    # instance = None

    key = get_cache_keyname(ft)
    start_str = await cache.sys.get(key)
    if start_str is None:
        target_day = datetime.date(2022, 7, 16)
    else:
        target_day = arrow.get(start_str).date()

    while True:
        _latest_day = await get_latest_day_str()
        if _latest_day is None:
            logger.error("no datascan:cursor:1d found!!!")
            return False

        if target_day <= _latest_day:
            logger.info(
                "finish running for bars:%s, %s (latest day %s)",
                ft.value,
                target_day,
                _latest_day,
            )
            break

        rc = await check_running_conditions(instance)
        if rc is False:
            return False

        if not TimeFrame.is_trade_day(target_day):  # 节假日取临近的交易日
            target_day = TimeFrame.day_shift(target_day, 0)
        else:
            target_day = TimeFrame.day_shift(target_day, -1)  # cache记录的天数前推一天
        logger.info("fetchbars1d, from jq: %s", target_day)

        # 从本地文件中读取有缺失记录的日期，逐天重新下载数据
        if reload_days:
            # 读取当天的证券列表
            all_secs_in_cache = await get_security_list(target_day)
            if all_secs_in_cache is None:
                logger.error("no security list in date %s", target_day)
                return False

            all_secs, all_indexes = split_securities(all_secs_in_cache)
            if len(all_secs) == 0 or len(all_indexes) == 0:
                logger.error("no stock or index list in date %s", target_day)
                return False

            rc = await validate_bars_min(target_day, all_secs, all_indexes, ft)
            if rc is False:
                logger.error("failed to get bars:%s for date %s", ft.value, target_day)
                return False
        else:
            # 获取日线的证券清单（已校正）
            secs_in_bars1d = await scan_bars_1d_for_seclist(target_day)
            if secs_in_bars1d is None:
                return False

            rc = await validate_bars_min(target_day, secs_in_bars1d, ft)
            if rc is False:
                logger.error("failed to get bars:%s for date %s", ft.value, target_day)
                return False

        # save timestamp
        await cache.sys.set(key, target_day.strftime("%Y-%m-%d"))
        input("next day...")

        if os.path.exists("/home/app/zillionare/omega_scanner_min/break1m.txt"):
            logger.info("break flag detected, exit, last day: %s", target_day)
            break

    return True


async def test_cache():
    dt = datetime.datetime(2022, 7, 19, 9, 35)
    p1 = 8.64
    p2 = 8.66
    p3 = 8.63
    p4 = 8.66
    vol1 = 121500
    vol2 = 1050341
    factor = 1.110297
    code = "000982.XSHE"

    import time

    key = "datascan:codelist"
    t0 = time.time()
    pipeline = cache.security.pipeline()
    for i in range(0, 5000):
        _dstr = dt.strftime("%y%m%d%h%M%s")
        data_str = f"{_dstr},{p1},{p2},{p3},{p4},{vol1},{vol2},{factor}"
        code = "0%d.XSHE" % i
        pipeline.hset(key, code, data_str)

    await pipeline.execute()
    t1 = time.time()
    print("time cost: ", t1 - t0)
