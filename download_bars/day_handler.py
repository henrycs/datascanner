import datetime
import logging
import os

import arrow
from coretypes import FrameType
from omicron.dal.cache import cache
from omicron.models.timeframe import TimeFrame

from download_bars.get_days import retrieve_bars_1d
from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher
from influx_data.security_list import get_security_list
from rapidscan.fix_days import scan_bars_1d_for_seclist
from rapidscan.fix_minutes import validate_bars_min
from time_utils import check_running_conditions, split_securities

logger = logging.getLogger(__name__)


async def scanner_handler_day():
    # set start day
    epoch_start_day = datetime.date(2005, 1, 4)
    instance = AbstractQuotesFetcher.get_instance()

    key = "datascan:cursor:1d"
    start_str = await cache.sys.get(key)

    if start_str is None:
        target_day = datetime.date(2022, 7, 16)
    else:
        target_day = arrow.get(start_str).date()

    while True:
        if target_day <= epoch_start_day:
            logger.info("finish running for bars:1d, %s", target_day)
            break

        rc = await check_running_conditions(instance)
        if rc is False:
            return False

        if not TimeFrame.is_trade_day(target_day):  # 节假日取临近的交易日
            target_day = TimeFrame.day_shift(target_day, 0)
        else:
            target_day = TimeFrame.day_shift(target_day, -1)  # cache记录的天数前推一天
        logger.info("fetchbars1d, from jq: %s", target_day)

        # 读取当天的证券列表
        all_secs_in_cache = await get_security_list(target_day)
        if all_secs_in_cache is None:
            logger.error("no security list in date %s", target_day)
            return False

        all_secs, all_indexes = split_securities(all_secs_in_cache)
        if len(all_secs) == 0 or len(all_indexes) == 0:
            logger.error("no stock or index list in date %s", target_day)
            return False

        rc = await retrieve_bars_1d(target_day, all_secs, all_indexes)
        if rc is False:
            logger.error("failed to get bars:1d for date %s", target_day)
            return False

        # save timestamp
        await cache.sys.set(key, target_day.strftime("%Y-%m-%d"))
        input("next day...")

        if os.path.exists("/home/henry/zillionare/omega_scanner/break.txt"):
            logger.info("break flag detected, exit, last day: %s", target_day)
            break

    return True
