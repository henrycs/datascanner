import datetime
import logging

import arrow
from coretypes import FrameType
from omicron.dal.cache import cache
from omicron.models.timeframe import TimeFrame

from download_bars.get_week_month import retrieve_bars_1w, retrieve_bars_month
from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher
from influx_data.security_list import get_security_list
from time_utils import check_running_conditions, get_cache_keyname

logger = logging.getLogger(__name__)


async def week_download_handler():
    ft = FrameType.WEEK

    key = get_cache_keyname(ft)
    start_str = await cache.sys.get(key)
    if start_str is None:
        target_day = datetime.date(2022, 7, 30)
    else:
        target_day = arrow.get(start_str).date()

    instance = AbstractQuotesFetcher.get_instance()

    while True:
        if target_day <= datetime.date(2005, 1, 7):  # 第一周
            logger.info("all weeks re-downloaded: %s", target_day)
            break

        rc = await check_running_conditions(instance)
        if rc is False:
            return False

        if not TimeFrame.is_trade_day(target_day):  # 节假日取临近的交易日，启动日期永远是周末
            target_day = TimeFrame.day_shift(target_day, 0)
        else:
            target_day = TimeFrame.week_shift(target_day, -1)  # cache记录的天数前推一周
        logger.info("fetchbars1w, from jq: %s", target_day)

        # 读取当天的证券列表
        all_secs = await get_security_list(target_day, "stock")
        if all_secs is None:
            logger.error("no security list in date %s", target_day)
            return False
        all_indexes = await get_security_list(target_day, "index")
        if all_indexes is None:
            logger.error("no security list in date %s", target_day)
            return False

        if len(all_secs) == 0 or len(all_indexes) == 0:
            logger.error("no stock or index list in date %s", target_day)
            return False

        # rc = await retrieve_bars_1w(target_day, all_secs, all_indexes)
        rc = True
        if rc is False:
            logger.error("failed to get bars:1w for date %s", target_day)
            return False

        # save timestamp
        await cache.sys.set(key, target_day.strftime("%Y-%m-%d"))
        input("next week day...")

    return True


async def month_download_handler():
    ft = FrameType.MONTH

    key = get_cache_keyname(ft)
    start_str = await cache.sys.get(key)
    if start_str is None:
        target_day = datetime.date(2022, 7, 31)
    else:
        target_day = arrow.get(start_str).date()

    instance = AbstractQuotesFetcher.get_instance()

    while True:
        if target_day <= datetime.date(2005, 1, 31):  # 第一月
            logger.info("all months re-downloaded: %s", target_day)
            break

        rc = await check_running_conditions(instance)
        if rc is False:
            return False

        if not TimeFrame.is_trade_day(target_day):  # 节假日取临近的交易日，启动日期永远是周末
            target_day = TimeFrame.month_shift(target_day, 0)
        else:
            target_day = TimeFrame.month_shift(target_day, -1)  # cache记录的天数前推一月
        logger.info("fetchbars1M, from jq: %s", target_day)

        # 读取当天的证券列表
        all_secs = await get_security_list(target_day, "stock")
        if all_secs is None:
            logger.error("no security list in date %s", target_day)
            return False
        all_indexes = await get_security_list(target_day, "index")
        if all_indexes is None:
            logger.error("no security list in date %s", target_day)
            return False

        if len(all_secs) == 0 or len(all_indexes) == 0:
            logger.error("no stock or index list in date %s", target_day)
            return False

        # rc = await retrieve_bars_month(target_day, all_secs, all_indexes)
        rc = True
        if rc is False:
            logger.error("failed to get bars:1w for date %s", target_day)
            return False

        # save timestamp
        await cache.sys.set(key, target_day.strftime("%Y-%m-%d"))
        input("next month day...")

    return True
