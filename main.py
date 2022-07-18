import datetime
import logging
import os
import arrow
from coretypes import FrameType
from omicron.models.timeframe import TimeFrame
from omicron.dal.cache import cache
from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher
from influx_tools import drop_bars_1M, drop_bars_1d,  drop_bars_1w, drop_bars_via_scope

from influx_data.security_list import get_security_list
from rapidscan.fix_days import scan_bars_1d_for_seclist
from rapidscan.fix_minutes import validate_bars_min
from rapidscan.get_days import retrieve_bars_1d
from time_utils import check_running_conditions, get_cache_keyname, get_latest_day_str, split_securities


logger = logging.getLogger(__name__)


async def scanner_handler_minutes(ft: FrameType):
    if ft not in (FrameType.MIN1, FrameType.MIN5, FrameType.MIN30, FrameType.MIN60):
        raise TypeError(f"FrameType not supported!")

    epoch_start_day = datetime.date(2005, 1, 4)
    #instance = AbstractQuotesFetcher.get_instance()
    instance = None
    
    key = get_cache_keyname(ft)
    start_str = await cache.sys.get(key)
    if start_str is None:
        target_day = datetime.date(2022, 7, 16)
    else:
        target_day = arrow.get(start_str).date()

    while(True):
        _latest_day = await get_latest_day_str()
        if _latest_day is None:
            logger.error("no datascan:cursor:1d found!!!")
            return False

        if target_day <= _latest_day:
            logger.info("finish running for bars:%s, %s (latest day %s)", ft.value, target_day, _latest_day)
            break

        rc = await check_running_conditions(instance)
        if rc is False:
            return False

        if not TimeFrame.is_trade_day(target_day):  # 节假日取临近的交易日
            target_day = TimeFrame.day_shift(target_day, 0)
        else:
            target_day = TimeFrame.day_shift(target_day, -1)  # cache记录的天数前推一天
        logger.info("fetchbars1d, from jq: %s", target_day)

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
        
        if os.path.exists("/home/henry/zillionare/omega_scanner_min/break1m.txt"):
            logger.info("break flag detected, exit, last day: %s", target_day)
            break

    return True


async def scanner_main(ft: FrameType):
    try:
        #await drop_bars_1d()
        #await drop_bars_1w()
        #await drop_bars_1M()
        #await drop_bars_via_scope(target_year, FrameType.WEEK)
        #return True

        await scanner_handler_minutes(ft)

    except Exception as e:
        logger.info("failed to execution: %s", e)
        return False
    
    logger.info("all tasks finished.")

    
