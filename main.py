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
from rapidscan.fix_minutes import validate_bars_1m
from rapidscan.get_days import retrieve_bars_1d
from time_utils import split_securities


logger = logging.getLogger(__name__)


async def check_running_conditions(instance):
    dt1 = datetime.time(3, 0, 0)
    dt2 = datetime.time(8, 0, 0)
    dt3 = datetime.time(17, 10, 0)

    now = datetime.datetime.now()    
    #now = datetime.datetime(2022, 7, 15, 18, 1, 0)
    nowtime = now.time()    

    quota = await instance.get_quota()
    logger.info("current quota: %d", quota['spare'])

    if not TimeFrame.is_trade_day(now):
        if quota['spare'] < 10 * 10000:
            logger.error("quota less than 10,0000, break...")
            return False        
        return True

    # in trade day
    if nowtime < dt1:  # 3点前有其他任务
        return False
    if nowtime > dt2 and nowtime < dt3:  #交易时间段不执行
        return False

    # 工作日需要保留400万给白天使用（实际需要256万）
    if nowtime < dt2:
        if quota['spare'] < 400 * 10000:
            logger.error("quota less than 4,000,000, break...")
            return False
    else:
        if quota['spare'] < 10 * 10000:
            logger.error("quota less than 10,0000, break...")
            return False

    return True


async def scanner_handler_day():
    # set start day
    epoch_start_day = datetime.date(2005, 1, 4)
    instance = AbstractQuotesFetcher.get_instance()

    key = "datascan:cursor:day"
    start_str = await cache.sys.get(key)

    if start_str is None:
        target_day = datetime.date(2022, 7, 16)
    else:
        target_day = arrow.get(start_str).date()    

    while(True):
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


async def get_latest_day_str():
    day_start_str = await cache.sys.get("datascan:cursor:day")
    if day_start_str is None:
        return None
    else:
        return arrow.get(day_start_str).date()


async def scanner_handler_minutes(ft: FrameType):
    epoch_start_day = datetime.date(2005, 1, 4)
    instance = AbstractQuotesFetcher.get_instance()

    if ft == FrameType.MIN1:
        key = "datascan:cursor:min1"
    else:
        return False
    start_str = await cache.sys.get(key)

    if start_str is None:
        target_day = datetime.date(2022, 7, 16)
    else:
        target_day = arrow.get(start_str).date()

    while(True):
        _latest_day = await get_latest_day_str()
        if _latest_day is None:
            logger.error("no datascan:cursor:day found!!!")
            return False

        if target_day <= _latest_day:
            logger.info("finish running for bars:1m, %s (latest day %s)", target_day, _latest_day)
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

        if ft == FrameType.MIN1:
            rc = await validate_bars_1m(target_day, secs_in_bars1d)
            if rc is False:
                logger.error("failed to get bars:1m for date %s", target_day)
                return False

        # save timestamp
        await cache.sys.set(key, target_day.strftime("%Y-%m-%d"))
        #input("next day...")
        
        if os.path.exists("/home/henry/zillionare/omega_scanner_1m/break1m.txt"):
            logger.info("break flag detected, exit, last day: %s", target_day)
            break

    return True


async def scanner_main():
    try:
        #await drop_bars_1d()
        #await drop_bars_1w()
        #await drop_bars_1M()
        #await drop_bars_via_scope(target_year, FrameType.WEEK)
        #return True

        await scanner_handler_minutes(FrameType.MIN1)
    
        #await get_sec_bars_1d(['000031.XSHE','600788.XSHG'], target_day)
    except Exception as e:
        logger.info("failed to execution: %s", e)
        return False
    
    logger.info("all tasks finished.")

    
