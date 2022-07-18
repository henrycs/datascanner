import datetime
import logging
import os
from tkinter import E

import arrow
from coretypes import FrameType
from omicron.dal.cache import cache
from omicron.models.timeframe import TimeFrame

from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher
from influx_data.security_list import get_security_list
from influx_tools import drop_bars_1d, drop_bars_1M, drop_bars_1w, drop_bars_via_scope
from rapidscan.fix_days import validate_bars_1d
from rapidscan.fix_minutes import validate_bars_1m
from rapidscan.get_days import retrieve_bars_1d
from rapidscan.get_week_month import retrieve_bars_1w, retrieve_bars_month
from time_utils import split_securities

logger = logging.getLogger(__name__)


def get_last_trade_day(target_year):
    now = datetime.datetime.now()
    if TimeFrame.is_trade_day(now):
        latest_trade_day = TimeFrame.day_shift(now, -1)
    else:
        latest_trade_day = TimeFrame.day_shift(now, 0)

    last_day = datetime.date(target_year, 12, 31)
    if last_day >= latest_trade_day:
        last_day = latest_trade_day

    return last_day


def check_next_running_time(target_day, last_day):
    if target_day > last_day:
        logger.info(
            "all data scanned, latest date: %s, %s(last day)", target_day, last_day
        )
        return False

    # now = datetime.datetime.now()
    # if now.hour == 0:  # 过了零点不运行，防止配额用尽
    #    logger.info("it's a new day now, exiting... %s, %s(last day), %s", target_day, last_day, now)
    #    return False

    return True


async def scanner_handler_week(target_year):
    key = "datascan:cursor:week"
    start_str = await cache.sys.get(key)

    first_day = False
    if start_str is None:
        first_day = True
        if target_year == 2005:
            _day = datetime.date(2005, 1, 4)
        else:
            _day = datetime.date(target_year, 1, 1)

        if not TimeFrame.is_trade_day(_day):
            target_day = TimeFrame.week_shift(_day, 1)
        else:
            if _day <= datetime.date(2005, 1, 7):
                target_day = datetime.date(2005, 1, 7)
            else:
                w1 = TimeFrame.floor(_day, FrameType.WEEK)
                if w1 == _day:
                    target_day = w1
                else:
                    target_day = TimeFrame.week_shift(_day, 1)
    else:
        target_day = arrow.get(start_str).date()  # 必须是正确的时间，上一次执行的当周最后一个交易日

    if target_day.year != target_year:
        print("date in cache not updated or removed: ", target_year, target_day)
        return False
    last_day = get_last_trade_day(target_year)

    instance = AbstractQuotesFetcher.get_instance()

    while True:
        quota = await instance.get_quota()
        if quota["spare"] < 20 * 10000:
            logger.error("quota less than 200,000, break...")
            return False
        logger.info("current quota: %d", quota["spare"])

        if first_day is False:  # 往后推进一周
            target_day = TimeFrame.week_shift(target_day, 1)
        else:
            first_day = False  # 起始的第一周已经处理

        if check_next_running_time(target_day, last_day):
            logger.info("fetch bars:1w from jq: %s", target_day)
        else:
            logger.info("finish running for %s", target_year)
            break

        # 读取当天的证券列表
        all_secs_in_cache = await get_security_list(target_day)
        if all_secs_in_cache is None:
            logger.error("no security list in date %s", target_day)
            return False

        all_secs, all_indexes = split_securities(all_secs_in_cache)
        if len(all_secs) == 0 or len(all_indexes) == 0:
            logger.error("no stock or index list in date %s", target_day)
            return False

        rc = await retrieve_bars_1w(target_day, all_secs, all_indexes)
        if rc is False:
            logger.error("failed to get bars:1w for date %s", target_day)
            return False

        # save timestamp
        await cache.sys.set(key, target_day.strftime("%Y-%m-%d"))
        # input("next week day...")

    return True


async def scanner_handler_month(target_year):
    key = "datascan:cursor:month"
    start_str = await cache.sys.get(key)

    first_day = False
    if start_str is None:
        first_day = True
        if target_year == 2005:
            _day = datetime.date(2005, 1, 4)
        else:
            _day = datetime.date(target_year, 1, 1)

        if not TimeFrame.is_trade_day(_day):
            target_day = TimeFrame.month_shift(_day, 1)
        else:
            if _day <= datetime.date(2005, 2, 1):
                target_day = datetime.date(2005, 1, 31)
            else:
                w1 = TimeFrame.floor(_day, FrameType.MONTH)
                if w1 == _day:
                    target_day = w1
                else:
                    target_day = TimeFrame.month_shift(_day, 1)
    else:
        target_day = arrow.get(start_str).date()  # 必须是正确的时间，上一次执行的当周最后一个交易日

    if target_day.year != target_year:
        print("date in cache not updated or removed: ", target_year, target_day)
        return False
    last_day = get_last_trade_day(target_year)

    instance = AbstractQuotesFetcher.get_instance()

    while True:
        quota = await instance.get_quota()
        if quota["spare"] < 10 * 10000:
            logger.error("quota less than 100,000, break...")
            return False
        logger.info("current quota: %d", quota["spare"])

        if first_day is False:  # 往后推进一月
            target_day = TimeFrame.month_shift(target_day, 1)
        else:
            first_day = False  # 起始的第一月已经处理

        if check_next_running_time(target_day, last_day):
            logger.info("fetch bars:1M from jq: %s", target_day)
        else:
            logger.info("finish running for %s", target_year)
            break

        # 读取当天的证券列表
        all_secs_in_cache = await get_security_list(target_day)
        if all_secs_in_cache is None:
            logger.error("no security list in date %s", target_day)
            return False

        all_secs, all_indexes = split_securities(all_secs_in_cache)
        if len(all_secs) == 0 or len(all_indexes) == 0:
            logger.error("no stock or index list in date %s", target_day)
            return False

        await retrieve_bars_month(target_day, all_secs, all_indexes)

        # save timestamp
        await cache.sys.set(key, target_day.strftime("%Y-%m-%d"))
        # input("next week day...")

    return True


async def scanner_main():
    target_year = 2025

    try:
        # await drop_bars_1d()
        # await drop_bars_1w()
        # await drop_bars_1M()
        # await drop_bars_via_scope(target_year, FrameType.WEEK)
        # return True

        while target_year < 2024:
            print("scanning in bars:1w ", target_year)

            # await scanner_handler_week(target_year)
            # await scanner_handler_month(target_year)
            await cache.sys.delete("datascan:cursor:week")
            rc = await scanner_handler_week(target_year)
            if rc is False:
                logger.error("failed to get data in %s for bars:1w", target_year)
                return False

            print("data processed in bars:1w, ", target_year)
            target_year += 1

        # await get_sec_bars_1d(['000031.XSHE','600788.XSHG'], target_day)
    except Exception as e:
        logger.info("failed to execution: %s", e)
        return False

    logger.info("all tasks finished.")
