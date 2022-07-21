import datetime
import logging
import os

import arrow
import numpy as np
from coretypes import FrameType
from omicron.dal.cache import cache
from omicron.models.timeframe import TimeFrame

from datascan.day_check import validate_day_bars
from datascan.minute_check import validate_minute_bars
from datascan.security_list_check import validate_security_list
from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


def get_scanning_date_cursor(scanning_type: int):
    if scanning_type == 0:
        return "datascan:cursor:last_week_check"
    elif scanning_type == 1:
        return "datascan:cursor:history_check"
    else:
        raise TypeError(f"Scanning type not supported: {scanning_type}")


async def save_days_with_issues(target_date: datetime.date):
    key = "datascan:data_integrity_results"
    await cache.sys.lpush(key, target_date.strftime("%Y-%m-%d"))


async def update_scanning_date(scanning_type: int, target_date: datetime.date):
    key = get_scanning_date_cursor(scanning_type)
    date_str = await cache.sys.get(key)
    if not date_str:
        await cache.sys.set(key, target_date.strftime("%Y-%m-%d"))
        return True

    _last = arrow.get(date_str).date()
    if scanning_type == 0:  # 近期扫描存最后一天
        if target_date > _last:
            await cache.sys.set(key, target_date.strftime("%Y-%m-%d"))
    else:
        if target_date < _last:  # 历史扫描存最早的一天
            await cache.sys.set(key, target_date.strftime("%Y-%m-%d"))
    return True


async def check_running_conditions(instance):
    # 运行时间，休息日的10点到23点， 聚宽剩余quota大于10万
    dt1 = datetime.time(10, 0, 0)
    dt2 = datetime.time(23, 0, 0)

    now = datetime.datetime.now()
    now = datetime.datetime(2022, 7, 15, 18, 1, 0)
    nowtime = now.time()

    if nowtime < dt1 or nowtime > dt2:
        return False

    quota = await instance.get_quota()
    # quota = {"spare": 5000000}
    logger.info("current quota: %d", quota["spare"])
    if quota["spare"] < 10 * 10000:
        logger.error("quota less than 10,0000, break...")
        return False

    return True


async def get_scope_for_first_running(scanning_type: int, start: datetime.date):
    if scanning_type == 0:
        w0d0 = TimeFrame.day_shift(start, -5)  # 5个交易日之前
        w0d1 = start
    else:
        w0d0 = TimeFrame.day_shift(start, -10)  # 10个交易日之前
        w0d1 = TimeFrame.day_shift(start, -6)

    days = TimeFrame.get_frames(w0d0, w0d1, FrameType.DAY)
    _nplist = np.random.choice(days, 2, replace=False)
    return _nplist.tolist()


async def get_scope_for_next_running(
    scanning_type: int, last_scanning_day: datetime, last_trade_day: datetime.date
):
    if scanning_type == 0:  # 对上周的数据进行扫描
        _new_start = TimeFrame.day_shift(last_scanning_day, 1)
        days = TimeFrame.get_frames(_new_start, last_trade_day, FrameType.DAY)
        if days is None or len(days) < 5:  # 必须最少5天的间隔
            return None
        _nplist = np.random.choice(days, 2, replace=False)  # 随机取两天
        return _nplist.tolist()
    else:
        if last_scanning_day < datetime.date(2005, 1, 31):
            return None  # 不再检查

        w0d0 = TimeFrame.day_shift(last_scanning_day, -6)  # 6个交易日之前
        w0d1 = TimeFrame.day_shift(last_scanning_day, -1)
        days = TimeFrame.get_frames(w0d0, w0d1, FrameType.DAY)
        _nplist = np.random.choice(days, 2, replace=False)
        return _nplist.tolist()


async def get_time_scope_for_scanning(scanning_type: int, last_day: datetime.date):
    # 每个星期随机抽取两天，返回时间节点数组
    key = get_scanning_date_cursor(scanning_type)
    date_str = await cache.sys.get(key)
    if not date_str:  # first time running
        days = await get_scope_for_first_running(scanning_type, last_day)
    else:
        # 上周扫描存放的是最后一天，历史扫描存放的是最早的一天，分别处理
        last_scanning_day = arrow.get(date_str).date()
        days = await get_scope_for_next_running(
            scanning_type, last_scanning_day, last_day
        )

    return days


async def validate_data_all(target_date: datetime.date):
    # 读取当天的证券列表
    all_stock, all_index = await validate_security_list(target_date)
    if (all_stock is None or len(all_stock) == 0) or (
        all_index is None or len(all_index) == 0
    ):
        logger.error("no security list (stock or index) in date %s", target_date)
        return False

    # rc = await validate_day_bars(target_date, all_stock, all_index)
    rc = True
    if rc is False:
        logger.error("failed to get bars:1d for date %s", target_date)
        return False

    for ft in (
        FrameType.MIN1,
        FrameType.MIN5,
        FrameType.MIN15,
        FrameType.MIN30,
        FrameType.MIN60,
    ):
        rc = await validate_minute_bars(target_date, all_stock, all_index, ft)
        if rc is False:
            logger.error("failed to get bars:%s for date %s", ft.value, target_date)
            return False

    return True


async def reverse_scanner_handler(scanning_type: int):
    # 0，最近一周正确性扫描
    # 1，历史回溯扫描

    instance = AbstractQuotesFetcher.get_instance()
    # instance = None

    now = datetime.datetime.now()
    now = datetime.datetime(2022, 7, 16, 11, 1, 0)
    if TimeFrame.is_trade_day(now):
        logger.info("only scanning data in non-trade days: %s", now.date())
        return False

    last_trade_day = TimeFrame.day_shift(now, 0)

    while True:
        rc = await check_running_conditions(instance)
        if rc is False:
            return False

        days = await get_time_scope_for_scanning(scanning_type, last_trade_day)
        if days is None or len(days) == 0:
            logger.info(
                "skip this execution since no valid time scope found, %s (type %d)",
                last_trade_day,
                scanning_type,
            )
            return True

        days.sort()
        logger.info(
            "data scanning starts, from %s, total days: %d", last_trade_day, len(days)
        )

        for _day in days:
            _day = TimeFrame.int2date(_day)
            _day = datetime.date(2006, 3, 2)

            try:
                rc = await validate_data_all(_day)
            except Exception as e:
                logger.error("validate_data_all(%s) exception: %s", _day, e)
                rc = False

            rc = False
            if not rc:
                await save_days_with_issues(_day)
                logger.error("failed to validate data of %s", _day)
            else:
                logger.info("data integrity check success: %s", _day)

            # save timestamp
            await update_scanning_date(scanning_type, _day)

            input("next day...")

        if os.path.exists("/home/henry/zillionare/data_scanner/break.txt"):
            logger.info("break flag detected, exit")
            break

    return True