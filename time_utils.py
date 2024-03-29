import datetime
import logging
import os

import arrow
from coretypes import FrameType
from omicron.dal.cache import cache
from omicron.models.timeframe import TimeFrame

from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


def split_securities(all_secs_in_cache):
    all_secs = set()
    all_indexes = set()
    for sec in all_secs_in_cache:
        code = sec["code"]
        _type = sec["type"]
        if _type == "stock":
            all_secs.add(code)
        else:
            all_indexes.add(code)

    return all_secs, all_indexes


def get_cache_keyname(ft: FrameType):
    if ft in (
        FrameType.MIN1,
        FrameType.MIN5,
        FrameType.MIN15,
        FrameType.MIN30,
        FrameType.MIN60,
        FrameType.DAY,
        FrameType.WEEK,
        FrameType.MONTH,
    ):
        return "datascan:cursor:%s" % ft.value
    else:
        raise TypeError("FrameType not supported!")


async def check_running_conditions(instance):
    # 周六需要取数据
    dt1 = datetime.time(2, 0, 0)
    dt2 = datetime.time(3, 30, 0)
    # 仅限周六取数据
    dt3 = datetime.time(8, 0, 0)
    dt4 = datetime.time(9, 32, 0)

    now = datetime.datetime.now()
    nowtime = now.time()

    quota = await instance.get_quota()
    # quota = {"spare": 5000000}
    logger.info("current quota: %d", quota["spare"])

    # in trade day and saturday
    if nowtime > dt1 and nowtime < dt2:
        return False
    if nowtime > dt3 and nowtime < dt4:
        return False

    if not TimeFrame.is_trade_day(now):
        if quota["spare"] < 10 * 10000:
            logger.error("quota less than 10,0000, break...")
            return False
        return True

    # 工作日需要保留400万给白天使用（实际需要256万）
    if nowtime < dt4:
        if quota["spare"] < 400 * 10000:
            logger.error("quota less than 4,000,000, break...")
            return False
    else:
        if quota["spare"] < 10 * 10000:
            logger.error("quota less than 10,0000, break...")
            return False

    return True


async def get_latest_day_str():
    key = get_cache_keyname(FrameType.DAY)
    day_start_str = await cache.sys.get(key)
    if day_start_str is None:
        return None
    else:
        return arrow.get(day_start_str).date()
