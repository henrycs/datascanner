import datetime
import logging
import math
import os

import arrow
import numpy as np
from coretypes import FrameType
from omicron.dal.cache import cache
from omicron.extensions.decimals import math_round
from omicron.models.timeframe import TimeFrame

from datascan.day_check import (
    scan_bars_1d_for_seclist,
    scan_bars_1d_pricelimits_for_seclist,
)
from datascan.security_list_check import get_security_list_db

logger = logging.getLogger(__name__)
_global_stock_pricestats = {}
_global_highlimit_list = []


def _validate_price_limit(code: str, p: float, latest: float, _high, _low):
    if p > 0:
        if not _high:
            return False
        if latest == _high:
            return True
    else:
        if not _low:
            return False
        if latest == _low:
            return True

    return False


def _sum_high_pricelist(
    code: str, p: float, high: float, low: float, latest: float, highlimit, lowlimit
):
    global _global_stock_pricestats, _global_highlimit_list

    p = math_round(p, 2)  # xx.xx
    if p <= 1.0:
        _global_stock_pricestats["hp1"] += 1
        return

    # 是否涨停
    limited = _validate_price_limit(code, p, latest, highlimit, lowlimit)
    if limited:
        _global_highlimit_list.append(code.split(".")[0])

        _global_stock_pricestats["highlimit"] += 1
        if high != low:
            _global_stock_pricestats["highlimit_n"] += 1
        return

    if p > 5.0:
        _global_stock_pricestats["high"] += 1
    else:
        _global_stock_pricestats["hp5"] += 1


def _sum_low_pricelist(
    code: str, p: float, high: float, low: float, latest: float, highlimit, lowlimit
):
    global _global_stock_pricestats

    p = math_round(p, 2)  # xx.xx
    if p >= -1.0:
        _global_stock_pricestats["lp1"] += 1
        return

    # 是否跌停
    limited = _validate_price_limit(code, p, latest, highlimit, lowlimit)
    if limited:
        _global_stock_pricestats["lowlimit"] += 1
        if high != low:
            _global_stock_pricestats["lowlimit_n"] += 1
        return

    if p < -5.0:
        _global_stock_pricestats["low"] += 1
    else:
        _global_stock_pricestats["lp5"] += 1


async def _compare_close_limit(code, sec_data, sec_limit):
    global _global_stock_pricestats

    latest = math_round(sec_data["close"], 2)
    high = math_round(sec_data["high"], 2)
    low = math_round(sec_data["low"], 2)

    highlimit = math_round(sec_limit["high_limit"], 2)
    lowlimit = math_round(sec_limit["low_limit"], 2)
    closed = math_round((highlimit + lowlimit) / 2, 2)

    delta = latest - closed
    if math.isclose(delta, 0, abs_tol=1e-3):
        _global_stock_pricestats["p0"] += 1
        return

    p = delta / closed * 100
    if p > 0:
        _sum_high_pricelist(code, p, high, low, latest, highlimit, lowlimit)
    else:
        _sum_low_pricelist(code, p, high, low, latest, highlimit, lowlimit)


async def sum_pricestat_for_date(target_date: datetime.date):
    all_stock_db = await get_security_list_db(target_date, "stock")
    if all_stock_db is None:
        return None

    # 读取日线
    logger.info("check bars:1d for open/close: %s", target_date)
    all_db_secs_data1 = await scan_bars_1d_for_seclist(target_date)
    if all_db_secs_data1 is None or len(all_db_secs_data1) == 0:
        logger.error("failed to get sec list from db for bars:1d/open, %s", target_date)
        return False

    logger.info("check bars:1d for price limits: %s", target_date)
    all_db_secs_data2 = await scan_bars_1d_pricelimits_for_seclist(target_date)
    if all_db_secs_data2 is None or len(all_db_secs_data2) == 0:
        logger.error(
            "failed to get sec list from db for bars:1d/limits, %s", target_date
        )
        return False

    global _global_stock_pricestats, _global_highlimit_list
    _global_highlimit_list = []

    total_count = len(all_stock_db)
    _global_stock_pricestats = {
        "date": target_date.strftime("%Y-%m-%d"),
        "total": total_count,
        "highlimit": 0,
        "highlimit_n": 0,
        "high": 0,
        "hp5": 0,
        "hp1": 0,
        "p0": 0,
        "lowlimit": 0,
        "lowlimit_n": 0,
        "low": 0,
        "lp5": 0,
        "lp1": 0,
    }

    pricelimit_list = {}
    for _data in all_db_secs_data2:
        code = _data["code"]
        pricelimit_list[code] = _data

    for sec_data in all_db_secs_data1:
        code = sec_data["code"]
        if code in all_stock_db:
            sec_data_limits = pricelimit_list[code]
            await _compare_close_limit(code, sec_data, sec_data_limits)

    _global_stock_pricestats["codelist"] = _global_highlimit_list
    print(_global_stock_pricestats)
    return _global_stock_pricestats


async def sum_price_stats():
    now = datetime.datetime.now()
    target_date = TimeFrame.day_shift(now, -30)
    file = "/home/henry/zillionare/pricestats.txt"
    with open(file, "w") as f:
        while target_date < now.date():
            param = await sum_pricestat_for_date(target_date)
            f.write(f"{str(param)}\n")

            target_date = TimeFrame.day_shift(target_date, 1)

    print("all finished")
