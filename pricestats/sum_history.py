import datetime
import logging
import math
import os

import arrow
import numpy as np
from coretypes import FrameType
from omicron.dal.cache import cache
from omicron.extensions.decimals import math_round
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame

from datascan.day_check import (
    scan_bars_1d_for_seclist,
    scan_bars_1d_pricelimits_for_seclist,
)
from datascan.minute_check import get_security_minutes_bars_bysecs
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


async def _compare_close_and_highlimit(code, sec_data, highlimit):
    latest = math_round(sec_data["close"], 2)
    high = math_round(sec_data["high"], 2)
    # low = math_round(sec_data["low"], 2)

    # highlimit = math_round(sec_limit["high_limit"], 2)
    # lowlimit = math_round(sec_limit["low_limit"], 2)

    if latest == highlimit:
        print(latest, high, highlimit)
        return 2  # 涨停
    if high == highlimit:
        print(latest, high, highlimit)
        return 1  # 触及涨停

    return 0


async def _calculate_highlimit_suminfo(highlimit: float, sec_data: list):
    # 触及涨停，第一次时间，上影线长度，后续开板封板次数一并计算
    dt_list = []
    _start_dt = None  # 初始状态，无涨停

    for _r in sec_data:
        _close = math_round(_r["close"], 2)
        _open = math_round(_r["open"], 2)
        _high = math_round(_r["high"], 2)
        _low = math_round(_r["low"], 2)
        if _high == highlimit:  # bar内触及涨停
            if not _start_dt:  # 上一个bar无涨停
                _start_dt = _r["_time"].item()  # 标记这个bar

                if _close != highlimit:  # 没封住
                    dt_list.append(_start_dt)  # 记录时间点，到此终结
                    _start_dt = None

                # 前一个bar未涨停收盘，这个bar收盘涨停时，算涨停开始

                continue  # 继续下一个bar

            # _start_dt有记录（上一个bar收盘时涨停了）
            if _open != highlimit:  # 算一次开板
                dt_list.append(_start_dt)

                _start_dt = _r["_time"].item()  # 从当前bar重新开始
                if _close != highlimit:  # 没封住
                    dt_list.append(_start_dt)  # 记录时间点，到此终结
                    _start_dt = None

                # 收盘涨停，继续标记上一个时间点

                # 继续下一个bar
            else:
                # 开盘延续了涨停，看bar内是否低走
                if _low == highlimit:  # bar内封住涨停
                    continue

                # 低走了，算一次开板
                dt_list.append(_start_dt)

                # 再看收盘
                if _close != highlimit:  # 没涨停
                    _start_dt = None  # 清零
                else:
                    _start_dt = _r["_time"].item()  # 从当前bar开始

                # 继续下一个bar
        else:
            # 未触及涨停，记录之前的时间点
            if _start_dt:
                dt_list.append(_start_dt)
                _start_dt = None

    # 填充最后一个节点
    if _start_dt:
        dt_list.append(_start_dt)

    return dt_list


async def sum_highlimits_for_date(target_date: datetime.date):
    # 收集涨停或者触及涨停的信息
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

    pricelimit_list = {}
    for _data in all_db_secs_data2:
        code = _data["code"]
        pricelimit_list[code] = _data

    results = {}
    _start = datetime.datetime.combine(target_date, datetime.time(9, 30, 0))
    _end = datetime.datetime.combine(target_date, datetime.time(15, 0, 1))

    for sec_data in all_db_secs_data1:
        code = sec_data["code"]
        name = Stock(code).display_name
        if name.find("ST") != -1:
            continue

        if code in all_stock_db:
            sec_data_limits = pricelimit_list[code]
            highlimit = math_round(sec_data_limits["high_limit"], 2)
            lowlimit = math_round(sec_data_limits["low_limit"], 2)
            base_price = math_round((highlimit + lowlimit) / 2, 2)
            _type = await _compare_close_and_highlimit(code, sec_data, highlimit)
            if _type == 0:
                continue

            db_min_data = await get_security_minutes_bars_bysecs(
                [code], FrameType.MIN1, _start, _end
            )
            if db_min_data is None or len(db_min_data) == 0:
                logger.error(
                    "failed to get sec list from db for bars:%s/open, %s",
                    FrameType.MIN30.value,
                    target_date,
                )
                break

            info = await _calculate_highlimit_suminfo(highlimit, db_min_data)
            if not info:
                logger.info("no high price found in MIN1 bars: %s", code)
                continue

            # 处理结果
            _sum_info = {"highlimit": highlimit, "type": _type}
            close_price = math_round(sec_data["close"], 2)
            open_price = math_round(sec_data["open"], 2)
            if _type == 1:
                base = max(open_price, close_price)
                delta = math_round((highlimit / base - 1) * 100, 2)
                _sum_info["first_date"] = info[0].strftime("%Y-%m-%d %H:%M:%S")
                _sum_info["break_times"] = len(info)
                _sum_info["last_date"] = ""
            else:
                delta = 0
                _sum_info["first_date"] = info[0].strftime("%Y-%m-%d %H:%M:%S")
                _sum_info["break_times"] = len(info) - 1  # 最后一个不算
                _sum_info["last_date"] = info[-1].strftime("%Y-%m-%d %H:%M:%S")

            _sum_info["delta"] = delta
            _sum_info["open_price"] = open_price
            _sum_info["close_price"] = close_price
            _sum_info["base_price"] = base_price

            results[code] = _sum_info

    # print(results)
    print(target_date)
    return {"dt": target_date.strftime("%Y-%m-%d"), "data": results}


async def sum_price_stats():
    now = datetime.datetime.now()
    target_date = TimeFrame.day_shift(now, -30)
    # file = "/home/henry/zillionare/pricestats.txt"
    file = "/home/henry/zillionare/highlimit_sum.txt"
    with open(file, "w") as f:
        while target_date < now.date():
            # param = await sum_pricestat_for_date(target_date)
            param = await sum_highlimits_for_date(target_date)
            f.write(f"{str(param)}\n")

            target_date = TimeFrame.day_shift(target_date, 1)

    print("all finished")
