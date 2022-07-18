import asyncio
import datetime
import logging
import os
from os import path
import time
from typing import List

import cfg4py
import numpy as np
from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


async def get_sec_bars_1m(secs_set: set, dt: datetime.date):
    secs = list(secs_set)
    all_valid_bars = {}

    instance = AbstractQuotesFetcher.get_instance()
    end_dt = datetime.datetime.combine(dt, datetime.time(15, 0))

    start = 0
    left = len(secs)
    while left > 0:
        if left > 15:  # 一次15*240=3500根
            end = start + 15
            left -= 15
        else:
            end = start + left
            left = 0

        _sec_list = secs[start : end]
        bars = await instance.get_bars_batch(_sec_list, end_dt, 240, "1m", include_unclosed=True)
        for code in list(bars.keys()):
            if not len(bars[code]):
                del bars[code]
                print("delete empty bar: ", code)
                continue
            if np.any(np.isnan(bars[code]["amount"])) or np.any(np.isnan(bars[code]["volume"])):
                del bars[code]
                print("delete bar with nan amount/volume: ", code)
                continue
            _date_in_bar = bars[code]["frame"][0]
            if _date_in_bar.date() != dt:
                del bars[code]
                logger.info("delete bar with earlier date (bars:1m): %s, %s", code, _date_in_bar)
                continue
            
            #print("sec added: ", code)
            all_valid_bars[code] = bars[code]

        start = end
    
    return all_valid_bars


async def get_sec_bars_1d(secs_set: set, dt: datetime.date):
    secs = list(secs_set)
    all_valid_bars = {}

    instance = AbstractQuotesFetcher.get_instance()
    end_dt = datetime.datetime.combine(dt, datetime.time(15, 0))

    start = 0
    left = len(secs)
    while left > 0:
        if left > 3000:
            end = start + 3000
            left -= 3000
        else:
            end = start + left
            left = 0

        _sec_list = secs[start : end]
        bars = await instance.get_bars_batch(_sec_list, end_dt, 1, "1d", include_unclosed=True)
        for code in list(bars.keys()):
            if not len(bars[code]):
                del bars[code]
                print("delete empty bar: ", code)
                continue
            if np.any(np.isnan(bars[code]["amount"])) or np.any(np.isnan(bars[code]["volume"])):
                del bars[code]
                print("delete bar with nan amount/volume: ", code)
                continue
            _date_in_bar = bars[code]["frame"][0]
            if _date_in_bar != dt:
                del bars[code]
                logger.info("delete bar with earlier date (bars:1d): %s, %s", code, _date_in_bar)
                continue
            
            #print("sec added: ", code)
            all_valid_bars[code] = bars[code]

        start = end
    
    return all_valid_bars


async def get_sec_bars_pricelimits(secs_set: set, dt: datetime.date):
    secs = list(secs_set)
    all_valid_bars = []

    instance = AbstractQuotesFetcher.get_instance()

    end = datetime.datetime.combine(dt, datetime.time(15, 0))

    bars = await instance.get_trade_price_limits(secs, end)
    bars = bars[~np.isnan(bars["low_limit"])]
    bars = bars[~np.isnan(bars["high_limit"])]
    bars = bars[bars["frame"] == dt]

    for info in bars:
        code = info[1]
        if info[0] != dt:
            logger.info("no data in target date (price limits), %s, %s", code, info)
    
    return bars


async def get_sec_bars_pricelimits_list(secs_set: set, dt: datetime.date):
    secs = list(secs_set)
    all_valid_bars = []

    instance = AbstractQuotesFetcher.get_instance()

    end = datetime.datetime.combine(dt, datetime.time(15, 0))

    bars = await instance.get_trade_price_limits(secs, end)
    bars = bars[~np.isnan(bars["low_limit"])]
    bars = bars[~np.isnan(bars["high_limit"])]

    for info in bars:
        code = info[1]
        if info[0] != dt:
            logger.info("no data in target date (price limits), %s, %s", code, info)
        else:
            #print("sec added: ", code)
            all_valid_bars.append(info)
    
    return all_valid_bars


async def get_sec_bars_1w(secs_set: set, dt: datetime.date, d0: datetime.date):
    secs = list(secs_set)
    all_valid_bars = {}

    instance = AbstractQuotesFetcher.get_instance()
    end_dt = datetime.datetime.combine(dt, datetime.time(15, 0))

    start = 0
    left = len(secs)
    while left > 0:
        if left > 3000:
            end = start + 3000
            left -= 3000
        else:
            end = start + left
            left = 0

        _sec_list = secs[start : end]
        bars = await instance.get_bars_batch(_sec_list, end_dt, 1, "1w", include_unclosed=True)
        for code in list(bars.keys()):
            if not len(bars[code]):
                del bars[code]
                print("delete empty bar: ", code)
                continue
            if np.any(np.isnan(bars[code]["amount"])) or np.any(np.isnan(bars[code]["volume"])):
                del bars[code]
                print("delete bar with nan amount/volume: ", code)
                continue
            _date_in_bar = bars[code]["frame"][0]
            if _date_in_bar < d0 or _date_in_bar > dt:
                del bars[code]
                logger.info("delete bar with invalid date (not in this week): %s, %s", code, _date_in_bar)
                continue
            
            #print("sec added: ", code)
            all_valid_bars[code] = bars[code]

        start = end
    
    return all_valid_bars



async def get_sec_bars_1M(secs_set: set, dt: datetime.date, d0: datetime.date):
    secs = list(secs_set)
    all_valid_bars = {}

    instance = AbstractQuotesFetcher.get_instance()
    end_dt = datetime.datetime.combine(dt, datetime.time(15, 0))

    start = 0
    left = len(secs)
    while left > 0:
        if left > 3000:
            end = start + 3000
            left -= 3000
        else:
            end = start + left
            left = 0

        _sec_list = secs[start : end]
        bars = await instance.get_bars_batch(_sec_list, end_dt, 1, "1M", include_unclosed=True)
        for code in list(bars.keys()):
            if not len(bars[code]):
                del bars[code]
                print("delete empty bar: ", code)
                continue
            if np.any(np.isnan(bars[code]["amount"])) or np.any(np.isnan(bars[code]["volume"])):
                del bars[code]
                print("delete bar with nan amount/volume: ", code)
                continue
            _date_in_bar = bars[code]["frame"][0]
            if _date_in_bar < d0 or _date_in_bar > dt:
                del bars[code]
                logger.info("delete bar with invalid date (not in this month): %s, %s", code, _date_in_bar)
                continue
            
            #print("sec added: ", code)
            all_valid_bars[code] = bars[code]

        start = end
    
    return all_valid_bars