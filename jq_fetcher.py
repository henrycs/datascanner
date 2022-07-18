import datetime
import logging
from typing import List

import numpy as np
from coretypes import FrameType

from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


async def get_sec_bars_min(secs_set: set, dt: datetime.date, ft: FrameType):
    secs = list(secs_set)
    all_valid_bars = {}

    instance = AbstractQuotesFetcher.get_instance()
    end_dt = datetime.datetime.combine(dt, datetime.time(15, 0))

    if ft == FrameType.MIN1:
        max_secs = 15
        n_bars = 240
    elif ft == FrameType.MIN5:
        max_secs = 80
        n_bars = 48
    elif ft == FrameType.MIN15:
        max_secs = 240
        n_bars = 16
    elif ft == FrameType.MIN30:
        max_secs = 480
        n_bars = 8
    elif ft == FrameType.MIN60:
        max_secs = 900
        n_bars = 4
    else:
        raise ValueError("invalid frametype: %s" % ft)

    start = 0
    left = len(secs)
    while left > 0:
        if left > max_secs:  # 一次15*240=3500根
            end = start + max_secs
            left -= max_secs
        else:
            end = start + left
            left = 0

        _sec_list = secs[start:end]
        bars = await instance.get_bars_batch(
            _sec_list, end_dt, n_bars, ft.value, include_unclosed=True
        )
        for code in list(bars.keys()):
            if not len(bars[code]):
                del bars[code]
                print("delete empty bar: ", code)
                continue
            if np.any(np.isnan(bars[code]["amount"])) or np.any(
                np.isnan(bars[code]["volume"])
            ):
                del bars[code]
                print("delete bar with nan amount/volume: ", code)
                continue
            _date_in_bar = bars[code]["frame"][0]
            if _date_in_bar.date() != dt:
                del bars[code]
                logger.info(
                    "delete bar with earlier date (bars:%s): %s, %s",
                    ft.value,
                    code,
                    _date_in_bar,
                )
                continue

            # print("sec added: ", code)
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

        _sec_list = secs[start:end]
        bars = await instance.get_bars_batch(
            _sec_list, end_dt, 1, "1d", include_unclosed=True
        )
        for code in list(bars.keys()):
            if not len(bars[code]):
                del bars[code]
                print("delete empty bar: ", code)
                continue
            if np.any(np.isnan(bars[code]["amount"])) or np.any(
                np.isnan(bars[code]["volume"])
            ):
                del bars[code]
                print("delete bar with nan amount/volume: ", code)
                continue
            _date_in_bar = bars[code]["frame"][0]
            if _date_in_bar != dt:
                del bars[code]
                logger.info(
                    "delete bar with earlier date (bars:1d): %s, %s", code, _date_in_bar
                )
                continue

            # print("sec added: ", code)
            all_valid_bars[code] = bars[code]

        start = end

    return all_valid_bars


async def get_sec_bars_pricelimits(secs_set: set, dt: datetime.date):
    secs = list(secs_set)

    instance = AbstractQuotesFetcher.get_instance()

    end = datetime.datetime.combine(dt, datetime.time(15, 0))

    bars = await instance.get_trade_price_limits(secs, end)
    bars = bars[~np.isnan(bars["low_limit"])]
    bars = bars[~np.isnan(bars["high_limit"])]
    bars = bars[bars["frame"] == dt]

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
            # print("sec added: ", code)
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

        _sec_list = secs[start:end]
        bars = await instance.get_bars_batch(
            _sec_list, end_dt, 1, "1w", include_unclosed=True
        )
        for code in list(bars.keys()):
            if not len(bars[code]):
                del bars[code]
                print("delete empty bar: ", code)
                continue
            if np.any(np.isnan(bars[code]["amount"])) or np.any(
                np.isnan(bars[code]["volume"])
            ):
                del bars[code]
                print("delete bar with nan amount/volume: ", code)
                continue
            _date_in_bar = bars[code]["frame"][0]
            if _date_in_bar < d0 or _date_in_bar > dt:
                del bars[code]
                logger.info(
                    "delete bar with invalid date (not in this week): %s, %s",
                    code,
                    _date_in_bar,
                )
                continue

            # print("sec added: ", code)
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

        _sec_list = secs[start:end]
        bars = await instance.get_bars_batch(
            _sec_list, end_dt, 1, "1M", include_unclosed=True
        )
        for code in list(bars.keys()):
            if not len(bars[code]):
                del bars[code]
                print("delete empty bar: ", code)
                continue
            if np.any(np.isnan(bars[code]["amount"])) or np.any(
                np.isnan(bars[code]["volume"])
            ):
                del bars[code]
                print("delete bar with nan amount/volume: ", code)
                continue
            _date_in_bar = bars[code]["frame"][0]
            if _date_in_bar < d0 or _date_in_bar > dt:
                del bars[code]
                logger.info(
                    "delete bar with invalid date (not in this month): %s, %s",
                    code,
                    _date_in_bar,
                )
                continue

            # print("sec added: ", code)
            all_valid_bars[code] = bars[code]

        start = end

    return all_valid_bars
