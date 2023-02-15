import datetime
import logging
import os
import pickle

import numpy as np
from coretypes import FrameType, SecurityType
from omicron.dal.cache import cache
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer, NumpyDeserializer
from omicron.models import get_influx_client
from omicron.models.security import Security
from omicron.models.timeframe import TimeFrame

from datascan.index_secs import get_index_sec_whitelist
from influx_data.security_list import get_security_list

logger = logging.getLogger(__name__)


def split_securities_by_type(all_secs_in_cache):
    all_secs = set()
    all_indexes = set()
    for sec in all_secs_in_cache:
        code = sec["code"]
        _type = sec["type"]
        if _type == "stock":
            all_secs.add(code)
        elif _type == "index":
            all_indexes.add(code)

    return all_secs, all_indexes


dtype_bars_min = [
    ("frame", "datetime64[s]"),
    ("code", "i4"),
    ("open", "f4"),
    ("high", "f4"),
    ("low", "f4"),
    ("close", "f4"),
    ("volume", "f8"),
    ("amount", "f8"),
    ("factor", "f4"),
]


dtype_bars_day = [
    ("frame", "datetime64[s]"),
    ("code", "i4"),
    ("open", "f4"),
    ("close", "f4"),
    ("high", "f4"),
    ("low", "f4"),
    ("high_limit", "f4"),
    ("low_limit", "f4"),
    ("volume", "f8"),
    ("amount", "f8"),
    ("factor", "f4"),
]


dtype_sec_list = [
    ("_time", "datetime64[s]"),
    ("code", "i4"),
    ("info", "O"),
]


def compact_sec_name(x):
    items = x.split(".")
    if items[1] == "XSHE":
        return int("1" + items[0])
    else:
        return int("2" + items[0])


def compact_board_name(x):
    items = x.split(".")
    return int(items[0])


def filter_secs(code_c: int, all_index_db, index_white_list):
    if code_c > 2000000:
        code = f"{code_c}.XSHG"
    else:
        code = f"{code_c}.XSHE"
    code = code[1:]

    if code in all_index_db:
        if code in index_white_list:
            return True
    else:
        return True

    return False


async def get_security_day_bars(start: datetime.datetime, end: datetime.datetime):
    client = get_influx_client()
    measurement = "stock_bars_1d"

    cols = [
        "_time",
        "code",
        "open",
        "close",
        "high",
        "low",
        "high_limit",
        "low_limit",
        "volume",
        "amount",
        "factor",
    ]

    flux = (
        Flux()
        .measurement(measurement)
        .range(start, end)
        .bucket(client._bucket)
        .fields(cols)
    )
    # _time,code,amount,close,factor,high,high_limit,low,low_limit,open,volume

    ds = NumpyDeserializer(
        dtype_bars_day,
        use_cols=cols,
        converters={"code": lambda x: compact_sec_name(x)},
        parse_date=None,
    )
    result = await client.query(flux, ds)
    if result.size == 0:
        return None

    return result


async def get_security_other_bars(
    ft: FrameType, start: datetime.datetime, end: datetime.datetime
):
    client = get_influx_client()
    measurement = "stock_bars_%s" % ft.value

    cols = [
        "_time",
        "code",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "amount",
        "factor",
    ]

    flux = (
        Flux()
        .measurement(measurement)
        .range(start, end)
        .bucket(client._bucket)
        .fields(cols)
    )
    # _time,code,amount,close,factor,high,high_limit,low,low_limit,open,volume

    ds = NumpyDeserializer(
        dtype_bars_min,
        use_cols=cols,
        converters={"code": lambda x: compact_sec_name(x)},
        parse_date=None,
    )
    result = await client.query(flux, ds)
    if result.size == 0:
        return None

    return result


async def get_security_minutes_bars(
    ft: FrameType, start: datetime.datetime, end: datetime.datetime
):
    client = get_influx_client()
    measurement = "stock_bars_%s" % ft.value

    cols = [
        "_time",
        "code",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "factor",
    ]
    flux = (
        Flux()
        .measurement(measurement)
        .range(start, end)
        .bucket(client._bucket)
        .fields(cols)
    )

    ds = NumpyDeserializer(
        dtype_bars_min,
        use_cols=cols,
        converters={"code": lambda x: compact_sec_name(x)},
        parse_date=None,
    )
    result = await client.query(flux, ds)
    if result.size == 0:
        return None

    return result


async def get_sec_list(start: datetime.datetime, end: datetime.datetime):
    client = get_influx_client()
    measurement = "security_list"

    flux = (
        Flux()
        .measurement(measurement)
        .range(start, end)
        .bucket(client._bucket)
        .fields(["info"])
    )

    data = await client.query(flux)
    if len(data) == 2:  # \r\n
        return None

    ds = DataframeDeserializer(
        sort_values="_time",
        usecols=["_time", "code", "info"],
        time_col="_time",
        engine="c",
    )

    converted_list = []
    secs = ds(data).to_records(index=False)
    secs = secs.astype(
        [
            ("_time", "datetime64[s]"),
            ("code", "O"),
            ("info", "O"),
        ]
    )
    if len(secs) != 0:
        # "_time", "code", "code, alias, name, start, end, type"
        for x in secs:
            code_c = compact_sec_name(x["code"])
            converted_list.append((x["_time"], code_c, x["info"]))
    else:
        return None

    return converted_list


async def get_xrxd_list(start: datetime.datetime, end: datetime.datetime):
    client = get_influx_client()
    measurement = "security_xrxd_reports"

    flux = (
        Flux()
        .measurement(measurement)
        .range(start, end)
        .bucket(client._bucket)
        .fields(["_time", "code", "info"])
    )

    data = await client.query(flux)
    if len(data) == 2:  # \r\n
        return None

    ds = DataframeDeserializer(
        sort_values="_time",
        usecols=["_time", "code", "info"],
        time_col="_time",
        engine="c",
    )

    converted_list = []
    secs = ds(data).to_records(index=False)
    secs = secs.astype(
        [
            ("_time", "datetime64[s]"),
            ("code", "O"),
            ("info", "O"),
        ]
    )
    if len(secs) != 0:
        for x in secs:
            code_c = compact_sec_name(x["code"])
            converted_list.append((x["_time"], code_c, x["info"]))
    else:
        return None

    return converted_list


async def get_board_bars_1d(start: datetime.datetime, end: datetime.datetime):
    client = get_influx_client()
    measurement = "board_bars_1d"

    cols = [
        "_time",
        "code",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "factor",
    ]
    flux = (
        Flux()
        .measurement(measurement)
        .range(start, end)
        .bucket(client._bucket)
        .fields(cols)
    )

    ds = NumpyDeserializer(
        dtype_bars_min,
        use_cols=cols,
        converters={"code": lambda x: compact_board_name(x)},
        parse_date=None,
    )
    result = await client.query(flux, ds)
    if result.size == 0:
        return None

    return result


async def get_sec_bars_data(ft: FrameType, target_date: datetime.date):
    if ft == FrameType.MIN30:
        _start = datetime.datetime.combine(target_date, datetime.time(9, 30, 0))
        _end = datetime.datetime.combine(target_date, datetime.time(15, 30, 1))

        secs = await get_security_minutes_bars(ft, _start, _end)
    elif ft == FrameType.DAY:
        _start = target_date
        _end = target_date
        secs = await get_security_day_bars(_start, _end)
    else:
        _start = target_date
        _end = target_date
        secs = await get_security_other_bars(ft, _start, _end)

    return secs


async def generate_pickle_data(ft: FrameType, target_date: datetime.date):
    # 从数据库中读取当天的证券列表
    all_index_db = await get_security_list(target_date, "index")
    logger.info("index secs in %s: %d", target_date, len(all_index_db))

    # 读取指数白名单，只存储名单中的指数
    index_white_list = get_index_sec_whitelist()

    secs_data = await get_sec_bars_data(ft, target_date)
    if secs_data is None or len(secs_data) == 0:
        logger.error("failed to get bars:%s for date %s", ft.value, target_date)
        os._exit(1)

    filter_items = []
    for sec in secs_data:
        code_c = sec["code"].item()
        rc = filter_secs(code_c, all_index_db, index_white_list)
        if rc:
            filter_items.append(sec)

    logger.info("secs data downloaded: %s, %d", target_date, len(filter_items))
    return filter_items


async def pack_bars():
    # FrameType.MIN30, DAY, WEEK, MONTH
    ft = FrameType.MONTH
    file = "/home/app/zillionare/pack_data/bars_1M_2022.pik"

    all_bars_items = []
    # 2022.1.4 Tuesday
    # 2023.1.3 Tuesday
    dt_start = datetime.date(2022, 1, 1)
    dt_end = datetime.date(2022, 12, 31)

    if ft == FrameType.MIN30:
        ft_step = FrameType.DAY
    else:
        ft_step = ft

    if TimeFrame.shift(dt_start, 0, ft_step) != dt_start:
        dt = TimeFrame.shift(dt_start, 1, ft_step)
    else:
        dt = dt_start

    while dt <= dt_end:
        secs_data = await generate_pickle_data(ft, dt)
        if secs_data is None or len(secs_data) == 0:
            logger.info("pack_bars, failed to get bars from database")
            os._exit(1)

        all_bars_items.extend(secs_data)
        dt = TimeFrame.shift(dt, 1, ft_step)

    logger.info("total records: %d", len(all_bars_items))

    if ft == FrameType.MIN30:
        np_bars_data = np.array(all_bars_items, dtype_bars_min)
    elif ft == FrameType.DAY:
        np_bars_data = np.array(all_bars_items, dtype_bars_day)
    else:
        np_bars_data = np.array(all_bars_items, dtype_bars_min)

    binary = pickle.dumps(np_bars_data, protocol=4)
    with open(file, "wb") as f:
        f.write(binary)

    logger.info("download finished")


async def pack_sec_list():
    file = "/home/app/zillionare/pack_data/seclist_2023.pik"

    dt_start = datetime.date(2023, 1, 1)
    dt_end = datetime.date(2023, 2, 10)

    secs_data = await get_sec_list(dt_start, dt_end)
    if secs_data is None or len(secs_data) == 0:
        logger.info("pack_sec_list, failed to get sec list from database")
        os._exit(1)
    logger.info("secs list downloaded: %s - %s, %d", dt_start, dt_end, len(secs_data))

    np_bars_data = np.array(secs_data, dtype_sec_list)
    binary = pickle.dumps(np_bars_data, protocol=4)
    with open(file, "wb") as f:
        f.write(binary)

    logger.info("security list download finished")


async def pack_xrxd_list():
    file = "/home/app/zillionare/pack_data/sec_xrxd_2023.pik"

    dt_start = datetime.date(2023, 1, 1)
    dt_end = datetime.date(2023, 12, 31)

    secs_data = await get_xrxd_list(dt_start, dt_end)
    if secs_data is None or len(secs_data) == 0:
        logger.info("pack_xrxd_list, failed to get sec xrxd from database")
        os._exit(1)
    logger.info(
        "secs xrxd reports downloaded: %s - %s, %d", dt_start, dt_end, len(secs_data)
    )

    np_bars_data = np.array(secs_data, dtype_sec_list)
    binary = pickle.dumps(np_bars_data, protocol=4)
    with open(file, "wb") as f:
        f.write(binary)

    logger.info("security xrxd list download finished")


async def pack_seclist_from_cache():
    secs = await cache.security.lrange("security:all", 0, -1)
    if len(secs) < 4000:
        logger.error("cannot read security list from cache!")
        os._exit(1)

    logger.info("secs in redis: %d", len(secs))

    file = "/home/app/zillionare/pack_data/redis_seclist.pik"
    binary = pickle.dumps(secs, protocol=4)
    with open(file, "wb") as f:
        f.write(binary)


async def _pack_calendar_data(category: str):
    secs = await cache.security.lrange(f"calendar:{category}", 0, -1)
    if len(secs) < 10:
        logger.error("cannot read calendar from cache!")
        os._exit(1)

    logger.info("calendar:%s in redis: %d", category, len(secs))

    file = f"/home/app/zillionare/pack_data/redis_calendar_{category}.pik"
    binary = pickle.dumps(secs, protocol=4)
    with open(file, "wb") as f:
        f.write(binary)


async def pack_calendar_from_cache():
    # 1) "calendar:1Y"
    # 2) "calendar:1Q"
    # 3) "calendar:1M"
    # 4) "calendar:1w"
    # 5) "calendar:1d"
    await _pack_calendar_data("1Y")
    await _pack_calendar_data("1Q")
    await _pack_calendar_data("1M")
    await _pack_calendar_data("1w")
    await _pack_calendar_data("1d")


async def pack_board_bars():
    file = "/home/app/zillionare/pack_data/board_2023.pik"

    dt_start = datetime.date(2023, 1, 1)
    dt_end = datetime.date(2023, 12, 31)

    secs_data = await get_board_bars_1d(dt_start, dt_end)
    if secs_data is None or len(secs_data) == 0:
        logger.info("pack_board_bars, failed to get board bars from database")
        os._exit(1)
    logger.info("board bars downloaded: %s - %s, %d", dt_start, dt_end, len(secs_data))

    np_bars_data = np.array(secs_data, dtype_bars_min)
    binary = pickle.dumps(np_bars_data, protocol=4)
    with open(file, "wb") as f:
        f.write(binary)

    logger.info("board bars download finished")


async def pack_data_from_db():
    # FrameType.MIN30, DAY, WEEK, MONTH

    # 30m, 1d, 1w, 1m, including price limits
    # await pack_bars()

    # security_list: code, info
    # await pack_sec_list()

    # xrxd info, from 2022.1.1 to now
    # await pack_xrxd_list()

    # boards, all data
    # board_bars_1d, 300008.THS
    # await pack_board_bars()

    # security list in cache
    # await pack_seclist_from_cache()

    # calendar in cache
    await pack_calendar_from_cache()

    logger.info("dump finished.")
    return True
