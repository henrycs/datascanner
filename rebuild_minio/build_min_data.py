import datetime
import logging
import os
import pickle

import arrow
import ciso8601
import numpy as np
from coretypes import FrameType, SecurityType
from omicron.dal.cache import cache
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer, NumpyDeserializer
from omicron.models.security import Security
from omicron.models.timeframe import TimeFrame

from datascan.scanner_utils import compare_sec_data_full
from dfs_tools import write_bars_dfs
from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher
from influx_data.security_list import get_security_list

logger = logging.getLogger(__name__)


def ciso8601_parse_date(x):
    return ciso8601.parse_datetime(x).date()


def ciso8601_parse_naive(x):
    return ciso8601.parse_datetime_as_naive(x)


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


def convert_data_format(tuple_arr):
    dtype = [
        ("frame", "O"),
        ("open", "f4"),
        ("high", "f4"),
        ("low", "f4"),
        ("close", "f4"),
        ("volume", "f8"),
        ("amount", "f8"),
        ("factor", "f4"),
    ]
    return np.array(tuple_arr, dtype=dtype)


def convert_data_format_for_line(sec_data):
    _time = sec_data["frame"].strftime("%Y-%m-%d %H:%M:%S")
    _dt = arrow.get(_time).naive
    v1 = sec_data["open"]
    v2 = sec_data["high"]
    v3 = sec_data["low"]
    v4 = sec_data["close"]
    v5 = sec_data["volume"]
    v6 = sec_data["amount"]
    v7 = sec_data["factor"]
    _arr = (_dt, v1, v2, v3, v4, v5, v6, v7)
    return _arr


async def get_security_minutes_bars(
    ft: FrameType, start: datetime.datetime, end: datetime.datetime
):
    client = Security.get_influx_client()
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
    dtype = [
        ("frame", "O"),
        ("code", "O"),
        ("open", "f4"),
        ("high", "f4"),
        ("low", "f4"),
        ("close", "f4"),
        ("volume", "f8"),
        ("amount", "f8"),
        ("factor", "f4"),
    ]

    flux = (
        Flux()
        .measurement(measurement)
        .range(start, end)
        .bucket(client._bucket)
        .fields(cols)
    )

    ds = NumpyDeserializer(
        dtype,
        use_cols=cols,
        converters={"_time": lambda x: ciso8601.parse_datetime(x)},
        parse_date=None,
    )
    result = await client.query(flux, ds)
    if result.size == 0:
        return None

    db_data_by_sec = {}
    for sec_data in result:
        code = sec_data["code"]
        _data = convert_data_format_for_line(sec_data)
        if code in db_data_by_sec:
            db_data_by_sec[code].append(_data)
        else:
            db_data_by_sec[code] = []
            db_data_by_sec[code].append(_data)

    new_data_by_sec = {}
    for code in db_data_by_sec.keys():
        _arr = db_data_by_sec[code]
        _np_arr = convert_data_format(_arr)
        new_data_by_sec[code] = _np_arr

    return new_data_by_sec


async def get_sec_minutes_data_db(ft: FrameType, target_date: datetime.date):
    _start = datetime.datetime.combine(target_date, datetime.time(9, 30, 0))
    _end = datetime.datetime.combine(target_date, datetime.time(15, 30, 1))
    secs = await get_security_minutes_bars(ft, _start, _end)
    return secs


async def generate_minio_for_min(target_date: datetime.date, ft: FrameType):
    # 从数据库中读取当天的证券列表
    all_stock_db = await get_security_list(target_date, "stock")
    all_index_db = await get_security_list(target_date, "index")
    logger.info("stock secs: %d, index secs: %d", len(all_stock_db), len(all_index_db))

    secs_data = await get_sec_minutes_data_db(ft, target_date)
    if secs_data is None or len(secs_data) == 0:
        logger.error("failed to get bars:%s for date %s", ft.value, target_date)
        return False

    all_stock_data = {}
    all_index_data = {}
    for code in secs_data.keys():
        if code in all_stock_db:
            all_stock_data[code] = secs_data[code]
        else:
            all_index_data[code] = secs_data[code]

    await write_bars_dfs(target_date, ft, all_stock_data, SecurityType.STOCK)
    logger.info(
        "finished processing stock bars:%s for %s (%s)",
        ft.value,
        target_date,
        SecurityType.STOCK,
    )

    await write_bars_dfs(target_date, ft, all_index_data, SecurityType.INDEX)
    logger.info(
        "finished processing index bars:%s for %s (%s)",
        ft.value,
        target_date,
        SecurityType.INDEX,
    )

    return True


def test_read_file():
    # file = "/home/henry/zillionare/rebuild_minio/20050223"  # 20220721
    file = "/home/henry/zillionare/rebuild_minio/20220711_new"
    with open(file, "rb") as f:
        binary1 = pickle.load(f)
        print("size: ", len(binary1))

    file = "/home/henry/zillionare/rebuild_minio/20220711"
    with open(file, "rb") as f:
        binary2 = pickle.load(f)
        print("size: ", len(binary2))

    for code in binary1:
        datalist1 = binary1[code]
        datalist2 = binary2[code]
        for i in range(0, 4):
            data1 = datalist1[i]
            data2 = datalist2[i]
            rc = compare_sec_data_full(data1, data2)
            if not rc:
                print("--------------------------------------")
                break

    print("success!")


def test_read_file_for_sec():
    file = "/home/henry/zillionare/20220719"
    with open(file, "rb") as f:
        binary1 = pickle.load(f)
        print("size: ", len(binary1))

    for code in binary1:
        if code == "601212.XSHG":
            print(binary1[code])

    print("success!")


async def rebuild_minio_for_min():
    # test_read_file_for_sec()
    # return

    # FrameType.MIN1,
    # FrameType.MIN5,
    # FrameType.MIN15,
    # FrameType.MIN30,

    ft = FrameType.MIN60
    file = "/home/henry/zillionare/rebuild_minio/days_60m.txt"
    with open(file, "r") as f:
        lines = f.readlines()
        for line in lines:
            if line is None or len(line) == 0:
                continue
            line = line.strip()
            if len(line) == 0:
                continue
            logger.info("start processing %s in %s", ft.value, line)

            target_date = arrow.get(line).naive.date()
            # target_date = datetime.date(2022, 7, 11)
            rc = await generate_minio_for_min(target_date, ft)
            if not rc:
                logger.error(
                    "failed to rebuild minio data for %s, %s", ft.value, target_date
                )
                break
            logger.info("finished processing %s of %s", ft.value, target_date)
            # break

    return True
