import datetime
import logging

import cfg4py
from coretypes import FrameType, bars_dtype
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer
from omicron.models import get_influx_client
from omicron.models.security import Security
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame
from omicron.models.timeframe import TimeFrame as tf

logger = logging.getLogger(__name__)


async def get_security_minutes_data(ft: FrameType, target_date: datetime.date):
    delta = datetime.timedelta(minutes=1)
    if ft == FrameType.MIN1:
        delta = datetime.timedelta(minutes=1)
    elif ft == FrameType.MIN5:
        delta = datetime.timedelta(minutes=5)
    elif ft == FrameType.MIN30:
        delta = datetime.timedelta(minutes=30)
    elif ft == FrameType.MIN60:
        delta = datetime.timedelta(minutes=60)
    else:
        raise ValueError("FrameType not supported, %s" % ft)

    _start = datetime.datetime.combine(target_date, datetime.time(9, 30, 0))
    _end = datetime.datetime.combine(target_date, datetime.time(9, 30, 1))
    secs1 = await get_security_minutes_bars(ft, _start + delta, _end + delta)
    len1 = len(secs1)

    _start = datetime.datetime.combine(target_date, datetime.time(11, 30, 0))
    _end = datetime.datetime.combine(target_date, datetime.time(11, 30, 1))
    secs2 = await get_security_minutes_bars(ft, _start, _end)
    len2 = len(secs2)
    if len1 != len2:
        logger.error(
            "bars:%s data corrupt, 9:30+%s:00 != 11:30:00, %d, %d",
            ft.value,
            ft.value,
            len1,
            len2,
        )
        return None

    _start = datetime.datetime.combine(target_date, datetime.time(13, 0, 0))
    _end = datetime.datetime.combine(target_date, datetime.time(13, 0, 1))
    secs3 = await get_security_minutes_bars(ft, _start + delta, _end + delta)
    len3 = len(secs3)
    if len2 != len3:
        logger.error(
            "bars:%s data corrupt, 11:30:00 != 13:01:00, %d, %d", ft.value, len2, len3
        )
        return None

    _start = datetime.datetime.combine(target_date, datetime.time(15, 0, 0))
    _end = datetime.datetime.combine(target_date, datetime.time(15, 0, 1))
    secs4 = await get_security_minutes_bars(ft, _start, _end)
    len4 = len(secs4)
    if len3 != len4:
        logger.info(
            "bars:%s data corrupt, 13:00+%s:00 != 15:00:00, %d, %d",
            ft.value,
            ft.value,
            len3,
            len4,
        )
        return None

    return secs1


async def get_security_minutes_bars(
    ft: FrameType, start: datetime.datetime, end: datetime.datetime
):
    client = get_influx_client()
    measurement = "stock_bars_%s" % ft.value

    flux = (
        Flux()
        .measurement(measurement)
        .range(start, end)
        .bucket(client._bucket)
        .fields(["open", "close"])
    )

    data = await client.query(flux)
    if len(data) == 2:  # \r\n
        return []

    ds = DataframeDeserializer(
        sort_values="_time",
        usecols=["_time", "code", "open", "close"],
        time_col="_time",
        engine="c",
    )
    actual = ds(data)
    secs = actual.to_records(index=False)
    return secs
