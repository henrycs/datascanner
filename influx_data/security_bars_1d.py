import datetime
import logging
from coretypes import FrameType, bars_dtype
import cfg4py
from omicron.models.timeframe import TimeFrame
from omicron.models.security import Security
from omicron.models.stock import Stock
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer
from omicron.models.timeframe import TimeFrame as tf


logger = logging.getLogger(__name__)


async def get_security_day_bars(start: datetime.datetime, end: datetime.datetime):
    client = Security.get_influx_client()
    measurement = "stock_bars_1d"

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


async def get_security_price_limits(start: datetime.datetime, end: datetime.datetime):
    client = Security.get_influx_client()
    measurement = "stock_bars_1d"

    flux = (
        Flux()
        .measurement(measurement)
        .range(start, end)
        .bucket(client._bucket)
        .fields(["high_limit", "low_limit"])
    )

    data = await client.query(flux)
    if len(data) == 2:  # \r\n
        return []

    ds = DataframeDeserializer(
        sort_values="_time",
        usecols=["_time", "code", "high_limit", "low_limit"],
        time_col="_time",
        engine="c",
    )
    actual = ds(data)
    secs = actual.to_records(index=False)
    return secs