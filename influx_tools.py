import datetime
import logging
from coretypes import FrameType, bars_dtype
import cfg4py
from omicron.models.timeframe import TimeFrame
from omicron.models.security import Security
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer
from omicron.models.timeframe import TimeFrame as tf


logger = logging.getLogger(__name__)


async def remove_security_list():
    client = Security.get_influx_client()
    measurement = "security_list"
    year = 2007
    while year < 2024:
        print("deleteing in security_list ", year)
        await client.delete(measurement, datetime.datetime(year,1,1))
        print("data deleted in security_list: ", year)
        year += 1


async def drop_bars_1d():
    client = Security.get_influx_client()
    measurement = "stock_bars_1d"  # day
    year = 2005
    while year < 2007:
        print("deleteing in bars:1d ", year)
        await client.delete(measurement, datetime.datetime(year,1,1))
        print("data deleted in bars:1d, ", year)
        year += 1
    
    print("drop_bars_1d: all finished.")


async def drop_bars_1w():
    client = Security.get_influx_client()
    measurement = "stock_bars_1w"  # week
    year = 2005
    while year < 2007:
        print("deleteing in bars:1w ", year)
        await client.delete(measurement, datetime.datetime(year,1,1))
        print("data deleted in bars:1w, ", year)
        year += 1
    
    print("drop_bars_1w: all finished.")


async def drop_bars_1M():
    client = Security.get_influx_client()
    measurement = "stock_bars_1M"  # month
    year = 2005
    while year < 2007:
        print("deleteing in bars:1M ", year)
        await client.delete(measurement, datetime.datetime(year,1,1))
        print("data deleted in bars:1M, ", year)
        year += 1
    
    print("drop_bars_1M: all finished.")


async def drop_bars_via_scope(target_year, ft: FrameType):
    if ft == FrameType.DAY:
        measurement = "stock_bars_1d"
    elif ft == FrameType.WEEK:
        measurement = "stock_bars_1w"
    elif ft == FrameType.MONTH:
        measurement = "stock_bars_1M"
    else:
        return False

    client = Security.get_influx_client()    
    start = datetime.datetime(target_year, 1, 1)
    end = datetime.datetime(target_year, 12, 31, 23, 59, 59)
    start_str = f"{start.isoformat(timespec='seconds')}Z"

    print("deleteing in ", measurement, target_year)
    await client.delete(measurement, stop=end, start=start_str)
    print("data deleted in ", measurement, target_year)
    
    print("drop ", measurement, " all finished.")


async def remove_sec_in_bars1d(code: str, target_date: datetime.date):
    # 删除日线内所有数据
    start = datetime.datetime.combine(target_date, datetime.time(0, 0, 0))
    end = datetime.datetime.combine(target_date, datetime.time(23, 59, 59))
    start_str = f"{start.isoformat(timespec='seconds')}Z"

    client = Security.get_influx_client()
    measurement = "stock_bars_1d"
    await client.delete(measurement, stop=end, start=start_str, tags={"code": code})


async def remove_sec_in_bars1m(code: str, target_date: datetime.date):
    # 删除分钟线内所有数据
    start = datetime.datetime.combine(target_date, datetime.time(0, 0, 0))
    end = datetime.datetime.combine(target_date, datetime.time(23, 59, 59))
    start_str = f"{start.isoformat(timespec='seconds')}Z"

    client = Security.get_influx_client()
    measurement = "stock_bars_1m"
    await client.delete(measurement, stop=end, start=start_str, tags={"code": code})
