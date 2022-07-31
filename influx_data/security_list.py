import datetime
import logging

import cfg4py
from coretypes import FrameType, bars_dtype
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer
from omicron.models.security import Security
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame
from omicron.models.timeframe import TimeFrame as tf

logger = logging.getLogger(__name__)


async def get_security_list(target_date: datetime.date, sec_type: str):
    all_secs_in_cache = await Security.select(target_date).types([sec_type]).eval()
    if all_secs_in_cache is None or len(all_secs_in_cache) < 100:
        print("failed to query securities from db")
        return None

    return set(all_secs_in_cache)
