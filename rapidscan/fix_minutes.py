import datetime
import logging
from coretypes import FrameType, bars_dtype
import cfg4py
from omicron.models.timeframe import TimeFrame
from coretypes import FrameType, SecurityType
from omicron.models.security import Security
from omicron.models.stock import Stock
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer
from omicron.models.timeframe import TimeFrame as tf
from dfs_tools import write_bars_dfs
from influx_data.security_bars_1d import get_security_day_bars
from influx_data.security_bars_1m import get_security_minutes_data
from influx_tools import remove_sec_in_bars1m
from jq_fetcher import get_sec_bars_min


logger = logging.getLogger(__name__)


async def get_min_for_price(all_secs_today, target_date: datetime.date, ft: FrameType):
    # download all data from jq
    all_secs_data = await get_sec_bars_min(all_secs_today, target_date)
    logger.info("total secs downloaded from bars:1m@jq, %d", len(all_secs_data))

    if len(all_secs_data) == 0:
        logger.info("no valid price data from bars:1m@jq, %s", target_date)
        return True

    await Stock.persist_bars(FrameType.MIN1, all_secs_data)
    logger.info("get from bars:1m@jq and saved into db, %s, %d", FrameType.MIN1, len(all_secs_data))

    logger.info("finished processing bars:1m for %s", target_date)
    return True


async def scan_bars_min_for_seclist(target_date: datetime.date, ft: FrameType):
    all_secs_in_bars = await get_security_minutes_data(ft, target_date)
    if all_secs_in_bars is None:
        return None

    secs_in_bars = set()
    for sec in all_secs_in_bars:
        code = sec[1]
        secs_in_bars.add(code)
    
    return secs_in_bars


async def validate_bars_min(target_day, secs_in_bars1d, ft: FrameType):
    # 获取所有分钟线的证券清单
    secs_in_bars = await scan_bars_min_for_seclist(target_day, ft)
    if secs_in_bars is None:
        return False

    # 检查是否有多余的股票
    x1 = secs_in_bars.difference(secs_in_bars1d)
    if len(x1) > 0:  # 需要删除
        for sec in x1:            
            logger.info("bars:%s, to be removed: %s", ft.value, sec)
            #await remove_sec_in_bars1m(sec, target_day)
        logger.info("RebuildMin1, bars:%s, secs to be removed: %d, %s", ft.value, len(x1), target_day)
    
    # 需要增补的股票
    to_be_added = secs_in_bars1d.difference(secs_in_bars)    
    if len(to_be_added) > 0:
        for sec in to_be_added:
            logger.info("bars:%s, to be added: %s", ft.value, sec)
        logger.info("RebuildMin1, bars:%s, secs to be added: %d, %s", ft.value, len(to_be_added), target_day)
    else:
        return True

    #rc = await get_min_for_price(to_be_added, target_day, ft)
    rc = True
    if not rc:
        logger.error("failed to process stock price data (%s): %s", ft, target_day)
        return False

    return True