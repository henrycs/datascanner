import datetime
import logging
import pickle

import cfg4py
from coretypes import FrameType, SecurityType
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer
from omicron.models.security import Security
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame
from omicron.models.timeframe import TimeFrame as tf

from datascan.jq_fetcher import get_sec_bars_1d, get_sec_bars_pricelimits
from dfs import Storage
from dfs_tools import get_trade_limit_filename, write_bars_dfs, write_price_limits_dfs
from influx_data.security_bars_1d import (
    get_security_day_bars,
    get_security_price_limits,
)

logger = logging.getLogger(__name__)


async def get_1d_for_price(
    all_secs_today, target_date: datetime.date, prefix: SecurityType
):
    # download all data from jq
    all_secs_data = await get_sec_bars_1d(all_secs_today, target_date)
    logger.info(
        "total secs downloaded from bars:1d@jq, %d, %s", len(all_secs_data), prefix
    )

    if len(all_secs_data) == 0:
        logger.error(
            "failed to get price data from bars:1d@jq, %s (%s)", target_date, prefix
        )
        return False

    await Stock.persist_bars(FrameType.DAY, all_secs_data)
    logger.info(
        "get from bars:1d@jq and saved into db, %s, %s, %d",
        prefix,
        FrameType.DAY,
        len(all_secs_data),
    )

    await write_bars_dfs(target_date, FrameType.DAY, all_secs_data, prefix)

    logger.info("finished processing bars:1d for %s (%s)", target_date, prefix)
    return True


async def get_1d_for_pricelimits(
    all_secs_today, target_date: datetime.date, prefix: SecurityType
):
    all_secs_data = await get_sec_bars_pricelimits(all_secs_today, target_date)
    logger.info(
        "total secs downloaded from bars:1d:limits@jq, %d, %s",
        len(all_secs_data),
        prefix,
    )

    if len(all_secs_data) == 0:
        logger.error(
            "failed to get price limits data from jq, %s (%s)", target_date, prefix
        )
        return False

    await Stock.save_trade_price_limits(all_secs_data, to_cache=False)
    logger.info(
        "get from bars:1d:limits@jq and saved into db, %s, %d",
        FrameType.DAY,
        len(all_secs_data),
    )

    await write_price_limits_dfs(target_date, all_secs_data, prefix)

    logger.info("finished processing bars:1d:high_limit, %s (%s)", target_date, prefix)
    return True


async def retrieve_bars_1d(target_day, all_secs, all_indexes):
    rc = await get_1d_for_price(all_secs, target_day, SecurityType.STOCK)
    if not rc:
        logger.error("failed to process stock price data: %s", target_day)
        return False

    rc = await get_1d_for_price(all_indexes, target_day, SecurityType.INDEX)
    if not rc:
        logger.error("failed to process index price data: %s", target_day)
        return False

    rc = await get_1d_for_pricelimits(all_secs, target_day, SecurityType.STOCK)
    if not rc:
        logger.error("failed to process stock price limits data: %s", target_day)
        return False

    rc = await get_1d_for_pricelimits(all_indexes, target_day, SecurityType.INDEX)
    if not rc:
        logger.error("failed to process index price limits data: %s", target_day)
        return False
