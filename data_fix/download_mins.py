import datetime
import logging

import cfg4py
from coretypes import FrameType, SecurityType, bars_dtype
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer
from omicron.models.security import Security
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame
from omicron.models.timeframe import TimeFrame as tf
from sqlalchemy import true

from datascan.jq_fetcher import get_sec_bars_min
from dfs_tools import write_bars_dfs
from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher
from influx_data.security_bars_1d import get_security_day_bars
from influx_data.security_bars_1m import get_security_minutes_data
from influx_tools import remove_sec_in_bars_min
from rebuild_minio.build_min_data import generate_minio_for_min

logger = logging.getLogger(__name__)


async def get_min_data_from_jq(
    all_secs_today,
    target_date: datetime.date,
    ft: FrameType,
    prefix: SecurityType,
):
    # download all data from jq
    all_secs_data = await get_sec_bars_min(all_secs_today, target_date, ft)
    logger.info(
        "total secs downloaded from bars:%s@jq, %d, %s",
        ft.value,
        len(all_secs_data),
        prefix,
    )

    if len(all_secs_data) == 0:
        logger.info("no valid price data from bars:%s@jq, %s", ft.value, target_date)
        return True

    await Stock.persist_bars(ft, all_secs_data)
    logger.info(
        "get from bars:%s@jq and saved into db, %d",
        ft.value,
        len(all_secs_data),
    )

    logger.info("finished processing bars:%s for %s, %s", ft.value, target_date, prefix)
    return True


async def redownload_bars_mins_for_target_day():
    target_day = datetime.date(2022, 7, 22)
    logger.info("fetchbars, mins, from jq: %s", target_day)

    all_stocks = {"688375.XSHG", "688231.XSHG"}
    all_indexes = set()

    for ft in (
        FrameType.MIN1,
        FrameType.MIN5,
        FrameType.MIN15,
        FrameType.MIN30,
        FrameType.MIN60,
    ):
        # 下载全部分钟线数据
        if len(all_stocks) > 0:
            rc = await get_min_data_from_jq(
                all_stocks, target_day, ft, SecurityType.STOCK
            )
            if not rc:
                logger.error(
                    "failed to process stock price data (%s): %s", ft, target_day
                )
                return False

        if len(all_indexes) > 0:
            rc = await get_min_data_from_jq(
                all_indexes, target_day, ft, SecurityType.INDEX
            )
            if not rc:
                logger.error(
                    "failed to process index price data (%s): %s", ft, target_day
                )
                return False

        rc = await generate_minio_for_min(target_day, ft)
        if not rc:
            logger.error(
                "failed to rebuild minio data for %s, %s", ft.value, target_day
            )

    return True
