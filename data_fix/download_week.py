import datetime
import logging

from coretypes import FrameType, SecurityType
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame

from datascan.jq_fetcher import get_sec_bars_1w
from dfs_tools import write_bars_dfs
from influx_data.security_list import get_security_list
from time_utils import split_securities

logger = logging.getLogger(__name__)


async def get_1w_for_price(
    all_secs_today, target_date: datetime.date, d0: datetime.date, prefix: SecurityType
):
    # d0: 本周第一天, target_date：本周最后一天

    # download all data from jq
    all_secs_data = await get_sec_bars_1w(all_secs_today, target_date, d0)
    logger.info(
        "total secs downloaded from bars:1w@jq, %d, %s", len(all_secs_data), prefix
    )

    if len(all_secs_data) == 0:
        logger.error(
            "failed to get price data from bars:1w@jq, %s (%s)", target_date, prefix
        )
        return False

    await Stock.persist_bars(FrameType.WEEK, all_secs_data)
    logger.info(
        "get from bars:1w@jq and saved into db, %s, %s, %d",
        prefix,
        FrameType.WEEK,
        len(all_secs_data),
    )

    await write_bars_dfs(target_date, FrameType.WEEK, all_secs_data, prefix)

    logger.info("finished processing bars:1w for %s (%s)", target_date, prefix)
    return True


async def redownload_bars1w_for_target_day():
    target_day = datetime.date(2022, 7, 22)
    logger.info("fetchbars1w, from jq: %s", target_day)

    w0 = TimeFrame.week_shift(target_day, -1)
    w1d0 = TimeFrame.day_shift(w0, 1)
    logger.info("first and last day of week: %s, %s", w1d0, target_day)

    all_secs_in_w1d0 = await get_security_list(w1d0)
    if all_secs_in_w1d0 is None:
        logger.error("no security list in date %s", w1d0)
        return False
    all_secs_0, all_indexes_0 = split_securities(all_secs_in_w1d0)
    if (all_secs_0 is None or len(all_secs_0) == 0) or (
        all_indexes_0 is None or len(all_indexes_0) == 0
    ):
        logger.error("no security list (stock or index) in date %s", w1d0)
        return False

    all_secs_in_w1d1 = await get_security_list(target_day)
    if all_secs_in_w1d1 is None:
        logger.error("no security list in date %s", target_day)
        return False
    all_secs_1, all_indexes_1 = split_securities(all_secs_in_w1d1)
    if (all_secs_1 is None or len(all_secs_1) == 0) or (
        all_indexes_1 is None or len(all_indexes_1) == 0
    ):
        logger.error("no security list (stock or index) in date %s", w1d0)
        return False

    # 合并本周第一天和最后一天的证券列表（不用判断两天是否为同一天）
    all_stock = all_secs_0.union(all_secs_1)
    all_index = all_indexes_0.union(all_indexes_1)

    rc = await get_1w_for_price(all_stock, target_day, w1d0, SecurityType.STOCK)
    if not rc:
        logger.error("failed to process stock price data (week): %s", target_day)
        return False

    rc = await get_1w_for_price(all_index, target_day, w1d0, SecurityType.INDEX)
    if not rc:
        logger.error("failed to process index price data (week): %s", target_day)
        return False

    return True
