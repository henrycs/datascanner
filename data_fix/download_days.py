import datetime
import logging

from download_bars.get_days import retrieve_bars_1d
from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher
from influx_data.security_list import get_security_list
from time_utils import split_securities

logger = logging.getLogger(__name__)


async def redownload_bars1d_for_target_day():
    target_day = datetime.date(2022, 11, 9)
    logger.info("fetchbars1d, from jq: %s", target_day)

    # 读取当天的证券列表
    all_secs = await get_security_list(target_day, "stock")
    if all_secs is None:
        logger.error("no security list in date %s", target_day)
        return False
    all_indexes = await get_security_list(target_day, "index")
    if all_indexes is None:
        logger.error("no security list in date %s", target_day)
        return False

    if len(all_secs) == 0 or len(all_indexes) == 0:
        logger.error("no stock or index list in date %s", target_day)
        return False

    rc = await retrieve_bars_1d(target_day, all_secs, all_indexes)
    if rc is False:
        logger.error("failed to get bars:1d for date %s", target_day)
        return False

    return True
