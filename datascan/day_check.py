import datetime
import logging

from omicron.dal.cache import cache
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.serialize import DataframeDeserializer
from omicron.models import get_influx_client
from omicron.models.security import Security

from datascan.index_secs import get_index_sec_whitelist
from datascan.jq_fetcher import get_sec_bars_1d, get_sec_bars_pricelimits
from datascan.scanner_utils import (
    compare_bars_for_openclose,
    compare_bars_for_pricelimits,
    get_secs_from_bars,
)

logger = logging.getLogger(__name__)


async def get_security_day_bars(start: datetime.datetime, end: datetime.datetime):
    client = get_influx_client()
    measurement = "stock_bars_1d"

    flux = (
        Flux()
        .measurement(measurement)
        .range(start, end)
        .bucket(client._bucket)
        .fields(["open", "high", "low", "close", "volume", "factor"])
    )

    data = await client.query(flux)
    if len(data) == 2:  # \r\n
        return []

    ds = DataframeDeserializer(
        sort_values="_time",
        usecols=["_time", "code", "open", "high", "low", "close", "volume", "factor"],
        time_col="_time",
        engine="c",
    )
    actual = ds(data)
    secs = actual.to_records(index=False)
    return secs


async def get_security_price_limits(start: datetime.datetime, end: datetime.datetime):
    client = get_influx_client()
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


async def scan_bars_1d_for_seclist(target_date: datetime.date):
    start = datetime.datetime.combine(target_date, datetime.time(0, 0, 0))
    end = datetime.datetime.combine(target_date, datetime.time(23, 59, 59))

    all_secs_in_bars = await get_security_day_bars(start, end)
    if all_secs_in_bars is None or len(all_secs_in_bars) == 0:
        logger.error("no secs found in bars:1d/open, %s", target_date)
        return None

    return all_secs_in_bars


async def scan_bars_1d_pricelimits_for_seclist(target_date: datetime.date):
    start = datetime.datetime.combine(target_date, datetime.time(0, 0, 0))
    end = datetime.datetime.combine(target_date, datetime.time(23, 59, 59))

    all_secs_in_bars = await get_security_price_limits(start, end)
    if all_secs_in_bars is None or len(all_secs_in_bars) == 0:
        logger.error("no secs found in bars:1d/limits, %s", target_date)
        return None

    return all_secs_in_bars


def get_security_difference(secs_in_bars, all_stock, all_index):
    # 检查是否有多余的股票
    x1 = secs_in_bars.difference(all_stock)
    y1 = x1.difference(all_index)
    if len(y1) > 0:  # 本地数据库中多余的股票或指数
        logger.error("secs in bars:1d but not in jq : %d", len(y1))
        return False

    return True


async def validate_day_bars(target_date: datetime.date, all_stock, all_index):
    logger.info("check bars:1d for open/close: %s", target_date)
    all_db_secs_data1 = await scan_bars_1d_for_seclist(target_date)
    if all_db_secs_data1 is None or len(all_db_secs_data1) == 0:
        logger.error("failed to get sec list from db for bars:1d/open, %s", target_date)
        return False

    # 因为有停牌的现象，所以对比证券清单的时候，只能比较本地是否有多余的票
    secs_list_1 = get_secs_from_bars(all_db_secs_data1)
    rc = get_security_difference(secs_list_1, all_stock, all_index)
    if not rc:
        return False

    logger.info("check bars:1d for price limits: %s", target_date)
    all_db_secs_data2 = await scan_bars_1d_pricelimits_for_seclist(target_date)
    if all_db_secs_data2 is None or len(all_db_secs_data2) == 0:
        logger.error(
            "failed to get sec list from db for bars:1d/limits, %s", target_date
        )
        return False

    secs_list_2 = get_secs_from_bars(all_db_secs_data2)
    rc = get_security_difference(secs_list_2, all_stock, all_index)
    if not rc:
        return False

    # 对比详细数据
    index_secs_whitelist = get_index_sec_whitelist()  # 只对比特定的指数
    all_secs_in_jq = all_stock.union(all_index)
    all_jq_secs_data1 = await get_sec_bars_1d(all_secs_in_jq, target_date)
    logger.info(
        "total secs downloaded from bars:1d/open@jq, %d, %s",
        len(all_jq_secs_data1),
        target_date,
    )
    rc = compare_bars_for_openclose(
        secs_list_1,
        all_db_secs_data1,
        all_jq_secs_data1,
        all_index,
        index_secs_whitelist,
    )
    if not rc:
        logger.error("failed to compare sec data in db and jq, %s", target_date)
        return False

    all_jq_secs_data2 = await get_sec_bars_pricelimits(all_secs_in_jq, target_date)
    logger.info(
        "total secs downloaded from bars:1d/limits@jq, %d, %s",
        len(all_jq_secs_data2),
        target_date,
    )
    rc = compare_bars_for_pricelimits(
        secs_list_2,
        all_db_secs_data2,
        all_jq_secs_data2,
        all_index,
        index_secs_whitelist,
    )
    if not rc:
        logger.error(
            "failed to compare sec data (price limit) in db and jq, %s", target_date
        )
        return False

    return True


async def get_all_secs_in_bars1d_db(target_date: datetime.date):
    all_db_secs_data = await scan_bars_1d_for_seclist(target_date)
    if all_db_secs_data is None or len(all_db_secs_data) == 0:
        logger.error("failed to get sec list from db for bars:1d/open, %s", target_date)
        return None

    return get_secs_from_bars(all_db_secs_data)
