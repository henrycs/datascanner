import datetime
import logging

import numpy as np
from coretypes import FrameType
from omicron.dal.cache import cache
from omicron.dal.influx.flux import Flux
from omicron.dal.influx.influxclient import InfluxClient
from omicron.dal.influx.serialize import EPOCH, DataframeDeserializer
from omicron.models.security import Security
from omicron.models.stock import Stock
from omicron.models.timeframe import TimeFrame
from omicron.models.timeframe import TimeFrame as tf

from datascan.index_secs import get_index_sec_whitelist
from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher

logger = logging.getLogger(__name__)


def split_securities_by_type_nparray(all_secs_in_cache):
    all_secs = set()
    all_indexes = set()
    for sec in all_secs_in_cache:
        code = sec[0]
        _type = sec[5]
        if _type == "stock":
            all_secs.add(code)
        elif _type == "index":
            all_indexes.add(code)

    return all_secs, all_indexes


async def get_security_list_db(target_date: datetime.date, sec_type: str):
    all_secs_in_cache = await Security.select(target_date).types([sec_type]).eval()
    if all_secs_in_cache is None or len(all_secs_in_cache) < 10:
        logger.error("failed to query securities from db, %s", target_date)
        return None

    return set(all_secs_in_cache)


async def get_security_list_jq(target_date: datetime.date):
    fetcher = AbstractQuotesFetcher.get_instance()

    all_secs = await fetcher.get_security_list(target_date)
    if all_secs is None or len(all_secs) < 10:
        msg = "failed to get security list(%s)" % target_date.strftime("%Y-%m-%d")
        logger.error(msg)
        return None

    return all_secs


async def validate_security_list(target_date: datetime.date):
    # 从数据库中读取当天的证券列表
    all_stock_db = await get_security_list_db(target_date, "stock")
    if all_stock_db is None:
        return None, None
    all_index_db = await get_security_list_db(target_date, "index")
    if all_index_db is None:
        return None, None

    all_secs_in_jq = await get_security_list_jq(target_date)
    if all_secs_in_jq is None:
        return None, None
    all_stock_jq, all_index_jq = split_securities_by_type_nparray(all_secs_in_jq)

    # 检查所有股票
    delta = all_stock_db.difference(all_stock_jq)
    if len(delta) > 0:
        logger.error("%d security(stock) not in jq side, %s", len(delta), target_date)
        return None, None
    delta = all_stock_jq.difference(all_stock_db)
    if len(delta) > 0:
        logger.error("%d security(stock) not in local db, %s", len(delta), target_date)
        return None, None

    # 检查所有指数
    delta = all_index_db.difference(all_index_jq)
    if len(delta) > 0:
        logger.error("%d security(index) not in jq side, %s", len(delta), target_date)
        return None, None
    delta = all_index_jq.difference(all_index_db)
    if len(delta) > 0:
        logger.info("index in jq but not in db: %s", delta)
        if delta in get_index_sec_whitelist():
            logger.error(
                "%d important security(index) not in local db, %s",
                len(delta),
                target_date,
            )
            return None, None

    return all_stock_db, all_index_db
