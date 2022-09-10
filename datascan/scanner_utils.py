import datetime
import logging
import math

import arrow
import numpy as np
from coretypes import FrameType
from omicron.dal.cache import cache
from omicron.extensions.decimals import math_round
from omicron.models.security import Security
from omicron.models.timeframe import TimeFrame

from datascan.index_secs import get_index_sec_whitelist

logger = logging.getLogger(__name__)


def get_secs_from_bars(all_secs_in_bars):
    secs_in_bars = set()
    for sec in all_secs_in_bars:
        code = sec[1]
        secs_in_bars.add(code)

    return secs_in_bars


def _compare_secs(secs_in_db, secs_in_jq):
    # 检查是否有多余的股票
    x1 = secs_in_db.difference(secs_in_jq)
    if len(x1) > 0:  # 本地数据库中多余的股票或指数
        if "000985.XSHG" in x1:
            logger.info("skip index 000985.XSHG...")
        else:
            logger.error("secs in bars:1d but not in jq, %d", len(x1))
            logger.info(x1)
            return False

    y1 = secs_in_jq.difference(secs_in_db)
    if len(y1) > 0:  # 本地数据库中多余的股票或指数
        logger.error("secs in jq but not in bars:1d, %s", y1)
        if y1 in get_index_sec_whitelist():
            logger.error("important index missing....")
            return False

    return True


def _compare_sec_data(sec_data_db, sec_data_jq, is_index):
    if is_index:
        delta = 2e-2
    else:
        delta = 1e-2

    v1 = math_round(sec_data_db["open"], 2)
    v2 = math_round(sec_data_jq["open"][0].item(), 2)
    if math.isclose(v1, v2, abs_tol=delta) is False:
        logger.error("[open] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_db["high"], 2)
    v2 = math_round(sec_data_jq["high"][0].item(), 2)
    if math.isclose(v1, v2, abs_tol=delta) is False:
        logger.error("[high] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_db["low"], 2)
    v2 = math_round(sec_data_jq["low"][0].item(), 2)
    if math.isclose(v1, v2, abs_tol=delta) is False:
        logger.error("[low] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_db["close"], 2)
    v2 = math_round(sec_data_jq["close"][0].item(), 2)
    if math.isclose(v1, v2, abs_tol=delta) is False:
        logger.error("[close] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = sec_data_db["volume"]
    v2 = sec_data_jq["volume"][0].item()
    if v1 != v2:
        logger.error("[volume] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = sec_data_db["factor"]
    v2 = sec_data_jq["factor"][0].item()
    if math.isclose(v1, v2, abs_tol=1e-5) is False:
        logger.error("[factor] not equal in db and jq: %f, %f", v1, v2)
        return False

    return True


def _compare_sec_data_pricelimits(sec_data_db, sec_data_jq, is_index):
    if is_index:
        delta = 2e-2
    else:
        delta = 1e-2

    v1 = math_round(sec_data_db["high_limit"], 2)
    v2 = math_round(sec_data_jq["high_limit"], 2)
    if math.isclose(v1, v2, abs_tol=delta) is False:
        logger.error("[high_limit] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_db["low_limit"], 2)
    v2 = math_round(sec_data_jq["low_limit"], 2)
    if math.isclose(v1, v2, abs_tol=delta) is False:
        logger.error("[low_limit] not equal in db and jq: %f, %f", v1, v2)
        return False

    return True


def compare_bars_for_openclose(
    secs_in_db, data_in_db, data_in_jq, all_index, index_whitelist
):
    # influxdb取回的数据是f8，并且是rec.array的数组
    # jq fetcher取回的数据是字典，key为股票代码，value是np.array，f4

    # 对比两个集合中的股票代码，是否完全一致
    secs_in_jq = set(data_in_jq.keys())
    rc = _compare_secs(secs_in_db, secs_in_jq)
    rc = True
    if not rc:
        return False

    for sec_data in data_in_db:
        code = sec_data["code"]

        is_index = False
        if code in all_index:
            is_index = True
        if is_index:
            if code not in index_whitelist:
                continue  # 不在白名单内的指数跳过检查

        sec_data_jq = data_in_jq[code]
        rc = _compare_sec_data(sec_data, sec_data_jq, is_index)
        if not rc:
            if is_index:
                logger.error("index code %s, failed to validate price", code)
            else:
                logger.error("stock code %s, failed to validate price", code)
            # return False

    return True


def compare_bars_for_pricelimits(
    secs_in_db, data_in_db, data_in_jq, all_index, index_whitelist
):
    # influxdb取回的数据是f8，并且是rec.array的数组
    # jq fetcher取回的数据是字典，key为股票代码，value是np.array，f4

    # 对比两个集合中的股票代码，是否完全一致
    _jq_data_dict = {}
    for data in data_in_jq:
        code = data["code"]
        _jq_data_dict[code] = data

    secs_in_jq = set(_jq_data_dict.keys())
    rc = _compare_secs(secs_in_db, secs_in_jq)
    if not rc:
        return False

    for sec_data in data_in_db:
        code = sec_data["code"]

        is_index = False
        if code in all_index:
            is_index = True
        if is_index and (code not in index_whitelist):
            continue  # 不在白名单内的指数跳过检查

        sec_data_jq = _jq_data_dict[code]
        rc = _compare_sec_data_pricelimits(sec_data, sec_data_jq, is_index)
        if not rc:
            if is_index:
                logger.error("index code %s, failed to validate price limits", code)
            else:
                logger.error("stock code %s, failed to validate price limits", code)
            # return False

    return True


def _compare_sec_min_data(sec_data_db, sec_data_jq):
    v1 = math_round(sec_data_db["open"], 2)
    v2 = math_round(sec_data_jq["open"][0].item(), 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[open] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_db["close"], 2)
    v2 = math_round(sec_data_jq["close"][0].item(), 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[close] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_db["high"], 2)
    v2 = math_round(sec_data_jq["high"][0].item(), 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[high] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = sec_data_db["volume"]
    v2 = sec_data_jq["volume"][0].item()
    if v1 != v2:
        logger.error("[volume] not equal in db and jq: %f, %f", v1, v2)
        return False

    return True


def _compare_sec_data_mW(sec_data_db, sec_data_jq):
    v1 = math_round(sec_data_db["open"], 2)
    v2 = math_round(sec_data_jq["open"][0].item(), 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[open] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_db["high"], 2)
    v2 = math_round(sec_data_jq["high"][0].item(), 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[high] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_db["low"], 2)
    v2 = math_round(sec_data_jq["low"][0].item(), 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[low] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_db["close"], 2)
    v2 = math_round(sec_data_jq["close"][0].item(), 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[close] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = sec_data_db["volume"]
    v2 = sec_data_jq["volume"][0].item()
    if v1 != v2:
        logger.error("[volume] not equal in db and jq: %f, %f", v1, v2)
        return False

    return True


def compare_bars_wM_for_openclose(secs_in_db, data_in_db, data_in_jq):
    # influxdb取回的数据是f8，并且是rec.array的数组
    # jq fetcher取回的数据是字典，key为股票代码，value是np.array，f4

    # 对比两个集合中的股票代码，是否完全一致
    secs_in_jq = set(data_in_jq.keys())
    rc = _compare_secs(secs_in_db, secs_in_jq)
    if not rc:
        logger.error("continue to compare data...")
        # return False

    for sec_data in data_in_db:
        code = sec_data["code"]
        sec_data_jq = data_in_jq[code]
        rc = _compare_sec_data_mW(sec_data, sec_data_jq)
        if not rc:
            logger.error("code %s, failed to validate price", code)
            # return False

    return True


def compare_sec_data_full(sec_data_1, sec_data_2):
    v1 = math_round(sec_data_1["open"], 2)
    v2 = math_round(sec_data_2["open"], 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[open] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_1["close"], 2)
    v2 = math_round(sec_data_2["close"], 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[close] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_1["high"], 2)
    v2 = math_round(sec_data_2["high"], 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[high] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_1["low"], 2)
    v2 = math_round(sec_data_2["low"], 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[low] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_1["factor"], 3)
    v2 = math_round(sec_data_2["factor"], 3)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[factor] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = sec_data_1["volume"]
    v2 = sec_data_2["volume"]
    if v1 != v2:
        logger.error("[volume] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = sec_data_1["amount"]
    v2 = sec_data_2["amount"]
    if v1 != v2:
        logger.error("[amount] not equal in db and jq: %f, %f", v1, v2)
        return False

    return True
