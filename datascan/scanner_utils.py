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
        logger.error("secs in bars:1d but not in jq, %d", len(x1))
        return False

    y1 = secs_in_jq.difference(secs_in_db)
    if len(y1) > 0:  # 本地数据库中多余的股票或指数
        logger.error("secs in jq but not in bars:1d, %d", len(y1))
        return False

    return True


def _compare_sec_data(sec_data_db, sec_data_jq):
    v1 = math_round(sec_data_db["open"], 2)
    v2 = math_round(sec_data_jq["open"], 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("open not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_db["close"], 2)
    v2 = math_round(sec_data_jq["close"], 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("close not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = sec_data_db["volume"]
    v2 = sec_data_jq["volume"]
    if v1 != v2:
        logger.error("volume not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_db["factor"], 3)
    v2 = math_round(sec_data_jq["factor"], 3)
    if math.isclose(v1, v2, abs_tol=1e-4) is False:
        logger.error("factor not equal in db and jq: %f, %f", v1, v2)
        return False

    return True


def _compare_sec_data_pricelimits(sec_data_db, sec_data_jq):
    v1 = math_round(sec_data_db["high_limit"], 2)
    v2 = math_round(sec_data_jq["high_limit"], 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("high_limit not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_db["low_limit"], 2)
    v2 = math_round(sec_data_jq["low_limit"], 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("low_limit not equal in db and jq: %f, %f", v1, v2)
        return False

    return True


def compare_bars_for_openclose(secs_in_db, data_in_db, data_in_jq):
    # influxdb取回的数据是f8，并且是rec.array的数组
    # jq fetcher取回的数据是字典，key为股票代码，value是np.array，f4

    # 对比两个集合中的股票代码，是否完全一致
    secs_in_jq = set(data_in_jq.keys())
    rc = _compare_secs(secs_in_db, secs_in_jq)
    if not rc:
        return False

    for sec_data in data_in_db:
        code = sec_data["code"]
        sec_data_jq = data_in_jq[code]
        rc = _compare_sec_data(sec_data, sec_data_jq)
        if not rc:
            logger.error("code %s, failed to validate price", code)
            return False

    return True


def compare_bars_for_pricelimits(secs_in_db, data_in_db, data_in_jq):
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
        sec_data_jq = _jq_data_dict[code]
        rc = _compare_sec_data_pricelimits(sec_data, sec_data_jq)
        if not rc:
            logger.error("code %s, failed to validate price limits", code)
            return False

    return True


def _compare_sec_min_data(sec_data_db, sec_data_jq):
    v1 = math_round(sec_data_db["open"], 2)
    v2 = math_round(sec_data_jq["open"], 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[open] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_db["close"], 2)
    v2 = math_round(sec_data_jq["close"], 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[close] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = math_round(sec_data_db["high"], 2)
    v2 = math_round(sec_data_jq["high"], 2)
    if math.isclose(v1, v2, abs_tol=1e-2) is False:
        logger.error("[high] not equal in db and jq: %f, %f", v1, v2)
        return False

    v1 = sec_data_db["volume"]
    v2 = sec_data_jq["volume"]
    if v1 != v2:
        logger.error("[volume] not equal in db and jq: %f, %f", v1, v2)
        return False

    return True


def compare_bars_min_for_openclose(secs_in_db, data_in_db, data_in_jq, n_bars):
    # influxdb取回的数据是f8，并且是rec.array的数组
    # jq fetcher取回的数据是字典，key为股票代码，value是np.array，f4

    # secs_in_db是抽选的股票，因此单向比较
    secs_in_jq = set(data_in_jq.keys())
    rc = _compare_secs(secs_in_db, secs_in_jq)
    if not rc:
        return False

    db_data_by_sec = {}
    for sec_data in data_in_db:
        code = sec_data["code"]
        if code in secs_in_db:
            if code not in db_data_by_sec:
                db_data_by_sec[code] = []
            db_data_by_sec[code].append(sec_data)

    for code in secs_in_db:
        sec_data_jq = data_in_jq[code]
        sec_data = db_data_by_sec[code]
        # 比较240/48/16/8/4根线
        for i in range(n_bars):
            rc = _compare_sec_min_data(sec_data[i], sec_data_jq[i])
            if not rc:
                logger.error("code %s, failed to validate price", code)
                return False

    return True
