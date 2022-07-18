import datetime
import logging
import pickle
from typing import AnyStr, Dict, List, Union
import cfg4py
from coretypes import FrameType, SecurityType
import numpy as np
from omicron.models.timeframe import TimeFrame
from dfs import Storage

logger = logging.getLogger(__name__)


def get_trade_limit_filename(prefix: SecurityType, dt: datetime.date):
    assert isinstance(prefix, SecurityType)
    assert isinstance(dt, (datetime.datetime, datetime.date, str))
    filename = [prefix.value, "trade_limit", str(TimeFrame.date2int(dt))]
    return "/".join(filename)


def get_bars_filename(
    prefix: SecurityType,
    dt: datetime.date,
    frame_type: FrameType,
) -> AnyStr:  # pragma: no cover
    """拼接bars的文件名
    如 get_bars_filename(SecurityType.Stock, datetime.datetime(2022,2,18), FrameType.MIN)
    Return: stock/1m/20220218
    """
    filename = []
    if isinstance(prefix, SecurityType) and prefix in (
        SecurityType.STOCK,
        SecurityType.INDEX,
    ):
        filename.append(prefix.value)
    else:
        raise TypeError(
            "prefix must be type SecurityType and in (SecurityType.STOCK, SecurityType.INDEX)"
        )

    if isinstance(frame_type, FrameType):
        filename.append(frame_type.value)
    elif isinstance(frame_type, str):
        filename.append(frame_type)
    else:
        raise TypeError("prefix must be type FrameType, str")
    if isinstance(dt, str):
        filename.append(TimeFrame.int2date(dt))
    elif isinstance(dt, datetime.datetime) or isinstance(dt, datetime.date):
        filename.append(str(TimeFrame.date2int(dt)))
    else:
        raise TypeError("dt must be type datetime, date, str, got type:%s" % type(dt))

    return "/".join(filename)


async def write_bars_dfs(
    dt: datetime.date,
    frame_type: List[FrameType],
    bars: Union[Dict[str, np.ndarray], np.ndarray],
    prefix: SecurityType
):
    dfs = Storage()
    if dfs is None:
        return False
    
    cfg = cfg4py.get_instance()
    binary = pickle.dumps(bars, protocol=cfg.pickle.ver)

    filename = get_bars_filename(prefix, dt, frame_type)
    logger.info(
        "write bars to dfs: %d secs (%d bytes) -> %s",
        len(bars),
        len(binary),
        filename,
    )
    await dfs.write(filename, binary)


async def write_price_limits_dfs(
    dt: datetime.date,
    bars: Union[Dict[str, np.ndarray], np.ndarray],
    prefix: SecurityType
):
    dfs = Storage()
    if dfs is None:
        return False
    
    cfg = cfg4py.get_instance()
    binary = pickle.dumps(bars, protocol=cfg.pickle.ver)

    filename = get_trade_limit_filename(prefix, dt)
    logger.info(
        "write price limits bars to dfs: %d secs (%d bytes) -> %s",
        len(bars),
        len(binary),
        filename,
    )
    await dfs.write(filename, binary)