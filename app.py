import asyncio
import datetime
import logging
import os
import sys
import time
from logging.handlers import TimedRotatingFileHandler
from os import path
from typing import List

import arrow
import cfg4py
import omicron
from coretypes import FrameType
from omicron.dal.cache import cache

from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher
from main import get_cache_keyname, scanner_main

logger = logging.getLogger(__name__)


def get_config_dir():
    _dir = path.normpath(os.path.join(path.dirname(__file__), "config"))
    return _dir


def init_log_path(log_dir):
    if os.path.exists(log_dir):
        return 0

    try:
        os.makedirs(log_dir)
    except Exception as e:
        print(e)
        exit("failed to create log folder")

    return 0


def init_logger(filename: str, loglevel: int):
    LOG_FORMAT = r"%(asctime)s %(levelname)s %(filename)s[line:%(lineno)d] %(message)s"
    DATE_FORMAT = r"%Y-%m-%d  %H:%M:%S %a"

    fh = TimedRotatingFileHandler(
        filename, when="D", interval=1, backupCount=7, encoding="utf-8"
    )
    fh.setLevel(loglevel)
    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)
    fh.setFormatter(formatter)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(loglevel)
    console.setFormatter(formatter)

    logging.basicConfig(
        level=loglevel,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
        handlers=[console, fh],
    )


class Omega(object):
    def __init__(self, fetcher_impl: str, **kwargs):
        self.fetcher_impl = fetcher_impl
        self.params = kwargs

    async def check_running_conditions(self, ft):
        dt1 = datetime.time(3, 0, 0)
        dt2 = datetime.time(8, 0, 0)
        dt3 = datetime.time(17, 10, 0)

        now = datetime.datetime.now()
        nowtime = now.time()

        if nowtime < dt1:  # 3点前有其他任务
            return False
        if nowtime > dt2 and nowtime < dt3:  # 交易时间段不执行
            return False

        key = get_cache_keyname(ft)
        start_str = await cache.sys.get(key)
        if start_str is None:
            target_day = datetime.date(2022, 7, 16)
        else:
            target_day = arrow.get(start_str).date()
        if target_day <= datetime.date(2005, 1, 4):
            return False

        return True

    async def init(self, *args):
        logger.info("init %s", self.__class__.__name__)

        await omicron.cache.init()
        try:
            await omicron.init()
        except Exception as e:
            logger.error("No calendar and securities in cache, %s", e)
            time.sleep(5)
            os._exit(1)

        logger.info("<<< init %s process done", self.__class__.__name__)

        ft = FrameType.MIN5
        rc = await self.check_running_conditions(ft)
        if rc:
            await AbstractQuotesFetcher.create_instance(
                self.fetcher_impl, **self.params
            )
            await scanner_main(ft)

        await omicron.close()


def start():
    current_dir = os.getcwd()
    print("current dir:", current_dir)

    config_dir = get_config_dir()
    cfg4py.init(config_dir, False)
    cfg = cfg4py.get_instance()

    loglevel = logging.INFO
    log_dir = path.normpath(os.path.join(current_dir, "logs"))
    init_log_path(log_dir)

    logfile = path.normpath(path.join(log_dir, "datascan.log"))
    init_logger(logfile, loglevel)

    fetcher = cfg.quotes_fetcher
    impl = fetcher.impl
    account = fetcher.account
    password = fetcher.password
    omega = Omega(impl, account=account, password=password)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(omega.init())
    # loop.run_forever()


if __name__ == "__main__":
    start()
