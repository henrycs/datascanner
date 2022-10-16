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

from data_fix.download_days import redownload_bars1d_for_target_day
from data_fix.download_mins import redownload_bars_mins_for_target_day
from data_fix.download_week import redownload_bars1w_for_target_day
from datascan.validate_data_in_week import reverse_scanner_handler
from download_bars.day_handler import scanner_handler_day
from download_bars.week_handler import month_download_handler, week_download_handler
from fetchers.abstract_quotes_fetcher import AbstractQuotesFetcher
from influx_tools import drop_bars_1M, remove_allsecs_in_bars1d
from pricestats.sum_history import sum_price_stats
from rapidscan.main import get_cache_keyname
from rebuild_minio.build_min_data import rebuild_minio_for_min

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

        await AbstractQuotesFetcher.create_instance(self.fetcher_impl, **self.params)

        try:
            # await remove_allsecs_in_bars1d(datetime.date(2022, 8, 15))
            # await drop_bars_1w()
            # await drop_bars_1M()
            # await drop_bars_via_scope(target_year, FrameType.WEEK)

            # await week_download_handler()
            # await month_download_handler()

            # await scanner_handler_day()
            # await scanner_handler_minutes(ft, False)
            await reverse_scanner_handler(scanning_type=0)
            # await sum_price_stats()

            # await redownload_bars1w_for_target_day()
            # await redownload_bars1d_for_target_day()
            # await redownload_bars_mins_for_target_day()

            # await rebuild_minio_for_min()
        except Exception as e:
            logger.exception(e)
            logger.info("failed to execution: %s", e)
            return False

        logger.info("all tasks finished.")
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
