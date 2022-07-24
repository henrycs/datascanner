"""
上证 000001.XSHG
深指 399001.XSHE,
创业板 399006.XSHE
科创50指数 000688.XSHG
上证50 000016.XSHG
沪深300 399300.XSHE
中证500 399905.XSHE
中证1000 000852.XSHG
"""


def get_index_sec_whitelist():
    secs_set = set()

    secs_set.add("000001.XSHG")  # 上证指数
    secs_set.add("399001.XSHE")  # 深证成指
    secs_set.add("399006.XSHE")  # 创业板指
    secs_set.add("000688.XSHG")  # 科创50指数
    secs_set.add("000016.XSHG")  # 上证50
    secs_set.add("399300.XSHE")  # 沪深300
    secs_set.add("399905.XSHE")  # 中证500
    secs_set.add("000852.XSHG")  # 中证1000

    return secs_set
