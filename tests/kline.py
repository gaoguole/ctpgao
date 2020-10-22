""" 这里是Tick的测试机制 """
import json
from datetime import datetime

from sdk.kline_support import HighKlineSupporter, Tick


def kline_print(kline):
    print(kline)


if __name__ == '__main__':
    with open("rb_tick.json", "r") as fs:
        data = json.load(fs)

    with open("trade_time.json", "r") as f:
        time_data = json.load(f)

    ticks = data["data"]

    for tick in ticks:
        try:
            tick["datetime"] = datetime.strptime(tick["datetime"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            tick["datetime"] = datetime.strptime(tick["datetime"], "%Y-%m-%d %H:%M:%S.%f")
    kresampler = HighKlineSupporter("rb", kline_print, [5], time_data)
    for tick in ticks:
        kresampler.update_tick(Tick(**tick))
