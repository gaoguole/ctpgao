"""
fixme: 这个是我目前用到的tick的触发机制 生成的k线价格和文华应该一致,  字段应该需要进行修改 大概一致, callback可以进行回调看你自己的需求怎么改
1.字段名字进行修改 这个有点依赖交易日判断我给你复制一份，这个是从QA那边拿到的， 懒得自己的 白嫖天神就完事了/ 或者你写一个交易日的判断
2.此处借助了tq的维护的夜盘时间表只要处理成标准格式就ok了

local_symbol:代码名称 随你自己定了

Tick:
    last_price：tick的最新价格,
    volume:增量增加的Tick
Kline：
    open_price/close_price/high_price/low_price  开收低高
    volume:当前k线里面的成交量
    interval: 周期
    first_volume: 一直在更新的成交量

"""
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime

from typing import Callable, Iterable

from sdk.date import get_day_from


@dataclass(init=False)
class Entity(dict):
    def __init__(self, **mapping):
        for key, value in mapping.items():
            setattr(self, key, value)


class Tick(Entity):
    last_price: float
    datetime: datetime
    local_symbol: str
    volume: int


class Kline(Entity):
    high_price: float
    close_price: float
    open_price: float
    low_price: float
    datetime: datetime
    volume: int
    local_symbol: str
    interval: int
    first_volume: int

    def __repr__(self):
        return f"Kline <code:{self.local_symbol}, datetime:{self.datetime}, high: {self.high_price}, open:{self.open_price}, low:{self.low_price}, close:{self.close_price}, volume:{self.volume}, frq:{self.interval}>"


class HighKlineSupporter:
    def __init__(self, code: str, callback: Callable, interval: Iterable, data: dict):
        assert code in data.keys()
        assert data[code].get("time") is not None
        if not isinstance(callback, Callable):
            raise TypeError("callable should be a function")
        self.callback = callback
        self.code = code
        self.last_entity: {int: None} = {x: None for x in interval}
        self.lock_free: {int: bool} = {x: True for x in interval}
        self.last_datetime = {x: None for x in interval}
        self._data = data

        self.h = self._data[self.code]["time"].get("night")
        self.night_allow = True if self.h is not None else False

    def update_tick(self, tick: Tick):
        bar = self.resample(tick)
        for x in bar:
            self.callback(x)

    def resample(self, tick_data: Tick) -> Kline or None:
        data = []
        for frq, last in self.last_entity.items():
            if last is None:
                self.last_entity[frq] = Kline(
                    datetime=tick_data.datetime,
                    high_price=tick_data.last_price,
                    low_price=tick_data.last_price,
                    close_price=tick_data.last_price,
                    open_price=tick_data.last_price,
                    interval=frq,
                    volume=0,
                    first_volume=tick_data.volume,
                    local_symbol=tick_data.local_symbol,
                )
                self.last_datetime[frq] = tick_data.datetime
            else:
                if self.lock_free[frq] is False:
                    if tick_data.datetime.hour == self.last_entity[frq].datetime.hour \
                            and tick_data.datetime.minute == self.last_entity[frq].datetime.minute \
                            and tick_data.datetime.second == self.last_entity[frq].datetime.second:
                        self.lock_free[frq] = True
                if frq != 1 and tick_data.datetime.minute % frq == 0 and abs(
                        (tick_data.datetime - self.last_datetime[frq]).seconds) >= 60 \
                        and self.lock_free[frq] is True:
                    temp = deepcopy(last)
                    check, tim = self.check_tick(tick_data)
                    if check is True:
                        temp.high_price = max(temp.high_price, tick_data.last_price)
                        temp.low_price = min(temp.low_price, tick_data.last_price)
                        temp.close_price = tick_data.last_price
                        temp.volume += max(tick_data.volume - temp.first_volume, 0)
                        self.last_entity[frq] = Kline(
                            datetime=tim,
                            high_price=tick_data.last_price,
                            low_price=tick_data.last_price,
                            close_price=tick_data.last_price,
                            open_price=tick_data.last_price,
                            interval=frq,
                            volume=0,
                            first_volume=tick_data.volume,
                            local_symbol=tick_data.local_symbol
                        )
                        self.lock_free[frq] = False
                        self.last_datetime[frq] = tim
                    else:
                        self.last_entity[frq] = Kline(
                            datetime=tick_data.datetime,
                            high_price=tick_data.last_price,
                            low_price=tick_data.last_price,
                            close_price=tick_data.last_price,
                            open_price=tick_data.last_price,
                            interval=frq,
                            volume=tick_data.volume - temp.first_volume,
                            first_volume=tick_data.volume,
                            local_symbol=tick_data.local_symbol
                        )
                        self.last_datetime[frq] = tick_data.datetime
                    data.append(temp)

                elif frq != 1 and self.lock_free[frq] is True:
                    self.last_entity[frq].high_price = max(self.last_entity[frq].high_price, tick_data.last_price)
                    self.last_entity[frq].low_price = min(self.last_entity[frq].low_price, tick_data.last_price)
                    self.last_entity[frq].close_price = tick_data.last_price
                    self.last_entity[frq].volume += max(tick_data.volume - self.last_entity[frq].first_volume, 0)
                    self.last_entity[frq].first_volume = tick_data.volume
                """
                处理一分钟的k线数据
                """
                if frq == 1 and tick_data.datetime.second == 0 and \
                        abs((tick_data.datetime - self.last_datetime[frq]).seconds) > 10 \
                        and self.lock_free[frq] is True:
                    temp = deepcopy(last)
                    check, tim = self.check_tick(tick_data)
                    if check is True:
                        """ 特殊时间需要特殊有处理 """
                        temp.high_price = max(temp.high_price, tick_data.last_price)
                        temp.low_price = min(temp.low_price, tick_data.last_price)
                        temp.close_price = tick_data.last_price
                        temp.volume += max(tick_data.volume - temp.first_volume, 0)
                        self.last_entity[frq] = Kline(
                            datetime=tim,
                            high_price=tick_data.last_price,
                            low_price=tick_data.last_price,
                            close_price=tick_data.last_price,
                            open_price=tick_data.last_price,
                            interval=frq,
                            volume=0,
                            first_volume=tick_data.volume,
                            local_symbol=tick_data.local_symbol
                        )
                        self.lock_free[frq] = False
                        self.last_datetime[frq] = tim
                    else:
                        self.last_entity[frq] = Kline(
                            datetime=tick_data.datetime,
                            high_price=tick_data.last_price,
                            low_price=tick_data.last_price,
                            close_price=tick_data.last_price,
                            open_price=tick_data.last_price,
                            interval=frq,
                            volume=tick_data.volume - temp.first_volume,
                            first_volume=tick_data.volume,
                            local_symbol=tick_data.local_symbol
                        )
                        self.last_datetime[frq] = tick_data.datetime
                    data.append(temp)
                elif frq == 1 and self.lock_free[frq]:
                    self.last_entity[frq].high_price = max(self.last_entity[frq].high_price, tick_data.last_price)
                    self.last_entity[frq].low_price = min(self.last_entity[frq].low_price, tick_data.last_price)
                    self.last_entity[frq].close_price = tick_data.last_price
                    """ 累积成交量 """
                    self.last_entity[frq].volume += max(tick_data.volume - self.last_entity[frq].first_volume, 0)
                    self.last_entity[frq].first_volume = tick_data.volume

        return data

    def check_tick(self, T: Tick):
        # todo: we need to check other product trade time, Now it only support future trade time
        if T.datetime.hour == 10 and T.datetime.minute == 15:
            return True, datetime.strptime(f"{T.datetime.date()} 10:30:00", "%Y-%m-%d %H:%M:%S")
        elif T.datetime.hour == 11 and T.datetime.minute == 30:
            return True, datetime.strptime(f"{T.datetime.date()} 13:30:00", "%Y-%m-%d %H:%M:%S")
        elif T.datetime.hour == 15 and T.datetime.minute == 0:
            return True, datetime.strptime(f"{T.datetime.date()} 15:00:00", "%Y-%m-%d %H:%M:%S")
        else:
            if self.night_allow:
                """ 处理夜盘 """
                hour, minute, second = [int(x) for x in self.h[0][-1].split(":")]
                hour = hour if hour <= 24 else hour - 24
                if T.datetime.hour == hour and T.datetime.minute == minute:  # make night kline true
                    return True, datetime.strptime(f"{get_day_from(date=str(T.datetime.date()), ne=1)} 09:00:00",
                                                   "%Y-%m-%d %H:%M:%S")
        return False, None
