import tqsdk
import time
from tqsdk.tafunc import time_to_str
api=tqsdk.TqApi(auth="270829786@qq.com,24729220a")

行情=api.get_tick_serial("CFFEX.IC2011")

while True:
    api.wait_update()
    print(行情.datetime.iloc[-1],行情.last_price.iloc[-1],time_to_str(time.time()))