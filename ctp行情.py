import tqsdk
import time
from sdk.gaoapi2 import GaoApi
from tqsdk.tafunc import time_to_str
api=GaoApi("040123","123456")

行情=api.get_tick_serial("CFFEX.IC2011")

while True:
    api.wait_update()
    print(行情.datetime.iloc[-1],行情.last_price.iloc[-1],time_to_str(time.time()))