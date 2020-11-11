from sdk.gaoapi2 import GaoApi
from tqsdk.tafunc import time_to_str,ma
import time
api=GaoApi("086515","123456")

行情=api.获取_K线("DCE.i2101",10,5)
print(行情)
行情2=api.获取_tick("SHFE.rb2101")
print(行情2.last_price)
while True:
    # print(行情)
    # 行情['ma']=ma(行情.close,5)
    # #print(time_to_str(行情.datetime.iloc[-1]))
    # print(time_to_str(time.time()))
    api.wait_update()
    if api.is_changing(行情.id.iloc[-1],"id"):
        print(行情)
        print(time_to_str(行情.datetime.iloc[-1]))