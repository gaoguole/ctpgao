from sdk.gaoapi2 import GaoApi
from tqsdk.tafunc import time_to_str,ma
import time
api=GaoApi("086515","123456")

# 获取 SHFE.cu1812 合约的报价
from tqsdk import TqApi

async def demo():
    quote = api.get_quote("SHFE.cu1812")
    async with api.register_update_notify(quote) as update_chan:
        async for _ in update_chan:
            print(quote.last_price)

api.create_task(demo())
while True:
    api.wait_update()