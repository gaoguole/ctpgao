from sdk.gaoapi2 import GaoApi
print(1)
api=GaoApi("040123","123456")
print(2)
行情=api.获取_K线("SHFE.rb2101",10)
api.获取_tick("SHFE.rb2101")
n=0
while True:
    持仓=api.查持仓("INE.sc2012")
    print(持仓.pos_long,持仓.open_price_long,持仓.pos_short,持仓.open_price_short,持仓.pos)
    print(持仓)
    api.wait_update()
    # 持仓=api.查持仓("SHFE.rb2101")
    # print(持仓)
    if n==0:
        n+=1
        api.查询当日成交订单()