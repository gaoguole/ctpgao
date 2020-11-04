from sdk.gaoapi2 import GaoApi
print(1)
api=GaoApi("040123","123456")
print(2)
行情=api.获取_K线("DCE.i2101",10)
api.获取_tick("DCE.i2101")
while True:
    持仓=api.查持仓("DCE.i2101")
    print(持仓.pos_long,持仓.pos_short,持仓.pos)
    #print(持仓.pos)
    api.wait_update()
    # 持仓=api.查持仓("SHFE.rb2101")
    # print(持仓)