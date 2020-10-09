import numpy as np
import pandas as pd
import tqsdk
from tqsdk.tafunc import time_to_str
import tqsdk.tafunc
import time
import numba
import re
import requests
import json

def 调仓函数(api,品种,目标值):
    当前持仓=api.get_position(品种)
    当前值=当前持仓.pos_long-当前持仓.pos_short
    if 目标值>当前值:
        需要增加仓位值=目标值-当前值
        if 当前持仓.pos_short>0:
            if 当前持仓.pos_short>=需要增加仓位值:
                if 需要增加仓位值:
                    追单平仓2(api,品种,"BUY",需要增加仓位值)
            else:
                平仓值1=当前持仓.pos_short
                开仓值1=需要增加仓位值-平仓值1
                if 平仓值1:
                    追单平仓2(api,品种,"BUY",平仓值1)
                if 开仓值1:
                
                    追单开仓2(api,品种,"BUY",开仓值1)
        else:
            if 需要增加仓位值:
                追单开仓2(api,品种,"BUY",需要增加仓位值)

    else:
        需要减少仓位值=当前值-目标值
        if 当前持仓.pos_long>0:
            if 当前持仓.pos_long>=需要减少仓位值:
                if 需要减少仓位值:
                    追单平仓2(api,品种,"SELL",需要减少仓位值)
            else:
                平仓值1=当前持仓.pos_long
                开仓值1=需要减少仓位值-平仓值1
                if 平仓值1:
                    追单平仓2(api,品种,"SELL",平仓值1)
                if 开仓值1:
                    追单开仓2(api,品种,"SELL",开仓值1)
        else:
            if 需要减少仓位值:
                追单开仓2(api,品种,"SELL",需要减少仓位值)

    while True:
        当前持仓=api.get_position(品种)
        当前值=当前持仓.pos_long-当前持仓.pos_short
        if 当前值==目标值:
            break
        if 当前值==目标值:
            break
        api.wait_update()
        当前持仓=api.get_position(品种)
        当前值=当前持仓.pos_long-当前持仓.pos_short
        if 当前值==目标值:
            break


def 开仓有效时间(有效time_list):
    当前时间=time.strftime("%H:%M",time.localtime())
    时间判断真假=[ x[0]<=当前时间<x[1] for x in 有效time_list]
    if any(时间判断真假):
        return 1
    return  0

def 查询最近成交时间(api,品种,买卖,开平):
    总成交=api.get_trade()
    l=[]
    for x in 总成交:
        成交信息=api.get_trade(x)
        l.append((成交信息.trade_date_time,成交信息.direction,成交信息.offset,\
            成交信息.price,成交信息.exchange_id+"."+成交信息.instrument_id ))
    l=sorted(l,key=lambda x: x[0],reverse=True)
    for x in l:
        if 买卖=="买" and 开平=="开":
            for x in l:
                if x[1]=="BUY" and x[2]=="OPEN" and x[4]==品种:
                    return x[0]
        if 买卖=="买" and 开平=="平":
            for x in l:
                if x[1]=="BUY" and x[2] in ("CLOSE","CLOSETODAY") and x[4]==品种:
                    return x[0]
        if 买卖=="卖" and 开平=="开":
            for x in l:
                if x[1]=="SELL" and x[2]=="OPEN" and x[4]==品种:
                    return x[0]
        if 买卖=="卖" and 开平=="平":
            for x in l:
                if x[1]=="SELL" and x[2] in ("CLOSE","CLOSETODAY") and x[4]==品种:
                    return x[0]
def 维护持仓_redis(连接,品种,买卖,开平,价格,数量):
    全品种=[ x.decode()  for  x in 连接.keys("*")]
    if 品种 not in 全品种:
        data={"多仓":0,"空仓":0,"多仓成本":0,"空仓成本":0}
    else:
        data=eval(连接.get(品种))
    if 买卖=="买" and 开平=="开":
        data["多仓成本"]= (data["多仓"]*data["多仓成本"]+价格*数量)/(data["多仓"]+数量)
        data["多仓"]=data["多仓"]+数量
    if 买卖=="买" and 开平=="平":
        data["空仓"]=data["空仓"]-数量

    if 买卖=="卖" and 开平=="开":
        data["空仓成本"]= (data["空仓"]*data["空仓成本"]+价格*数量)/(data["空仓"]+数量)
        data["空仓"]=data["空仓"]+数量
    if 买卖=="卖" and 开平=="平":
        data["多仓"]=data["多仓"]-数量
    
    连接.set(品种,str(data))
    return data

def 获取持仓_redis(连接,品种):
    全品种=[ x.decode()  for  x in 连接.keys("*")]
    if 品种 not in 全品种:
        data={"多仓":0,"空仓":0,"多仓成本":0,"空仓成本":0}
        连接.set(品种,str(data))
    else:
        data=eval(连接.get(品种).decode())
    return data


def 维护持仓(路径地址,买卖,开平,价格,数量):
    try:
        f=open(路径地址,"r")
        data=eval(f.read())
        f.close()
    except:
        f=open(路径地址,"w")
        f.close()
        data={"多仓":0,"空仓":0,"多仓成本":0,"空仓成本":0}
    if 买卖=="买" and 开平=="开":
        data["多仓成本"]= (data["多仓"]*data["多仓成本"]+价格*数量)/(data["多仓"]+数量)
        data["多仓"]=data["多仓"]+数量
    if 买卖=="买" and 开平=="平":
        data["空仓"]=data["空仓"]-数量

    if 买卖=="卖" and 开平=="开":
        data["空仓成本"]= (data["空仓"]*data["空仓成本"]+价格*数量)/(data["空仓"]+数量)
        data["空仓"]=data["空仓"]+数量
    if 买卖=="卖" and 开平=="平":
        data["多仓"]=data["多仓"]-数量
    f=open(路径地址,"w")
    f.write(str(data))
    f.close()
    return data
def 获取持仓(路径地址):
    try:
        f=open(路径地址,"r")
        data=eval(f.read())
        f.close()
    except:
        f=open(路径地址,"w")
        f.close()
        data={"多仓":0,"空仓":0,"多仓成本":0,"空仓成本":0}
    return data


def 查询最近成交价格(api,买卖,开平):
    总成交=api.get_trade()
    l=[]
    for x in 总成交:
        成交信息=api.get_trade(x)
        l.append((time_to_str(成交信息.trade_date_time),成交信息.direction,成交信息.offset,成交信息.price))
    l=sorted(l,key=lambda x: x[0],reverse=True)
    for x in l:
        if 买卖=="买" and 开平=="开":
            for x in l:
                if x[1]=="BUY" and x[2]=="OPEN":
                    return x[3]
        if 买卖=="买" and 开平=="平":
            for x in l:
                if x[1]=="BUY" and x[2] in ("CLOSE","CLOSETODAY"):
                    return x[3]

        if 买卖=="卖" and 开平=="开":
            for x in l:
                if x[1]=="SELL" and x[2]=="OPEN":
                    return x[3]
        if 买卖=="卖" and 开平=="平":
            for x in l:
                if x[1]=="SELL" and x[2] in ("CLOSE","CLOSETODAY"):
                    return x[3]

def 追单开仓2(api,品种,方向,需要平的数量,增加的点差=20):
    qutoe=api.get_quote(品种)
    最小变动单位=查询合约相关信息(api,品种)["合约最小跳数"]
    if 方向=="SELL":
        下单价格=qutoe.lower_limit if qutoe.last_price-最小变动单位*增加的点差< qutoe.lower_limit else qutoe.last_price-最小变动单位*增加的点差
    else:
        下单价格=qutoe.upper_limit if qutoe.last_price+最小变动单位*增加的点差> qutoe.upper_limit else qutoe.last_price+最小变动单位*增加的点差

    a=api.insert_order(品种,方向,"OPEN",需要平的数量,下单价格)
    while True:
        api.wait_update()
        a=api.get_order(a.order_id)
        if a.status=="FINISHED":
            成交价格=a.trade_price
            break
    print("成交价格",成交价格)
    return 成交价格
def 追单平仓2(api,品种,方向,需要平的数量,增加的点差=20):
    持仓=api.get_position(品种)
    多仓=持仓.pos_long_his+持仓.pos_long_today
    空仓=持仓.pos_short_his+持仓.pos_short_today
    qutoe=api.get_quote(品种)
    最小变动单位=查询合约相关信息(api,品种)["合约最小跳数"]
    成交价格=0
    if 品种.split('.')[0] in ('SHFE','INE') :
        if 方向=="SELL":
            下单价格=qutoe.lower_limit if qutoe.last_price-最小变动单位*增加的点差< qutoe.lower_limit else qutoe.last_price-最小变动单位*增加的点差
            平昨日仓=min(需要平的数量,持仓.pos_long_his)
            if 平昨日仓:

                a=api.insert_order(品种,方向,'CLOSE',平昨日仓,limit_price=下单价格)
                #tqsdk.lib.InsertOrderUntilAllTradedTask(api,品种,方向,"CLOSE",平昨日仓)
                while True:
                    api.wait_update()
                    a=api.get_order(a.order_id)
                    if a.status=="FINISHED":
                        成交价格=a.trade_price
                        break
            还需平的仓=需要平的数量-平昨日仓
            平今日仓=min(还需平的仓,持仓.pos_long_today)
            if 平今日仓:
                a=api.insert_order(品种,方向,'CLOSETODAY',平今日仓,limit_price=下单价格)
                #tqsdk.lib.InsertOrderUntilAllTradedTask(api,品种,方向,"CLOSETODAY",平今日仓)
                while True:
                    api.wait_update()
                    a=api.get_order(a.order_id)
                    if a.status=="FINISHED":
                        成交价格=a.trade_price
                        break

        else:
            下单价格=qutoe.upper_limit if qutoe.last_price+最小变动单位*增加的点差> qutoe.upper_limit else qutoe.last_price+最小变动单位*增加的点差
            平昨日仓=min(需要平的数量,持仓.pos_short_his)
            if 平昨日仓:
                a=api.insert_order(品种,方向,'CLOSE',平昨日仓,limit_price=下单价格)
                #tqsdk.lib.InsertOrderUntilAllTradedTask(api,品种,方向,"CLOSE",平昨日仓)
                while True:
                    api.wait_update()
                    a=api.get_order(a.order_id)
                    if a.status=="FINISHED":
                        成交价格=a.trade_price
                        break
            还需平的仓=需要平的数量-平昨日仓
            平今日仓=min(还需平的仓,持仓.pos_short_today)
            if 平今日仓:
                a=api.insert_order(品种,方向,'CLOSETODAY',平今日仓,limit_price=下单价格)
                while True:
                    api.wait_update()
                    a=api.get_order(a.order_id)
                    if a.status=="FINISHED":
                        成交价格=a.trade_price
                        break

                #tqsdk.lib.InsertOrderUntilAllTradedTask(api,品种,方向,"CLOSETODAY",平今日仓)    
    else:
        if 方向=="SELL":
            下单价格=qutoe.lower_limit if qutoe.last_price-最小变动单位*增加的点差< qutoe.lower_limit else qutoe.last_price-最小变动单位*增加的点差
            if min(多仓,需要平的数量):
                a=api.insert_order(品种,方向,'CLOSE',min(多仓,需要平的数量),limit_price=下单价格)
                #tqsdk.lib.InsertOrderUntilAllTradedTask(api,品种,方向,"CLOSE",min(多仓,需要平的数量))
                while True:
                    api.wait_update()
                    a=api.get_order(a.order_id)
                    if a.status=="FINISHED":
                        成交价格=a.trade_price
                        break
        else:
            下单价格=qutoe.upper_limit if qutoe.last_price+最小变动单位*增加的点差> qutoe.upper_limit else qutoe.last_price+最小变动单位*增加的点差
            if min(空仓,需要平的数量):
                a=api.insert_order(品种,方向,'CLOSE',min(空仓,需要平的数量),limit_price=下单价格)
                #tqsdk.lib.InsertOrderUntilAllTradedTask(api,品种,方向,"CLOSE",min(空仓,需要平的数量))
                while True:
                    api.wait_update()
                    a=api.get_order(a.order_id)
                    if a.status=="FINISHED":
                        成交价格=a.trade_price
                        break
    # 持仓=api.get_position(品种)
    # while True:
        
    #     if 方向=="SELL":
    #         if 持仓.pos_long==0:
    #             break
    #     else:
    #         if 持仓.pos_short==0:
    #             break
    #     api.wait_update()
    #     time.sleep(1)

    return 成交价格
def 品种无交易所简称查带交易简称(api,无交易所的简称列表):
    主力合约=查询所有主力合约(api)
    全称列表=[ x.split("@")[1]  for x in 主力合约  ]
    简称列表=[ x.split(".")[1] for x in 全称列表]
    l=[]
    try:
        for x in 无交易所的简称列表:
            l.append(全称列表[简称列表.index(x)])
    except:
        print(x,"没有这个品种")
    return l

def 根据合约名字列表和周期查询出data字典(合约名称列表,周期):
    d={}

    for x in 合约名称列表:
        if "@" not in x:
            大类名字= re.findall("([a-zA-Z]*)[0-9]{1,}$",x)[0]
        else:
            大类名字= x.split(".")[-1]

        data=pd.read_csv(".\\data\\"+大类名字+"\\"+x+"\\"+str(周期)+".csv",header=None)
        d[x]=data.to_numpy()
    return d
        

def numpy_to_pandas(data):
    return pd.DataFrame(data,columns=["datetime",'open','high','low','close',"volume","open_oi","close_oi"])




def 根据限定时间查到涉及到的具体合约(start时间,end时间,查询列表,历史data文件夹):
    l=[]
    startnew_timestamp=time.mktime(time.strptime(start时间,"%Y-%m-%d"))*1e9
    endnew_timestamp=time.mktime(time.strptime(end时间,"%Y-%m-%d"))*1e9

    for x in 查询列表:
        data=pd.read_csv(历史data文件夹+"\\"+x+'.csv')
        for y in range(len(data)):
            #print(startnew_timestamp,data.datetime.iloc[x],endnew_timestamp)
            if startnew_timestamp<data.datetime.iloc[y]<endnew_timestamp:
                if data.symbol_main.iloc[y] not in l:
                    l.append(data.symbol_main.iloc[y])
    return l
        


def dingding_message(地址,内容):
    headers = {'Content-Type': 'application/json'}
    data =  {
    "msgtype": "text",
    "text": {
        "content": 内容
    }
    }
    requests.post(地址, data=json.dumps(data), headers=headers)

def 查询合约中文名(品种):
    合约名字=品种.split('.')[1]
    合约具体=re.findall("([a-zA-Z]{1,})",合约名字)[0]
    # print(合约具体)
    d={
        "IF":"IF",
        "IH":"IH",
        "IC":"IC",
        "TS":"二债",
        "TF":"五债",
        "TF":"五债",
        "T":"十债",
        "cu":"沪铜",
        "zn":"沪锌",
        "al":"沪铝",
        "pd":"沪铅",
        "ni":"沪镍",
        "sn":"沪锡",
        "au":"沪金",
        "ag":"沪银",
        'rb':"螺纹",
        'wr':"线材",
        'hc':"热卷",
        'ss':"SS",
        'fu':"燃油",
        'bu':"沥青",
        'ru':"橡胶",
        'sp':"纸浆",



        "m":"豆粕",
        "y":"豆油",
        "a":"豆一",
        "b":"豆二",
        "p":"棕榈",
        "c":"玉米",
        "cs":"淀粉",
        "rr":"粳米",
        "jd":"鸡蛋",
        "bb":"胶板",
        "fb":"纤板",
        "l":"塑料",
        "v":"PVC",
        "eg":"EG",
        "pp":"PP" ,
        "eb":"EB",
        "j":"焦炭",
        "jm":"焦煤",
        "i":"铁矿",
        "pg":"LPG",
        "SR":"白糖",
        "CF":"郑棉",
        "CY":"棉纱",
        "ZC":"郑煤",
        "FG":"玻璃",
        "TA":"PTA" ,
        "MA":"郑醇",
        "UR":"尿素",
        "SA":"纯碱",
        "WH":"郑麦",
        "RI":"早稻",
        "LR":"晚稻",
        "JR":"粳稻",
        "RS":"菜籽",
        "OI":"郑油",
        "RM":"菜粕",
        "SF":"硅铁",
        "SM":"锰硅",
        "AP":"苹果",
        "CJ":"红枣",
        "sc":"原油",
        "nr":"NR",

    }

    if 合约具体 in d:
        return d[合约具体]
    return 合约具体
def ea_break(api,成交记录文件地址):
    struct_time=time.localtime()
    时间=time.strftime("%H:%M:%S",struct_time)
    if "02:30:00"<=时间<"08:30:00":
        return 1
    if "15:15:00"<=时间<"20:30:00":
        try:
            f=open(成交记录文件地址,"r")
            data=f.read()
            f.close()
        except:
            data=""
        f=open(成交记录文件地址,'a+')
        a=api.get_trade()
        if not data:
            f.write("成交时间,成交品种,下单方向,开平标志,委托价格,成交价格,成交手数,委托单号,成交单号,成交手续费\n")
        for x in a:
            b=api.get_trade(x)
            #获取成交的订单信息
            c=api.get_order(b.order_id)
            f.write(','.join([time_to_str(b.trade_date_time),
            b.exchange_id+b.instrument_id,
            b.direction,
            b.offset,
            str(c.limit_price),
            str(b.price),
            str(b.volume),
            b.order_id,
            b.trade_id,
            str(b.commission)])+'\n'
            )
        f.close()
        return 1
    return 0

def 订阅异常(api,x):
    try:
        return api.get_kline_serial(x,60*60*24,2000)
    except:
        return 0
def 查询品种主力的月份(api,品种):
    t1=time.time()
    总合约=查询所有合约(api)
    t2=time.time()
    品种相关合约=[ x for x in 总合约 if re.findall("^"+品种+"[0-9]{1,}$",x)]
    品种相关合约=sorted(品种相关合约)
    t3=time.time()
    订阅data合约字典={}
    # 订阅data合约字典={  x:api.get_kline_serial(x,60*60*24,2000)  for x in 品种相关合约}
    for x in 品种相关合约:
        a=订阅异常(api,x)
        if type(a) !=type(0):
            订阅data合约字典[x]=a

    t4=time.time()
    订阅data合约新字典={   x:{ 订阅data合约字典[x].datetime.iloc[y]  : 订阅data合约字典[x].volume.iloc[y] for y in range(len(订阅data合约字典[x]))}      for x in 订阅data合约字典}
    t5=time.time()
    订阅主连=api.get_kline_serial("KQ.m@"+品种,60*60*24,2000)
    t6=time.time()
    l=[]
    for x in range(0,2000,10):
        if 订阅主连.datetime.iloc[x]:
            日期=订阅主连.datetime.iloc[x]
            Volume=订阅主连.volume.iloc[x]
            for y in 订阅data合约新字典:
                if 日期 in 订阅data合约新字典[y]:
                    if 订阅data合约新字典[y][日期]==Volume:
                        l.append(y)
                        break
            else:
                l.append("")
        else:
            l.append("")
    t7=time.time()
    l.append(api.get_quote("KQ.m@"+品种)["underlying_symbol"])
    t8=time.time()
    #print("t2-t1",t2-t1,"t3-t2",t3-t2,"t4-t3",t4-t3,"t5-t4",t5-t4,"t6-t5",t6-t5,"t7-t6",t7-t6,"t8-t7",t8-t7)
    b=set([ x[-2:]  for x in l if x])
    return b
def 查询品种能成为主力合约的合约(api,品种):
    总合约=查询所有合约(api)
    品种相关合约=[ x for x in 总合约 if re.findall("^"+品种+"[0-9]{1,}$",x)]
    主力月=查询品种主力的月份(api,品种)
    l=[]
    for x in 品种相关合约:
        if x[-2:] in 主力月:
            l.append(x)
    return l


def 查询品种历史主连映射(api,品种):
    总合约=查询所有合约(api)
    品种相关合约=[ x for x in 总合约 if re.findall("^"+品种+"[0-9]{1,}$",x)]
    合约月份=查询品种主力的月份(api,品种)
    品种相关合约=[ x for x in 品种相关合约 if x[-2:] in 合约月份]
    #订阅data合约字典={  x:api.get_kline_serial(x,60*60*24,2000)  for x in 品种相关合约}
    订阅data合约字典={}
    for x in 品种相关合约:
        a=订阅异常(api,x)
        if type(a) !=type(0):
            订阅data合约字典[x]=a
    订阅data合约新字典={   x:{ 订阅data合约字典[x].datetime.iloc[y]  : 订阅data合约字典[x].volume.iloc[y] for y in range(len(订阅data合约字典[x]))}      for x in 订阅data合约字典}
    订阅主连=api.get_kline_serial("KQ.m@"+品种,60*60*24,2000)
    l=[]
    for x in range(2000):
        if 订阅主连.datetime.iloc[x]:
            日期=订阅主连.datetime.iloc[x]
            Volume=订阅主连.volume.iloc[x]
            for y in 订阅data合约新字典:
                if 日期 in 订阅data合约新字典[y]:
                    if 订阅data合约新字典[y][日期]==Volume:
                        l.append(y)
                        break
            else:
                l.append("")
        else:
            l.append("")
    l[-1]=api.get_quote("KQ.m@"+品种)["underlying_symbol"]
    return pd.DataFrame({"datetime":订阅主连.datetime,"symbol_main":pd.Series(l)})




def 查询当前时间是否在交易时间内(api,品种,秒数):
    交易时间=查询合约相关信息(api,品种)['交易时间']
    交易时间=交易时间["day"]+交易时间["night"]
    l=[]
    for x in 交易时间:
        if x[1]>"24:00:00":
            l.append([x[0],"24:01:00"])
            l.append(["00:00:00","%02d"%(int(x[1][:2])-24)+x[1][2:]])
        else:
            l.append(x)
    交易时间=l
    t1=time.time()-秒数
    t1=time.localtime(t1)
    t1=time.strftime("%H:%M:%S",t1)
    t2=time.time()+秒数
    t2=time.localtime(t2)
    t2=time.strftime("%H:%M:%S",t2)
    data1= [ 1 if x[0]<=t1<x[1]   else 0 for x in 交易时间 ]
    data2= [ 1 if x[0]<=t2<x[1]   else 0 for x in 交易时间 ]
    if any(data1) and any(data2):
        return 1
    return 0
    
def 查询所有合约(api):
    return [ x for x in api._data["quotes"]]
def 查询所有在交易合约(api):
    return [ x for x in api._data["quotes"] if api._data["quotes"][x]["expired"]==False]
def 查询所有主力合约(api):
    return [  x for x in 查询所有合约(api) if ".m@" in x]
def 查询所有指数合约(api):
    return [  x for x in 查询所有合约(api) if ".i@" in x]

def 查询所有主力合约映射到的具体合约(api):
    合约原=查询所有主力合约(api)
    return [  api._data["quotes"][x]['underlying_symbol']  for x in 合约原]

def 查询合约相关信息(api,品种):
    data=api._data['quotes'][品种]
    d={}
    d['交易时间']=data['trading_time']
    d['合约倍数']=data['volume_multiple']
    d['合约最小跳数']=data['price_tick']
    return d

def 撤销所有平多订单(api,品种):
    全部订单=api.get_order()
    for x in 全部订单:
        单一订单=api.get_order(x)
        if 单一订单.instrument_id==品种.split(".")[1] and 单一订单.status=="ALIVE" and 单一订单.direction =="SELL" and (单一订单.offset=="CLOSE" or 单一订单.offset=="CLOSETODAY"):
            api.cancel_order(单一订单.order_id)
            api.wait_update()
            while True:
                a=api.get_order(x)
                if a.status!="ALIVE":
                    break
                api.wait_update()

def 撤销所有平空订单(api,品种):
    全部订单=api.get_order()
    for x in 全部订单:
        单一订单=api.get_order(x)
        if 单一订单.instrument_id==品种.split(".")[1] and 单一订单.status=="ALIVE" and 单一订单.direction =="BUY" and (单一订单.offset=="CLOSE" or 单一订单.offset=="CLOSETODAY"):
            api.cancel_order(单一订单.order_id)
            api.wait_update()
            while True:
                a=api.get_order(x)
                if a.status!="ALIVE":
                    break
                api.wait_update()
def WH_获取时间对应id(行情,时间点):
    a=time.strptime(时间点,'%Y-%m-%d %H:%M:%S')
    时间点new_timestamp=time.mktime(a)*1e9
    for x in range(len(行情)):
        if 行情.datetime.iloc[x]>=时间点new_timestamp:
            return 行情.id.iloc[x]
def psar(barsdata, iaf = 0.02, maxaf = 0.2):
    length = len(barsdata)
    # dates = list(barsdata['Date'])
    high = list(barsdata['high'])
    low = list(barsdata['low'])
    close = list(barsdata['close'])
    psar = close[0:len(close)]
    psarbull = [None] * length
    psarbear = [None] * length
    bull = True
    af = iaf
    ep = low[0]
    hp = high[0]
    lp = low[0]
    
    for i in range(2,length):
        if bull:
            psar[i] = psar[i - 1] + af * (hp - psar[i - 1])
        else:
            psar[i] = psar[i - 1] + af * (lp - psar[i - 1])
        
        reverse = False
        
        if bull:
            if low[i] < psar[i]:
                bull = False
                reverse = True
                psar[i] = hp
                lp = low[i]
                af = iaf
        else:
            if high[i] > psar[i]:
                bull = True
                reverse = True
                psar[i] = lp
                hp = high[i]
                af = iaf
    
        if not reverse:
            if bull:
                if high[i] > hp:
                    hp = high[i]
                    af = min(af + iaf, maxaf)
                if low[i - 1] < psar[i]:
                    psar[i] = low[i - 1]
                if low[i - 2] < psar[i]:
                    psar[i] = low[i - 2]
            else:
                if low[i] < lp:
                    lp = low[i]
                    af = min(af + iaf, maxaf)
                if high[i - 1] > psar[i]:
                    psar[i] = high[i - 1]
                if high[i - 2] > psar[i]:
                    psar[i] = high[i - 2]
                    
        if bull:
            psarbull[i] = psar[i]
        else:
            psarbear[i] = psar[i]
 
    return  pd.Series(psar)

def WH_barlast(data,yes=True):
    #获取真值索引
    b=data[data==yes].index
    #去掉非真值data
    c=data.where(data==yes,np.NaN)
    #真值索引上的data更新为真值索引
    c[b]= b
    #填充序列
    d=c.fillna(method='ffill')
    #返回序列索引号和填充序列的差
    return c.index-d

#DATE获取日期
def WH_DATE(time_Series,time_Series2):
    #格式化时间为指定格式
    b=time_Series.apply(lambda x:time.strftime("%H:%M:%S",time.localtime(x/1e9)))
    c=time_Series2.apply(lambda x:time.strftime("%Y-%m-%d",time.localtime(x/1e9)))
    当前状态=''
    l=[]
    for x in range(len(b)):
        if x==0:
            if b.iloc[x]>="21:00:00":
                当前状态="第一天夜里"
            l.append(x)
        else:
            if b.iloc[x]=="21:00:00" :
                当前状态="第一天夜里"
                l.append(x)
            if b.iloc[x]=="09:00:00":
                if 当前状态=="第一天夜里":
                    当前状态="第二天白天"
                else:
                    当前状态="第二天白天"
                    l.append(x)
    d=pd.Series([ np.NaN for x in range(len(time_Series))])
    d[l]=c.tolist()[-len(l):]
    d=d.fillna(method='ffill')
    return d
def WH_转换为年月日UpdateTime(time_Series):
    return time_Series.apply(lambda x:time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(x/1e9)))



# api=tqsdk.TqApi()
# k=api.get_kline_serial("SHFE.rb2005",60*60)
# k1=api.get_kline_serial("SHFE.rb2005",60*60*24)
# print(WH_DATE(k.datetime,k1.datetime).tolist())
# api.close()
@numba.jit
def numba_hhv(data,N):
    newdata=np.full(len(N),0)
    for x in range(len(N)):
        temp=data[x]
        for y in range(N[x]):
            if data[x-y]>temp:
                temp=data[x-y]
        newdata[x]=temp
    return newdata


def WH_hhv(H,NN):
    NN=NN.fillna(1)
    data=H.to_numpy()
    N=NN.to_numpy()
    new_data=numba_hhv(data,N)
    return pd.Series(new_data)

@numba.jit
def numba_llv(data,N):
    newdata=np.full(len(N),0)
    for x in range(len(N)):
        temp=data[x]
        for y in range(N[x]):
            if data[x-y]<temp:
                temp=data[x-y]
        newdata[x]=temp
    return newdata


def WH_llv(H,NN):
    NN=NN.fillna(1)
    data=H.to_numpy()
    N=NN.to_numpy()
    new_data=numba_hhv(data,N)
    return pd.Series(new_data)

def WH_ref(H,NN):
    H.fillna(1)
    NN=NN.fillna(1)
    滑动计算集合=set(NN)
    H1=H.copy()
    for x in 滑动计算集合:
        索引=NN[NN==x].index
        H1[索引]=H.shift(int(x))[索引]
    return H1

# @numba.jit
# def numba_ref(data,N):
#     newdata=np.full(len(N),0.0)
#     for x in range(len(N)):
#         newdata[x]=data[x-N[x]]
#     return newdata
# def WH_ref(H,NN):
#     NN=NN.fillna(0)
#     data=H.to_numpy()
#     N=NN.to_numpy()
#     new_data=numba_ref(data,N)
#     return pd.Series(new_data)

# a=pd.Series([1,1,1,2,2,2,3,3,3,4,4,4])
# b=pd.Series([0,1,1,2,2,2,3,3,3,4,4,4])
# print(WH_ref(a,b))

#VALUEWHEN(COND,X) 当COND条件成立时，取X的当前值。如COND条件不成立，则取上一次COND条件成立时X的值。
def WH_VALUEWHEN(COND,X):
    b=COND[COND].index
    c=COND.where(COND,np.NaN)
    c[b]= X[b]
    d=c.fillna(method='ffill')
    return d

def WH_OPENMINUTE(time_Series,有无夜盘="有"):
    b=time_Series.apply(lambda x:time.strftime("%H:%M:%S",time.localtime(x/1e9)))
    if 有无夜盘=="有":
        真值序列索引= b[b=="21:00:00"].index
    else:
        真值序列索引= b[b=="9:00:00"].index
    d=pd.Series([ np.NaN for x in range(len(time_Series))])
    d[真值序列索引]=time_Series[真值序列索引]
    d=d.fillna(method='ffill')
    return (time_Series-d)/(1e9*60)

def WH_and(*args):
    return pd.Series( ( all(x) for x in zip(*args)))
def WH_or(*args):
    return pd.Series( ( any(x) for x in zip(*args)))
def WH_max(a,b):
    a=a.fillna(0)
    b=a.fillna(0)
    return tqsdk.tafunc.max(a,b)

def WH_信号间隔过滤(开多仓真值序列1,间隔):
    temp=-1
    l=[]
    for x,v in 开多仓真值序列1.items():
        if v:
            if temp==-1 or x-temp>间隔:
                l.append(True)
                temp=x
            else:
                l.append(False)
        else:
            l.append(False)
    return pd.Series(l)
# a=[1,0,0,1,1,0,0,0,0,1,1,1,0,1]
# a=pd.Series(a)
# print(WH_信号间隔过滤(a,2))


def WH_开平点计算持仓(开仓点列表,平仓点列表,方向):
    空值序列=pd.Series(np.full([len(开仓点列表[0])],np.NaN))
    #print(空值序列)
    for x in range(len(开仓点列表)):
        #print(开仓点列表[x])
        a索引=开仓点列表[x][开仓点列表[x]==1].index
        空值序列[a索引]=方向
    for x in range(len(平仓点列表)):
        a索引=平仓点列表[x][平仓点列表[x]==1].index
        空值序列[a索引]=0
    持仓矩阵=空值序列.fillna(method="ffill")
    # print(持仓矩阵)
    if 方向==1:
        开仓点矩阵=(持仓矩阵-持仓矩阵.shift(1))>0
        平仓点矩阵=(持仓矩阵-持仓矩阵.shift(1))<0
    else:
        开仓点矩阵=(持仓矩阵-持仓矩阵.shift(1))<0
        平仓点矩阵=(持仓矩阵-持仓矩阵.shift(1))>0

    return 空值序列.fillna(method="ffill"),开仓点矩阵,平仓点矩阵

def WH_算盈利(开仓点列表,平仓点列表,方向,开仓价格,平仓价格):
    持仓过程,a,b=WH_开平点计算持仓(开仓点列表,平仓点列表,方向)
    # print(250)
    a索引=a[a==1].index
    b索引=b[b==1].index
    c1=pd.Series(np.full((1,len(a)),np.NaN)[0])
    c1[a索引]= 开仓价格[a索引]
    开仓成本=c1.fillna(method='ffill')
    持仓过程[b索引]=方向
    return (平仓价格-开仓成本)*持仓过程

def WH_算开仓成本(开仓点列表,平仓点列表,方向,开仓价格):
    持仓过程,a,b=WH_开平点计算持仓(开仓点列表,平仓点列表,方向)
    hehe=开仓点列表[0]
    # print(len(hehe[hehe==1].index))
    # print("看看")
    # print(a)
    # print(b)
    a索引=a[a==1].index
    b索引=b[b==1].index
    c1=pd.Series(np.full((1,len(a)),np.NaN)[0])
    # print(开仓价格)
    c1[a索引]= 开仓价格[a索引]
    # print(250250250)
    开仓成本=c1.fillna(method='ffill')
    #持仓过程[b索引]=方向
    # print(开仓成本)
    return 开仓成本
def WH_WEEKDAY(time_Series,time_Series2,有无夜盘="有"):
    日期序列=WH_DATE(time_Series,time_Series2)
    # print(日期序列)
    日期序列=日期序列.fillna("1970-01-01")
    b=日期序列.apply(lambda x: (time.strptime(x,"%Y-%m-%d").tm_wday)+1)
    return b

def 算持仓(a,b):
    a索引=a[a==1].index
    b索引=b[b==1].index
    c=pd.Series(np.full((1,len(a)),np.NaN)[0])
    c[a索引]=1
    c[b索引]=0
    d=c.fillna(method="ffill")
    return d

def 算盈利(a,b,开盘价格):
    a索引=a[a==1].index
    b索引=b[b==1].index
    c1=pd.Series(np.full((1,len(a)),np.NaN)[0])
    d=算持仓(a,b)
    c1[a索引]= 开盘价格[a索引]
    开仓成本=c1.fillna(method='ffill')
    d[b索引]=1
    return (开盘价格-开仓成本)*d

def 算盈利_开平价格不同(a,b,开仓价格,平仓价格):
    a索引=a[a==1].index
    b索引=b[b==1].index
    c1=pd.Series(np.full((1,len(a)),np.NaN)[0])
    d=算持仓(a,b)
    c1[a索引]= 开仓价格[a索引]
    开仓成本=c1.fillna(method='ffill')
    d[b索引]=1
    return (平仓价格-开仓成本)*d
#平仓价格字典={"平仓点序列":[a,b,c],"平仓点价格序列":[a1,b1,c1]}
def WH_算平仓后盈利金额_开平价格不同_平仓价格多序列(a,b,开仓价格,平仓价格字典):
    a1=pd.Series(np.full([len(开仓价格)],np.NaN))
    b1=pd.Series(np.full([len(开仓价格)],np.NaN))
    a1[a]=1
    b1[b]=1
    a1=a1.fillna(0)
    b1=b1.fillna(0)

    b的真值索引=b[b==1].index
    平仓价格序列=pd.Series(np.full([len(开仓价格)],np.NaN))
    for x in range(len(平仓价格字典["平仓点序列"])):
        找到真值索引=平仓价格字典["平仓点序列"][x][平仓价格字典["平仓点序列"][x]==1].index
        交集序列索引=b的真值索引&找到真值索引
        平仓价格序列[交集序列索引]=平仓价格字典["平仓点价格序列"][x][交集序列索引]
    浮盈=算盈利_开平价格不同(a1,b1,开仓价格,平仓价格序列)
    b索引=b1[b1==1].index
    return 浮盈[b索引]

# def WH_算平仓后盈利金额(a,b,开仓价格,平仓价格):
#     a1=pd.Series(np.full([len(开仓价格)],np.NaN))
#     b1=pd.Series(np.full([len(开仓价格)],np.NaN))
#     a1[a]=1
#     b1[b]=1
#     a1=a1.fillna(0)
#     b1=b1.fillna(0)
#     浮盈=算盈利_开平价格不同(a1,b1,开仓价格,平仓价格)
#     b索引=b1[b1==1].index
#     return 浮盈[b索引]

def WH_算平仓后盈利金额(a,b,开盘价格):
    a1=pd.Series(np.full([len(开盘价格)],np.NaN))
    b1=pd.Series(np.full([len(开盘价格)],np.NaN))
    a1[a]=1
    b1[b]=1
    a1=a1.fillna(0)
    b1=b1.fillna(0)
    浮盈=算盈利(a1,b1,开盘价格)
    b索引=b1[b1==1].index
    return 浮盈[b索引]
def WH_融合多空开平点(买开点,卖平点,卖开点,买平点):
    总序列数=len(买开点)
    当前仓位=0
    新买开点=[]
    新卖平点=[]
    新卖开点=[]
    新买平点=[]
    for x in range(总序列数):
        if 当前仓位==0:
            if 买开点.iloc[x]:
                新买开点.append(1)
                新卖平点.append(0)
                新卖开点.append(0)
                新买平点.append(0)
                当前仓位=1
            elif 卖开点.iloc[x]:
                新买开点.append(0)
                新卖平点.append(0)
                新卖开点.append(1)
                新买平点.append(0)
                当前仓位=-1
            else:
                新买开点.append(0)
                新卖平点.append(0)
                新卖开点.append(0)
                新买平点.append(0)
        elif 当前仓位==1:
            if 卖平点.iloc[x]:
                新买开点.append(0)
                新卖平点.append(1)
                新卖开点.append(0)
                新买平点.append(0)   
                当前仓位=0    
            else:
                新买开点.append(0)
                新卖平点.append(0)
                新卖开点.append(0)
                新买平点.append(0)    
        elif 当前仓位==-1:
            if 卖平点.iloc[x]:
                新买开点.append(0)
                新卖平点.append(0)
                新卖开点.append(0)
                新买平点.append(1)   
                当前仓位=0    
            else:
                新买开点.append(0)
                新卖平点.append(0)
                新卖开点.append(0)
                新买平点.append(0)  
    return pd.Series(新买开点),pd.Series(新卖平点),pd.Series(新卖开点),pd.Series(新买平点)
def WH_盈利拼接(盈利1序列,盈利2序列,n):
    a=盈利1序列.index
    b=盈利2序列.index
    c=pd.Series(np.full([n],np.NaN))
    c[a]=盈利1序列
    c[b]=盈利2序列
    d=c[np.isnan(c)==False].index
    return c[d]

        


