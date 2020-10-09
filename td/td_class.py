# -*- coding: utf-8 -*-
import thosttraderapi as api
import janus
import asyncio
import time
import queue
import numpy
import whfunc
import websockets
import traceback
from 交易依赖类 import CTradeSpi,order_insert,order_close,ReqQryTradingAccount,ReqQryInvestorPosition
web_CTP={}
async def 交易后端连接(websocket, path):
    global CTP用户名对应实例和登陆名,web_CTP
    #客户端服务端消息
    while True:
        try:
            recv_str = await websocket.recv()
        except:
            CTP用户名对应实例和登陆名[web_CTP[websocket][0]]["td_websockets"].remove(websocket)
            break

        try:
            recv_str=eval(recv_str)
        except:
            print("异常",recv_str)
        a=""
        #处理连接
        if websocket not in web_CTP:
            for x in CTP用户名对应实例和登陆名:
                for y in CTP用户名对应实例和登陆名[x]["td_user_pass"]:
                    try:
                        #登陆成功,设置权限,和添加通知,还有发送当前数据
                        print(y[:2])
                        if [recv_str['user'],recv_str['password']]==y[:2]:
                            web_CTP[websocket]=[x,y[2]]
                            print("position",CTP用户名对应实例和登陆名[x]["td_obj"].position)
                            print("order",CTP用户名对应实例和登陆名[x]["td_obj"].order)
                            await websocket.send(str({"position":CTP用户名对应实例和登陆名[x]["td_obj"].position,"order":CTP用户名对应实例和登陆名[x]["td_obj"].order,
                            "trade":CTP用户名对应实例和登陆名[x]["td_obj"].trade,"account":CTP用户名对应实例和登陆名[x]["td_obj"].account,
                            }))
                            CTP用户名对应实例和登陆名[x]["td_websockets"].append(websocket)
                            a=1
                            break
                    except:
                        print("异常2",recv_str)
                        traceback.print_exc()
                if a:
                    break
                
            else:
                print(recv_str)
                await websocket.send(str({"msg":"逗呢,账户密码不对"}))


        else:
            try:
                if recv_str["rq"]=="order_insert":
                    if web_CTP[websocket][1]!=0:
                        await 发消息(websocket,{"msg":"报单权限不足","md5":recv_str["md5"]})
                    else:
                        实例=CTP用户名对应实例和登陆名[web_CTP[websocket][0]]["td_obj"]
                        order_insert(实例,recv_str["md5"],recv_str["symbol"],recv_str["direction"],recv_str["offset"],recv_str["volume"],recv_str["price"],recv_str["advanced"])

                if recv_str["rq"]=="order_close":
                    if web_CTP[websocket][1]!=0:
                        await 发消息(websocket,{"msg":"撤单权限不足","md5":recv_str["md5"]})
                    else:
                        实例=CTP用户名对应实例和登陆名[web_CTP[websocket][0]]["td_obj"]
                        order_close(实例,recv_str["md5"])
            except:
                traceback.print_exc()
                print("异常3",recv_str)




async def 发消息(socket,data):
    await socket.send(str(data))
async def 查询资金(user,n):
    global CTP用户名对应实例和登陆名,查询刷新
    print(n,"运行查资金")
    await asyncio.sleep(n)
    while True:
        ReqQryTradingAccount(CTP用户名对应实例和登陆名[user]["td_obj"])
        查询刷新=1
        await asyncio.sleep(1.5)
        if 查询刷新:
            break
    ReqQryInvestorPosition(CTP用户名对应实例和登陆名[user]["td_obj"])


async def 推送交易端消息():
    global CTP用户名对应实例和登陆名,loop,myqueue,查询刷新
    最近查询时间={}
    当前状态={}
    while True:
        data= await myqueue.async_q.get()
        data_copy=data.copy()
        data_copy.pop("user")
        for x in CTP用户名对应实例和登陆名[data["user"]]["td_websockets"]:
            loop.create_task(发消息(x,data_copy))

        #判断最近查询时间间隔,创建查询任务
        if "account" not in data:
            if 当前状态[data["user"]]=="无任务":
                if time.time()-最近查询时间[data["user"]]>2:
                    loop.create_task(查询资金(data["user"],0))
                else:
                    loop.create_task(查询资金(data["user"],2-(time.time()-最近查询时间[data["user"]])))
                当前状态[data["user"]]="有任务"
            else:
                查询刷新=0

        else:
            最近查询时间[data["user"]]=time.time()
            当前状态[data["user"]]="无任务"


async def 启动程序(账户data):
    global CTP用户名对应实例和登陆名,myqueue
    CTP用户名对应实例和登陆名={}
    myqueue=janus.Queue()
    #每个CTP账户实例遍历
    for x in 账户data:
        if 账户data[x] is None:
            账户data[x]=[list(x[1:3])+[0]]

        #登陆交易实例
        tradeapi=api.CThostFtdcTraderApi_CreateFtdcTraderApi()
        BROKERID,USERID,PASSWORD,AppID,AuthCode,FrontAddr=x
        tradespi=CTradeSpi(tradeapi,BROKERID,USERID,PASSWORD,AppID,AuthCode,myqueue.sync_q)
        tradeapi.RegisterFront(FrontAddr)
        tradeapi.RegisterSpi(tradespi)
        tradeapi.SubscribePrivateTopic(api.THOST_TERT_RESTART)
        tradeapi.SubscribePublicTopic(api.THOST_TERT_RESTART)
        tradeapi.Init()
        while True:
            if tradespi.init_start is None:
                time.sleep(1)
            else:
                break


        CTP用户名对应实例和登陆名[x[1]]={"td_obj":tradespi,'td_user_pass':账户data[x],"td_websockets":[]}
    print(CTP用户名对应实例和登陆名)
    while True:
        await asyncio.sleep(1000)

def 启动交易账户登录(账户data,绑定通信地址="localhost",绑定通信端口=8765):
    global loop,myqueue
    loop=asyncio.get_event_loop()
    start_server = websockets.serve(交易后端连接, 绑定通信地址, 绑定通信端口)
    loop.run_until_complete(start_server)
    loop.create_task(启动程序(账户data))
    loop.create_task(推送交易端消息())
    loop.run_forever()
