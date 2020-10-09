import tqsdk
import thostmduserapi as mdapi
import redis
import whfunc
import tqsdk
import re 
import time
import queue
from tqsdk.tafunc import time_to_str
from tq_md_objs import TQ_md_class,CFtdcMdSpi
import schedule


def 启动行情记录2(symbol_list,list_duration_seconds,行情类型="TQ",redis_con=None,天勤连接=None,通达信连接=None,data_length=2000,行情地址="tcp://101.230.209.178:53313",md_subscription_name="gaoctp"):
    存储tick队列=queue.Queue()
    if not redis_con:
        redis_con=redis.Redis(db=15)


    if not 天勤连接:
        天勤连接=tqsdk.TqApi(auth="270829786@qq.com,24729220a")

    if 行情类型=="TQ":
        单symbol_dict=set()
        需要订阅的CTPsymbol=[]
        订阅中的主连合约={}
        订阅中的主连合约_反向={}
        订阅中的指数合约={}
        天勤需要添加的合约=[]
        for x in symbol_list:
            if "KQ.m" in x:
                订阅中的主连合约[x]=天勤连接.get_quote(x).underlying_symbol
                需要订阅的CTPsymbol.append(订阅中的主连合约[x])
                天勤需要添加的合约.append(订阅中的主连合约[x])
            elif "KQ.i" in x:
                symbol=x.split("@")[1]
                订阅中的指数合约[x]=  [ y for y in 天勤连接._data['quotes']  if  天勤连接._data['quotes'][y]["expired"]==False  and   re.findall("^"+symbol+"[0-9]{3,4}$",y)  ] 
                需要订阅的CTPsymbol+=订阅中的指数合约[x]
                天勤需要添加的合约+=订阅中的指数合约[x]
                list_duration_seconds.append(86400)
            else:
                需要订阅的CTPsymbol.append(x)
                单symbol_dict.add(x)

        symbol_list= list(set(symbol_list+天勤需要添加的合约))

        订阅中的主连合约_反向={ 订阅中的主连合约[x]:x for x in 订阅中的主连合约}
        list_duration_seconds=list(set(list_duration_seconds))
        天勤=TQ_md_class(symbol_list,list_duration_seconds,天勤连接,redis_con,data_length,md_subscription_name)

    #建立一个原版行情API实例
    mduserapi=mdapi.CThostFtdcMdApi_CreateFtdcMdApi()
    #建立一个自定义的行情类()的连接实例
    mduserspi=CFtdcMdSpi(mduserapi,存储tick队列,需要订阅的CTPsymbol)
    #为连接实例设置连接地址
    mduserapi.RegisterFront(行情地址)
    mduserapi.RegisterSpi(mduserspi)
    #启动连接线程
    mduserapi.Init()    
    #进行线程阻塞

    单symbol_dict映射={ x.split('.')[1]:x for x in 需要订阅的CTPsymbol}
    while True:
        try:
            symbol,UpdateTime, UpdateMillisec, TradingDay,ActionDay,LastPrice, Volume, AskPrice1,AskVolume1, BidPrice1,BidVolume1,OpenInterest,\
                PreSettlementPrice,PreClosePrice, PreOpenInterest,OpenPrice,HighestPrice,LowestPrice,Turnover,ClosePrice,SettlementPrice,UpperLimitPrice,LowerLimitPrice,BidPrice2,BidVolume2,AskPrice2,AskVolume2,\
                    BidPrice3,BidVolume3,AskPrice3,AskVolume3,BidPrice4,BidVolume4,AskPrice4,AskVolume4,BidPrice5,BidVolume5,AskPrice5,AskVolume5,AveragePrice=存储tick队列.get(timeout=60)
        except:
            if time_to_str(time.time())[11:13] in ("16","03"):
                break
            continue
        if 行情类型=="TQ":
            #处理本条data
            天勤.updata(单symbol_dict映射[symbol],UpdateTime, UpdateMillisec, TradingDay,ActionDay,LastPrice, Volume, AskPrice1,AskVolume1, BidPrice1,BidVolume1,OpenInterest,PreSettlementPrice,PreClosePrice, PreOpenInterest,OpenPrice,HighestPrice,LowestPrice,Turnover,ClosePrice,SettlementPrice,UpperLimitPrice,LowerLimitPrice,BidPrice2,BidVolume2,AskPrice2,AskVolume2,BidPrice3,BidVolume3,AskPrice3,AskVolume3,BidPrice4,BidVolume4,AskPrice4,AskVolume4,BidPrice5,BidVolume5,AskPrice5,AskVolume5,AveragePrice)

            #处理主连
            if 单symbol_dict映射[symbol] in  订阅中的主连合约_反向:
                天勤.updata(订阅中的主连合约_反向[单symbol_dict映射[symbol]],UpdateTime, UpdateMillisec, TradingDay,ActionDay,LastPrice, Volume, AskPrice1,AskVolume1, BidPrice1,BidVolume1,OpenInterest,PreSettlementPrice,PreClosePrice, PreOpenInterest,OpenPrice,HighestPrice,LowestPrice,Turnover,ClosePrice,SettlementPrice,UpperLimitPrice,LowerLimitPrice,BidPrice2,BidVolume2,AskPrice2,AskVolume2,BidPrice3,BidVolume3,AskPrice3,AskVolume3,BidPrice4,BidVolume4,AskPrice4,AskVolume4,BidPrice5,BidVolume5,AskPrice5,AskVolume5,AveragePrice)



            #如果处理data后,为空列表,change_i_data
            if 存储tick队列.empty():
                time.sleep(0.003)
                if 存储tick队列.empty():
                    #print(time_to_str(time.time()))
                    天勤.change_i_data(订阅中的指数合约)
                     #整体处理后,全部推送
                    天勤.all_push()
                else:
                    #print("我错了")
                    pass


def 启动行情记录(symbol_list,list_duration_seconds,行情类型="TQ",redis_con=None,天勤连接=None,通达信连接=None,data_length=2000,行情地址="tcp://101.230.209.178:53313",md_subscription_name="gaoctp"):


    schedule.every().day.at("08:30").do(启动行情记录2,symbol_list,list_duration_seconds,行情类型,redis_con,天勤连接,通达信连接,data_length,行情地址,md_subscription_name)
    schedule.every().day.at("20:30").do(启动行情记录2,symbol_list,list_duration_seconds,行情类型,redis_con,天勤连接,通达信连接,data_length,行情地址,md_subscription_name)
    当前时间=time_to_str(time.time())[11:16]
    if 当前时间>"20:30" or "00:00"<当前时间<"02:30" or "08:30"<当前时间<"15:30":
        启动行情记录2(symbol_list,list_duration_seconds,行情类型,redis_con,天勤连接,通达信连接,data_length,行情地址,md_subscription_name)


    while True:

        # 启动服务

        schedule.run_pending()

        time.sleep(1)
    #mduserapi.Join()



    



