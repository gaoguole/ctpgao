import tqsdk
import thostmduserapi as mdapi
import redis
import time
from tqsdk.tafunc import time_to_str
import os 
import zipfile

#可以改用任意期货公司的前置
行情地址="tcp://101.230.209.178:53313"


当前交易日=""
#继承了原来的行情API
class CFtdcMdSpi(mdapi.CThostFtdcMdSpi):
    def __init__(self,tapi,redis_con,symbol_list):
        #继承原来的init
        mdapi.CThostFtdcMdSpi.__init__(self)

        #设置api连接
        self.tapi=tapi
        self.subID=[x.split('.')[1] for x in symbol_list]
        self.red=redis_con

    #启动后默认调用连接
    def OnFrontConnected(self) -> "void":
        print ("OnFrontConnected")
        #设置一个登录结构体
        loginfield = mdapi.CThostFtdcReqUserLoginField()
        #对登录结构体进行传参
        loginfield.BrokerID="8000"
        loginfield.UserID="000005"
        loginfield.Password="123456"
        loginfield.UserProductInfo="python dll"
        #讲登录结构体传参，并且进行请求，调用请求函数
        self.tapi.ReqUserLogin(loginfield,0)
    
    #  登录回调
    def OnRspUserLogin(self, pRspUserLogin: 'CThostFtdcRspUserLoginField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool') -> "void":
        print (f"OnRspUserLogin, SessionID={pRspUserLogin.SessionID},ErrorID={pRspInfo.ErrorID},ErrorMsg={pRspInfo.ErrorMsg}")

        #订阅行情，根据文档，十分要注意，传入的一定是二进制的合约名,
        ret=self.tapi.SubscribeMarketData([id.encode('utf-8') for id in self.subID],len(self.subID))

    # 行情回调
    def OnRtnDepthMarketData(self, pDepthMarketData: 'CThostFtdcDepthMarketDataField') -> "void":
        global 当前交易日
        #print(pDepthMarketData)
        当前交易日=pDepthMarketData.TradingDay
        #print(pDepthMarketData.InstrumentID)
        l=[
        pDepthMarketData.InstrumentID,
        pDepthMarketData.UpdateTime,
        pDepthMarketData.UpdateMillisec,
        pDepthMarketData.TradingDay,
        pDepthMarketData.ActionDay,
        pDepthMarketData.LastPrice,
        pDepthMarketData.Volume,
        pDepthMarketData.AskPrice1,
        pDepthMarketData.AskVolume1,
        pDepthMarketData.BidPrice1,
        pDepthMarketData.BidVolume1,
        pDepthMarketData.OpenInterest,
        #PreSettlementPrice
        pDepthMarketData.PreSettlementPrice,
        #PreClosePrice
        pDepthMarketData.PreClosePrice,
        #PreOpenInterest
        pDepthMarketData.PreOpenInterest,
        #OpenPrice
        pDepthMarketData.OpenPrice,
        #HighestPrice
        pDepthMarketData.HighestPrice,
        #LowestPrice
        pDepthMarketData.LowestPrice,
        #Turnover
        pDepthMarketData.Turnover,
        #ClosePrice
        pDepthMarketData.ClosePrice,
        #SettlementPrice
        pDepthMarketData.SettlementPrice,
        #UpperLimitPrice
        pDepthMarketData.UpperLimitPrice,
        #LowerLimitPrice
        pDepthMarketData.LowerLimitPrice,
        # ///BidPrice2
        pDepthMarketData.BidPrice2,
        # ///BidVolume2
        pDepthMarketData.BidVolume2,
        # ///AskPrice2
        pDepthMarketData.AskPrice2,
        # ///AskVolume2
        pDepthMarketData.AskVolume2,
        # ///BidPrice3
        pDepthMarketData.BidPrice3,
        # ///BidVolume3
        pDepthMarketData.BidVolume3,
        # ///AskPrice3
        pDepthMarketData.AskPrice3,
        # ///AskVolume3
        pDepthMarketData.AskVolume3,
        # ///BidPrice4
        pDepthMarketData.BidPrice4,
        # ///BidVolume4
        pDepthMarketData.BidVolume4,
        # ///AskPrice4
        pDepthMarketData.AskPrice4,
        # ///AskVolume4
        pDepthMarketData.AskVolume4,
        # ///BidPrice5
        pDepthMarketData.BidPrice5,
        # ///BidVolume5
        pDepthMarketData.BidVolume5,
        # ///AskPrice5
        pDepthMarketData.AskPrice5,
        # ///AskVolume5
        pDepthMarketData.AskVolume5,
        # ///AveragePrice
        pDepthMarketData.AveragePrice,
        ]
        self.red.lpush(pDepthMarketData.InstrumentID,','.join( [str(x) for x in l ]))

    #订阅行情后的订阅成功与否的回报
    def OnRspSubMarketData(self, pSpecificInstrument: 'CThostFtdcSpecificInstrumentField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool') -> "void":
        pass
def 转存(red):
    a=red.keys("*")
    当天日期="".join(time_to_str(time.time())[:10].split("-"))
    if 当天日期 !=当前交易日:
        return
    if 当天日期 not in os.listdir():
        os.mkdir(当天日期)

    #循环遍历写一下
    for x in a:
        x=x.decode()
        b= red.lrange(x,0,-1)[::-1]
        f=open(".\\"+当天日期+"\\"+x+".csv",'w')
        f.write("\n".join([ y.decode() for y in b ]))
        f.close()
        red.delete(x)

    az=zipfile.ZipFile(".\\"+当天日期+'.zip', mode='w', compression=0, allowZip64=True, compresslevel=None)
    for name_file in os.listdir(".\\"+当天日期): 
        az.write(filename=".\\"+当天日期+"\\"+name_file,compress_type=zipfile.ZIP_BZIP2,compresslevel=None)
        os.remove(".\\"+当天日期+"\\"+name_file)
    az.close()


def 启动():
    while True:
        #获取合约
        api=tqsdk.TqApi(auth="270829786@qq.com,24729220a")
        合约=api.query_quotes(ins_class="FUTURE",expired=False)
        api.close()

        #建立一个原版行情API实例
        mduserapi=mdapi.CThostFtdcMdApi_CreateFtdcMdApi()
        #建立一个自定义的行情类()的连接实例
        red=redis.Redis(db=14)
        mduserspi=CFtdcMdSpi(mduserapi,red,合约)
        #为连接实例设置连接地址
        mduserapi.RegisterFront(行情地址)
        mduserapi.RegisterSpi(mduserspi)
        #启动连接线程
        mduserapi.Init()    
        #进行线程阻塞
        while True:
            if time_to_str(time.time())[11:16]=="15:30":
                转存(red)
                mduserapi.Release()
                while True:
                    time.sleep(10)
                    if time_to_str(time.time())[11:16]=="20:30":
                        break
                break
            time.sleep(10)

if __name__ == "__main__":
    启动()