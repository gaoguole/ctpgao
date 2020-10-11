import pandas as pd 
import json
import redis
from tqsdk.tafunc import time_to_str,ma,crossup
import time
import numpy
import asyncio
import aioredis
import whfunc
import websockets
import uuid
import copy
from threading import Thread
import sys 
from PyQt5 import QtWidgets
import sys
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton
from PyQt5.QtWidgets import QApplication,QWidget,QPushButton,QMessageBox,QLineEdit,QHBoxLayout,QVBoxLayout,QGridLayout,QInputDialog,QLabel,QTextBrowser,QColorDialog,QFontDialog
class MyDict(dict):
    __setattr__ = dict.__setitem__
    __getattr__ = dict.__getitem__

class  新界面(QtWidgets.QMainWindow):

    def __init__(self,对象):
        super().__init__()
        self.api=对象
        self.create_ui()
    
    def create_ui(self):
        布局=QHBoxLayout()
        左侧总布局=QVBoxLayout()
        左侧上方布局=QGridLayout()
        左侧下方布局=QHBoxLayout()

# #-------------设置左侧页面内容--------------------
#         self.左上提示框文字提示=["序号","账户","备注","状态","倍率","权益","可用资金","资金使用率"]
#         self.左侧提示框列表=[]
#         self.全选缓存=False
#         for x in range(1,len(self.左上提示框文字提示)+1):
#             self.左侧提示框列表.append(标签(self.左上提示框文字提示[x-1]))
#             左侧上方布局.addWidget(self.左侧提示框列表[-1],0,x)
#         self.复选框组件列表=[]
#         for x in range(26):
#             self.复选框组件列表.append(复选框())
#             左侧上方布局.addWidget(self.复选框组件列表[-1],x,0)

#         self.序号标签列表=[]
#         self.账户标签列表=[]
#         self.备注标签列表=[]
#         self.状态标签列表=[]
#         self.倍率标签列表=[]
#         self.权益标签列表=[]
#         self.可用资金标签列表=[]
#         self.资金使用率标签列表=[]

#         for x in range(len(data)):
#             self.序号标签列表.append(标签(str(x+1)))
#             左侧上方布局.addWidget(self.序号标签列表[-1],x+1,1)

#             self.账户标签列表.append(标签(data[x]["账户"]))
#             左侧上方布局.addWidget(self.账户标签列表[-1],x+1,2)

#             self.备注标签列表.append(标签(data[x]["账户备注"]))
#             左侧上方布局.addWidget(self.备注标签列表[-1],x+1,3)

#             self.状态标签列表.append(标签("未登录"))
#             左侧上方布局.addWidget(self.状态标签列表[-1],x+1,4)

#             临时输入框=文本输入框()
#             临时输入框.resize(100,60)
#             临时输入框.设置文本(str(data[x]["倍率"]))
#             临时输入框.setMaxLength(3)


#             self.倍率标签列表.append(临时输入框)
#             左侧上方布局.addWidget(临时输入框,x+1,5)


#             self.权益标签列表.append(标签("    "))
#             左侧上方布局.addWidget(self.权益标签列表[-1],x+1,6)
            
#             self.可用资金标签列表.append(标签("    "))
#             左侧上方布局.addWidget(self.可用资金标签列表[-1],x+1,7)

#             self.资金使用率标签列表.append(标签("    "))
#             左侧上方布局.addWidget(self.资金使用率标签列表[-1],x+1,8)
#         for x in range(1,8):
#             if x!=5:
#                 左侧上方布局.setColumnStretch(x,1)

#         # 左侧下方布局.addWidget(标签("提示: 如果有问题,叫我哈"))
#         左侧总布局.添加布局(左侧上方布局)
# #        左侧总布局.添加布局(左侧下方布局)
        

        右侧总布局=QGridLayout()
        右侧上方布局=右侧总布局
#-------------设置右侧页面内容--------------------



# #--------------------------------持仓页面----------------------------------------
#         #当前页提示, 当前页 ,   查持仓按钮,查委托, 查成交 ,进行下单,  下条件单
#         self.右侧提示组件=[]
#         self.当前页="持仓页"
#         self.右侧提示组件.append(标签("当前页:"))
#         右侧上方布局.addWidget(self.右侧提示组件[-1],0,0)

#         self.右侧提示组件.append(标签(self.当前页))
#         右侧上方布局.addWidget(self.右侧提示组件[-1],0,1)

#         self.右侧提示组件.append(标签("功能提示按钮->"))
#         右侧上方布局.addWidget(self.右侧提示组件[-1],0,2)

#         self.右侧提示组件.append(下按按钮("查持仓"))
#         self.右侧提示组件[3].设置点击事件(self.chi_cang)
#         右侧上方布局.addWidget(self.右侧提示组件[-1],0,3)

#         self.右侧提示组件.append(下按按钮("查委托"))
#         self.右侧提示组件[4].设置点击事件(self.wei_tuo)
#         右侧上方布局.addWidget(self.右侧提示组件[-1],0,4)

#         self.右侧提示组件.append(下按按钮("查成交"))
#         self.右侧提示组件[5].设置点击事件(self.cheng_jiao)
#         右侧上方布局.addWidget(self.右侧提示组件[-1],0,5)

#         self.右侧提示组件.append(下按按钮("下普通单"))
#         self.右侧提示组件[6].设置点击事件(self.pu_tong_xia_dan)
#         右侧上方布局.addWidget(self.右侧提示组件[-1],0,6)

#         self.右侧提示组件.append(下按按钮("下条件单"))
#         self.右侧提示组件[7].设置点击事件(self.tiao_jian_xia_dan)
#         右侧上方布局.addWidget(self.右侧提示组件[-1],0,7)

#         右侧上方布局.addWidget(标签("作者:QQ270829786"),0,8)
#         右侧上方布局.addWidget(标签("微信:gaoguoleqqsanguo"),0,9)


#         self.右侧持仓数据=设置数据层次(26,8)
#         self.右侧持仓数据.setHorizontalHeaderLabels(["品种","合约","多总仓","空总仓","多可用","空可用 ","多成本","空成本"])
#         self.持仓表格=表格()
#         self.持仓表格.设置模型(self.右侧持仓数据)
#         self.持仓表格.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
#         #self.持仓表格.horizontalHeader().setSectionResizeMode(0, QHeaderView.Interactive)
#         右侧上方布局.addWidget(self.持仓表格,1,0,34,11)

#         self.持仓表格.setVisible(False)

 

# #------------------------查询委托页面---------------------------------------------
#         self.可撤单标签=QLabel("是否显示未成交单")
#         右侧上方布局.addWidget(self.可撤单标签,1,0)
#         self.可撤单复选框=QCheckBox()
#         右侧上方布局.addWidget(self.可撤单复选框,1,1)

#         self.右侧报单数据=设置数据层次(26,11)
#         self.右侧报单数据.setHorizontalHeaderLabels(["时间","合约","状态","买卖","开平","委托价","委托量 ","可撤量","已成交",'id',"是否撤单"])
#         self.报单表格=表格()
#         self.报单表格.设置模型(self.右侧报单数据)
#         self.报单表格.setColumnWidth(0, 240)
#         self.报单表格.setColumnWidth(1, 80)
#         self.报单表格.setColumnWidth(2, 80)
#         self.报单表格.setColumnWidth(3, 80)
#         self.报单表格.setColumnWidth(4, 80)
#         self.报单表格.setColumnWidth(5, 80)
#         self.报单表格.setColumnWidth(6, 80)
#         self.报单表格.setColumnWidth(7, 80)
#         self.报单表格.setColumnWidth(8, 80)
#         self.报单表格.setColumnWidth(9, 240)
#         self.报单表格.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
#         self.报单表格.clicked.connect(self.gao)
#         右侧上方布局.addWidget(self.报单表格,2,0,33,11)


#         self.可撤单标签.setVisible(False)
#         self.可撤单复选框.setVisible(False)
#         self.报单表格.setVisible(False)


# #---------------------------查询成交页面-----------------------------
#         self.右侧成交数据=设置数据层次(26,8)
#         self.右侧成交数据.setHorizontalHeaderLabels(["时间","报单id","成交id","合约代码","买卖","开平","成交价格","成交量"])
#         self.成交表格=表格()
#         self.成交表格.设置模型(self.右侧成交数据)
#         #self.成交表格.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
#         #self.成交表格.horizontalHeader().setStretchLastSection(True)
#         #self.成交表格.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)

#         self.成交表格.setColumnWidth(0, 240)
#         self.成交表格.setColumnWidth(1, 240)
#         self.成交表格.setColumnWidth(2, 240)
#         self.成交表格.setColumnWidth(3, 80)
#         self.成交表格.setColumnWidth(4, 80)
#         self.成交表格.setColumnWidth(5, 80)
#         self.成交表格.setColumnWidth(6, 80)
#         self.成交表格.setColumnWidth(7, 80)
#         self.成交表格.setColumnWidth(8, 80)
#         self.成交表格.setVisible(False)

#         右侧上方布局.addWidget(self.成交表格,1,0,34,11)

#         #右侧下方布局.addWidget(标签("提示: 如果有问题,叫我哈"))

# #-------------------------普通下单界面----------------
#         self.下单界面控件列表=[]
#         self.下单界面控件列表.append(标签("下单品种"))
#         右侧上方布局.addWidget(self.下单界面控件列表[-1],1,0)
#         self.下单界面控件列表.append(文本输入框())
#         右侧上方布局.addWidget(self.下单界面控件列表[-1],1,1)

#         # self.下单界面控件列表.append(标签("BUY/SELL"))
#         self.下单界面控件列表.append(QButtonGroup())
#         self.下单界面控件列表.append(QRadioButton("买"))
#         self.下单界面控件列表.append(QRadioButton("卖"))
#         self.下单界面控件列表[-3].添加按钮(self.下单界面控件列表[-1],0)
#         self.下单界面控件列表[-3].添加按钮(self.下单界面控件列表[-2],1)

#         右侧上方布局.addWidget(self.下单界面控件列表[-2],2,0)
#         右侧上方布局.addWidget(self.下单界面控件列表[-1],2,1)

#         self.下单界面控件列表.append(QButtonGroup())
#         self.下单界面控件列表.append(QRadioButton("开仓"))
#         self.下单界面控件列表.append(QRadioButton("平仓"))

#         self.下单界面控件列表[-3].添加按钮(self.下单界面控件列表[-1],0)
#         self.下单界面控件列表[-3].添加按钮(self.下单界面控件列表[-2],1)

#         右侧上方布局.addWidget(self.下单界面控件列表[-2],3,0)
#         右侧上方布局.addWidget(self.下单界面控件列表[-1],3,1)

#         self.下单界面控件列表.append(QButtonGroup())
#         self.下单界面控件列表.append(QRadioButton("市价追单"))
#         self.下单界面控件列表.append(QRadioButton("限价挂单"))
#         self.下单界面控件列表.append(文本输入框())
#         右侧上方布局.addWidget(self.下单界面控件列表[-3],4,0)
#         右侧上方布局.addWidget(self.下单界面控件列表[-2],4,1)
#         右侧上方布局.addWidget(self.下单界面控件列表[-1],4,2)
#         self.下单界面控件列表.append(标签("下单手数"))
#         self.下单界面控件列表.append(文本输入框())
#         右侧上方布局.addWidget(self.下单界面控件列表[-2],5,0)
#         右侧上方布局.addWidget(self.下单界面控件列表[-1],5,1)
#         self.下单界面控件列表.append(下按按钮("确认下单"))
#         self.下单界面控件列表[-1].设置点击事件(self.xia_dan)
#         右侧上方布局.addWidget(self.下单界面控件列表[-1],6,1)

#         for x in self.下单界面控件列表:
#             if str(type(x))!="<class '绘图.按钮组'>":
#                 x.setVisible(False)

# #--------------------条件下单界面---------------------------
#         self.条件单控件列表=[]
#         self.条件单控件列表=[]
#         self.条件单控件列表.append(标签("下单品种"))
#         右侧上方布局.addWidget(self.条件单控件列表[-1],1,0)
#         self.条件单控件列表.append(文本输入框())
#         右侧上方布局.addWidget(self.条件单控件列表[-1],1,1)

#         # self.下单界面控件列表.append(标签("BUY/SELL"))
#         self.条件单控件列表.append(QButtonGroup())
#         self.条件单控件列表.append(QRadioButton("买"))
#         self.条件单控件列表.append(QRadioButton("卖"))
#         self.条件单控件列表[-3].添加按钮(self.条件单控件列表[-1],0)
#         self.条件单控件列表[-3].添加按钮(self.条件单控件列表[-2],1)

#         右侧上方布局.addWidget(self.条件单控件列表[-2],2,0)
#         右侧上方布局.addWidget(self.条件单控件列表[-1],2,1)

#         self.条件单控件列表.append(QButtonGroup())
#         self.条件单控件列表.append(QRadioButton("开仓"))
#         self.条件单控件列表.append(QRadioButton("平仓"))

#         self.条件单控件列表[-3].添加按钮(self.条件单控件列表[-1],0)
#         self.条件单控件列表[-3].添加按钮(self.条件单控件列表[-2],1)

#         右侧上方布局.addWidget(self.条件单控件列表[-2],3,0)
#         右侧上方布局.addWidget(self.条件单控件列表[-1],3,1)

#         self.条件单控件列表.append(QButtonGroup())
#         self.条件单控件列表.append(QRadioButton("市价追单"))
#         self.条件单控件列表.append(QRadioButton("限价挂单"))

#         self.条件单控件列表[-3].添加按钮(self.条件单控件列表[-1],0)
#         self.条件单控件列表[-3].添加按钮(self.条件单控件列表[-2],1)

#         self.条件单控件列表.append(文本输入框())
#         右侧上方布局.addWidget(self.条件单控件列表[-3],4,0)
#         右侧上方布局.addWidget(self.条件单控件列表[-2],4,1)
#         右侧上方布局.addWidget(self.条件单控件列表[-1],4,2)
#         self.条件单控件列表.append(标签("下单手数"))
#         self.条件单控件列表.append(文本输入框())
#         右侧上方布局.addWidget(self.条件单控件列表[-2],5,0)
#         右侧上方布局.addWidget(self.条件单控件列表[-1],5,1)
#         # self.下单界面控件列表.append(下按按钮("确认下单"))
#         # 右侧上方布局.addWidget(self.下单界面控件列表[-1],6,1)   

#         self.条件单控件列表.append(标签("最新价格"))
#         self.条件单控件列表.append(QButtonGroup())
#         self.条件单控件列表.append(QRadioButton("大于等于"))
#         self.条件单控件列表.append(QRadioButton("小于等于"))
#         self.条件单控件列表[-3].添加按钮(self.条件单控件列表[-1],0)
#         self.条件单控件列表[-3].添加按钮(self.条件单控件列表[-2],1)
#         self.条件单控件列表.append(文本输入框())
#         右侧上方布局.addWidget(self.条件单控件列表[-5],6,0)
#         右侧上方布局.addWidget(self.条件单控件列表[-3],6,1)
#         右侧上方布局.addWidget(self.条件单控件列表[-2],6,2)
#         右侧上方布局.addWidget(self.条件单控件列表[-1],6,3)

#         self.条件单控件列表.append(下按按钮("确认下单"))
#         self.条件单控件列表[-1].设置点击事件(self.tiao_jian)
#         右侧上方布局.addWidget(self.条件单控件列表[-1],7,1)    
#         self.右侧条件单数据=设置数据层次(25,10)
#         self.右侧条件单数据.setHorizontalHeaderLabels(["时间","合约","买卖","开平","委托价","委托量 ","条件内容","是否触发",'id',"是否删除"])
#         self.条件报单表格=表格()
#         self.条件报单表格.设置模型(self.右侧条件单数据)
#         #self.条件报单表格.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
#         #self.持仓表格.horizontalHeader().setSectionResizeMode(0, QHeaderView.Interactive)

#         self.条件报单表格.setColumnWidth(0, 240)
#         self.条件报单表格.setColumnWidth(1, 80)
#         self.条件报单表格.setColumnWidth(2, 80)
#         self.条件报单表格.setColumnWidth(3, 80)
#         self.条件报单表格.setColumnWidth(4, 80)
#         self.条件报单表格.setColumnWidth(5, 80)
#         self.条件报单表格.setColumnWidth(6, 120)
#         self.条件报单表格.setColumnWidth(7, 80)
#         self.条件报单表格.setColumnWidth(8, 80)
#         self.条件单控件列表.append(self.条件报单表格)
#         self.条件报单表格.clicked.connect(self.gao2)
#         右侧上方布局.addWidget(self.条件报单表格,8,0,26,11)
#         for x in self.条件单控件列表:
#             if str(type(x))!="<class '绘图.按钮组'>":
#                 x.setVisible(False)
#         右侧上方布局.setColumnStretch(0,2)
#         布局.添加布局(左侧总布局)
#         布局.添加布局(右侧总布局)
#         self.setLayout(布局)



class my_UI(Thread):
    def __init__(self,对象):
        super().__init__()
        self.data=对象
    def run(self):
        app = QtWidgets.QApplication(sys.argv)
        ui = 新界面(self.data)
        ui.show()
        sys.exit(app.exec_())

day="day"
night="night"





class  GaoApi(object):
    
    def __init__(self,user=None,password=None,loop=None,订阅推送="gaoctp",data库连接地址="localhost",端口=6379,
    data库数字=15,data库密码=None,交易连接地址='ws://127.0.0.1:8765',Gui=False):
        self.data库连接地址=data库连接地址
        self.端口=端口
        self.data库数字=data库数字
        self.data库密码=data库密码
        self.red=redis.Redis(host=data库连接地址, port=端口,db=data库数字,password=data库密码)
        if loop:
            self.loop=loop
        else:
            self.loop=asyncio.SelectorEventLoop() 
        #判断对象是否改变
        self.cache_obj={}
        self.订阅推送=订阅推送
        self._整体行情更新={}
        self._单品行情更新={}
        self.增加任务=[]
        self.异步连接=None

        self.user=user
        self.password=password
        
        self.connect_path=交易连接地址
        #持仓
        self.position={}
        #报单
        self.order={}
        #成交
        self.trade={}
        #资金
        self.account=MyDict()

        self.my_websocket=""
        print(5555)
        self.loop.create_task(self.connect_rd())
        if Gui:
            ui=my_UI(self)
            ui.start()
            
        self.loop.run_forever()

    async def  connect_rd(self):
        async with websockets.connect(self.connect_path) as websocket:
            self.my_websocket=websocket
            await websocket.send(str({"user":self.user,"password":self.password}))
            while True:
                a=await websocket.recv()
                a=eval(a.replace("nan","numpy.nan").replace("1.7976931348623157e+308","numpy.nan"))
                #print(a)
                for x in a:
                    if x=="msg":
                        print(a["msg"])
                    if x=="order":
                        if not a[x]:
                            self.order.clear()
                            continue
                        for y in a[x]:
                            if y not in self.order:
                                self.order[y]=MyDict(a[x][y])
                            else:
                                self.order[y].update(a[x][y])
                        # self.order.update(a[x])
                    if x=="trade":
                        if not a[x]:
                            self.trade.clear()
                            continue
                        for y in a[x]:
                            if y not in self.trade:
                                self.trade[y]=MyDict(a[x][y])
                            else:
                                self.trade[y].update(a[x][y])
                    if x=="position":
                        for y in a[x]:
                            if y not in self.position:
                                self.position[y]=MyDict(a[x][y])
                            else:
                                self.position[y].update(a[x][y])
                        # self.position.update(a[x])
                    if x=="account":
                        self.account.update(a[x])
                self.loop.stop()
    async def order_add(self,md5,symbol,direction,offset,volume,limit_price,advanced):
        d={"rq":"order_insert","md5":str(md5),"symbol":str(symbol),"direction":str(direction),"offset":str(offset),"volume":int(volume),"price":float(limit_price),"advanced":str(advanced)}
        await self.my_websocket.send(str(d))
    def insert_order(self, symbol: str, direction: str, offset: str, volume: int, limit_price:float = None,
                    advanced:str = "GFD"):
        if advanced not in ("GFD","FAK","FOK"):
            print("报单类型不支持")
            return
        id =uuid.uuid4().hex
        if limit_price==None:
            if direction=="BUY":
                limit_price=self.get_quote(symbol)["upper_limit"]
            else:
                limit_price=self.get_quote(symbol)["lower_limit"]


        self.增加任务.append(self.order_add(id,symbol,direction,offset,volume,limit_price,advanced))
        self.order[id]={
                    "order_id":id,
                    "exchange_order_id":"",
                    "instrument_id":symbol.split(".")[1],
                    "direction": direction,
                    "offset": offset,
                    "volume_orign":volume,
                    "volume_left":volume,
                    "limit_price":limit_price,
                    "price_type":  "LIMIT"  ,
                    "volume_condition": "ANY" if advanced in ("GFD","FAK") else "ALL" ,
                    "time_condition":  "GFD" if advanced=="GFD" else "IOC",
                    "insert_date_time":0,
                    "last_msg":"",
                    "CTP_status": "",
                    "status":    "ALIVE" ,
                    "trade_price":numpy.nan,
                    "trade_records":{}
        }
        return self.order[id]
    def 下单(self,品种,买卖方向,开平方向,下单量,价格=None,下单类型="GFD"):
        if 买卖方向=="买":
            买卖方向="BUY"
        elif 买卖方向=="卖":
            买卖方向="SELL"
        if 开平方向=="开":
            开平方向="OPEN"
        elif 开平方向=="平":
            开平方向="CLOSE"
        elif 开平方向=="平今":
            开平方向="CloseToday"
        self.insert_order(品种,买卖方向,开平方向,下单量,价格,下单类型)

    async def close_order(self,md5):
        d={"rq":"order_close", "md5":str(md5),}
        await self.my_websocket.send(str(d))
    def cancel_order(self, orderid:str) -> None:
        self.增加任务.append(self.close_order(orderid))
    def 撤单(self,订单编号):
        self.cancel_order(订单编号)

    def get_account(self):
        return self.account
    
    def 查资金(self):
        return self.account
    def get_order(self,order_id=None):
        if order_id is None:
            return self.order
        else:
            if order_id in self.order:
                return self.order[order_id]
            else:
                return None
    def 查订单(self,订单号=None):
        return self.get_order(订单号)

    def get_trade(self,trade_id=None):
        if trade_id is None:
            return self.trade
        else:
            if trade_id in self.trade:
                return self.trade[trade_id]
            else:
                return None
    def 查成交(self,成交号=None):
        return self.get_trade(成交号)
        
    def get_position(self,symbol=None):
        if symbol is None:
            return self.position
        else:
            if symbol in self.position:
                return self.position[symbol]
            else:
                self.position[symbol]={"exchange_id":symbol.split(".")[0],
                "instrument_id":symbol.split(".")[1],
                'pos_long_his':0,
                "pos_long_today":0,
                "pos_short_his":0,
                "pos_short_today":0,
                "open_price_long":0,
                "open_price_short":0,
                "position_price_long":0,
                "position_price_short":0,
                "position_cost_long":0,
                "position_cost_short":0,
                "float_profit_long":0,
                "float_profit_short":0,
                "float_profit":0,
                "position_profit_long":0,
                "position_profit_short":0,
                "position_profit":0,
                "margin_long":0,
                "margin_short":0,
                'margin':0,
                #净持仓
                "pos":0,
                "pos_long":0,
                "pos_short":0,
                "volume_long_frozen_today":0,
                "volume_long_frozen_his":0,
                "volume_short_frozen_today":0,
                "volume_short_frozen_his":0,
                "OpenCost_long_today":0,
                "OpenCost_long_his":0,
                "PositionCost_long_today":0,
                "PositionCost_long_his":0,
                "margin_long_today":0,
                "margin_long_his":0,
                "OpenCost_short_today":0,
                "OpenCost_short_his":0,
                "PositionCost_short_today":0,
                "PositionCost_short_his":0,
                "margin_short_today":0,
                "margin_short_his":0,
                "volume_long_frozen":0,
                "volume_short_frozen":0,
                }
                return self.position[symbol]
    def 查持仓(self,品种=None):
        return self.get_position(品种)

    async def 单symbol更新K任务(self,symbol):
        if self.异步连接 is None:
            self.异步连接 = await aioredis.create_redis_pool((self.data库连接地址, self.端口),db=self.data库数字,password=self.data库密码,loop=self.loop)
        ch1 = await self.异步连接.subscribe(symbol)
        ch1=ch1[0]
        async def reader(channel):
            async for message in channel.iter():
                data= eval(message.decode().replace("nan","numpy.nan").replace("1.7976931348623157e+308","numpy.nan"))
                #print(data)
                for x in self._单品行情更新[symbol]:
                    if x =="quote":
                        self._单品行情更新[symbol][x].update(data['q'])
                        if symbol in self.position:
                            if self.position[symbol]["pos_long"]>0:
                                self.position[symbol]["float_profit_long"]= ((data['q']["last_price"]-self.position[symbol]["open_price_long"])*self.position[symbol]["pos_long"])*data['q']["volume_multiple"]
                            if self.position[symbol]["pos_short"]>0:
                                self.position[symbol]["float_profit_short"]= ((self.position[symbol]["open_price_short"]-data['q']["last_price"])*self.position[symbol]["pos_short"])*data['q']["volume_multiple"]
                            if self.position[symbol]["pos_long"] or self.position[symbol]["pos_short"]:
                                self.position[symbol]["float_profit"]=self.position[symbol]["float_profit_long"]+self.position[symbol]["float_profit_short"]
                    else:
                        周期=int(x.split("_")[0])
                        if self._单品行情更新[symbol][x].datetime.iloc[-1]!=data['k'][周期][0]:
                            self._单品行情更新[symbol][x].update(self._单品行情更新[symbol][x].shift(-1))
                            self._单品行情更新[symbol][x].iloc[-1]=data['k'][周期]
                        else:
                            self._单品行情更新[symbol][x].iloc[-1]=data['k'][周期]
                self.loop.stop()
        await reader(ch1)
    
    async def 全symbol更新K任务(self):
        if self.异步连接 is None:
            self.异步连接 = await aioredis.create_redis_pool((self.data库连接地址, self.端口),db=self.data库数字,password=self.data库密码,loop=self.loop)
        ch1 = await self.异步连接.subscribe(self.订阅推送)
        ch1=ch1[0]

        async def reader(channel):
            async for message in channel.iter():
                if not self._整体行情更新:
                    continue
                data= eval(message.decode().replace("nan","numpy.nan").replace("1.7976931348623157e+308","numpy.nan"))
                for y in self._整体行情更新:
                    #先拿到symbol
                    for x in self._整体行情更新[y]:
                        if x =="quote":
                            self._整体行情更新[y][x].update(data['q'])
                        else:
                            周期=int(x.split("_")[0])
                            if self._整体行情更新[y][x].datetime.iloc[-1]!=data[y]['k'][周期][0]:
                                self._整体行情更新[y][x].update(self._整体行情更新[y][x].shift(-1))
                                self._整体行情更新[y][x].iloc[-1]=data[y]['k'][周期]
                            else:
                                self._整体行情更新[y][x].iloc[-1]=data[y]['k'][周期]
                self.loop.stop()
        await reader(ch1)


    def _set_wait_timeout(self):
        self._wait_timeout = True
        self.loop.stop()



    def wait_update(self,deadline=None):


        deadline_handle = None if deadline is None else self.loop.call_later(max(0, deadline - time.time()),
                                                                              self._set_wait_timeout)
        while True:
            if self.增加任务:
                self.loop.create_task( self.增加任务.pop())
            else:
                break

        self.loop.run_forever()
        if deadline_handle:
            deadline_handle.cancel()



    def get_kline_serial(self,symbol,周期,长度=200,接受通信方式="单体"):

        if symbol in self._整体行情更新 and 接受通信方式=="全体":
            if str(周期)+str("_")+str(长度) in self._整体行情更新[symbol]:
                return self._整体行情更新[symbol][str(周期)+str("_")+str(长度)]
        if symbol in self._单品行情更新 and 接受通信方式!="全体":
            if str(周期)+str("_")+str(长度) in self._单品行情更新[symbol]:
                return self._单品行情更新[symbol][str(周期)+str("_")+str(长度)]

        a= self.red.lrange(symbol+str(周期),0,长度-1)
        if a:
            b=[json.loads(x.decode()) for x in a[::-1] ]
        else:
            b=[]
        if len(b)<长度:
            c=[[0,0,0,0,0,0,0,0,0] for x in range(长度-len(b)) ]
            b=c+b
        c=pd.DataFrame(b,columns=["datetime",'id','open',"high","low",'close','volume',"open_oi","close_oi"]) 
        if 接受通信方式=="全体":
            if symbol not in self._整体行情更新:
                self._整体行情更新[symbol]={}
            self._整体行情更新[symbol].update({str(周期)+str("_")+str(长度):c})
        else:
            if symbol not in self._单品行情更新:
                self.增加任务.append(self.单symbol更新K任务(symbol))

                self._单品行情更新[symbol]={}
            self._单品行情更新[symbol].update({str(周期)+str("_")+str(长度):c})  
            
        return c

    def 获取_K线(self,品种,周期,长度=200,更新方式="单体"):
        return self.get_kline_serial(品种,周期,长度,更新方式)


    def get_tick_serial(self,symbol,长度=200,接受通信方式="单体"):
        if symbol in self._整体行情更新 and 接受通信方式=="全体":
            if str(0)+str("_")+str(长度) in self._整体行情更新[symbol]:
                return self._整体行情更新[symbol][str(0)+str("_")+str(长度)]
        if symbol in self._单品行情更新 and 接受通信方式!="全体":
            if str(0)+str("_")+str(长度) in self._单品行情更新[symbol]:
                return self._单品行情更新[symbol][str(0)+str("_")+str(长度)]

        nan=numpy.nan
        a= self.red.lrange(symbol+"0",0,长度)
        if a:
            b=[eval(x.decode().replace("nan","numpy.nan").replace("1.7976931348623157e+308","numpy.nan")) for x in a[::-1] ]
        else:
            b=[]
        if len(b)<长度:
            c=[[0,0,0,0] for x in range(长度-len(b))]

            b=c+b
        #[当前时间秒,LastPrice,Volume,卖一价格,卖一量,买一价格,买一量]
        c=pd.DataFrame(b,columns=["datetime",'last_price','volume',"ask_price1","ask_volume1","bid_price1","bid_volume2"])
        if 接受通信方式=="全体":
            if symbol not in self._整体行情更新:
                self._整体行情更新[symbol]={}
            self._整体行情更新[symbol].update({str(0)+str("_")+str(长度):c})
        else:
            if symbol not in self._单品行情更新:
                self.增加任务.append(self.单symbol更新K任务(symbol))

                self._单品行情更新[symbol]={}
            self._单品行情更新[symbol].update({str(0)+str("_")+str(长度):c})  
        return c    
    def 获取_tick线(self,品种,长度=200,更新方式="单体"):
        return self.get_tick_serial(品种,长度,更新方式)


    def get_quote(self,symbol,接受通信方式="单体"):

        if symbol in self._整体行情更新 and 接受通信方式=="全体":
            if "quote" in self._整体行情更新[symbol]:
                return self._整体行情更新[symbol][ "quote"]
        if symbol in self._单品行情更新 and 接受通信方式!="全体":
            if  "quote" in self._单品行情更新[symbol]:
                return self._单品行情更新[symbol][ "quote"]


        day="day"
        night="night"

        a=self.red.get(symbol+"quote").decode().replace("nan","numpy.nan").replace("1.7976931348623157e+308","numpy.nan")
        a=MyDict(eval(a))
        if 接受通信方式=="全体":
            if symbol not in self._整体行情更新:
                self._整体行情更新[symbol]={}
            self._整体行情更新[symbol].update({'quote':a})
        else:
            if symbol not in self._单品行情更新:
                self.增加任务.append(self.单symbol更新K任务(symbol))

                self._单品行情更新[symbol]={}
            self._单品行情更新[symbol].update({'quote':a})
        return a
    def 获取_tick(self,品种,更新方式="单体"):
        return self.get_quote(品种,更新方式)


    def is_changing(self,对象,对象名字):
        if 对象名字 not in self.cache_obj:
            self.cache_obj[对象名字]= copy.deepcopy(对象)
            return True
        else:
            if 对象!=self.cache_obj[对象名字]:
                self.cache_obj[对象名字]= copy.deepcopy(对象)
                return True
            else:
                return False
    def 判断对象是否改变(self,对象,对象名字):
        return self.is_changing(对象,对象名字)


if __name__ == "__main__":
    mid=0
    n=0
    api=GaoApi('040123','123456')
    a=api.get_kline_serial('SHFE.rb2101',10)
    while True:
        t1=time.time()
        api.wait_update(t1+5)
        持仓=api.get_position("SHFE.cu2010")
        if n==0:
            b=api.insert_order("SHFE.cu2010","BUY","CLOSE",1)
            n=1
        # if api.is_changing(持仓["pos"],"持仓数量"):
        #print("多仓",持仓["pos_long"],"空仓",持仓["pos_short"])
            # ma5=ma(a.close,5)
            # ma10=ma(a.close,10)
            # print(time_to_str(a.datetime.iloc[-1]),持仓["pos_long"],"上穿",crossup(ma5,ma10).iloc[-2],"下穿",crossup(ma10,ma5).iloc[-2])
            # if crossup(ma5,ma10).iloc[-2] and 持仓["pos_long"]==0:
            #     api.insert_order("SHFE.rb2101","BUY","OPEN",1)
            # if crossup(ma10,ma5).iloc[-2] and 持仓["pos_long"]>0:
            #     api.insert_order("SHFE.rb2101","SELL","CLOSETODAY",1)
        #     if n==0:
        #         b=api.insert_order("SHFE.rb2101","BUY","OPEN",1,3450)
        print(b)
        print(api.get_order(b["order_id"]))
        #     n+=1
        # if n==5:
        #     api.cancel_order(b["order_id"])
        # a=api.get_order()
        # for x in a:
        #     if a[x]["volume_left"]==0:
        #         print(a[x])

