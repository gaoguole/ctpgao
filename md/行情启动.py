
import CTP_md
#行情订阅,如果用到郑州品种的带夜盘的 日线周期,一定要订阅一个上海交易所带夜盘的品种
品种=["CFFEX.IC2011","CFFEX.IF2011","DCE.i2101","DCE.eg2101","SHFE.rb2101","CZCE.MA101","CZCE.TA101"]
周期=[10]

CTP_md.启动行情记录2(品种,周期)