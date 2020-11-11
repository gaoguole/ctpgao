import redis
import time
from tqsdk.tafunc import time_to_str
red=redis.Redis(db=15)
n=0
t2=0
while True:
    a=red.lrange("DCE.i210110",0,5)
    if n!=eval(a[0].decode())[1]:
        t1=time.time()
        print(t1-t2)
        t2=t1
        n=eval(a[0].decode())[1]
        print("----------")
        for x in a:
            print(time_to_str(eval(x.decode())[0]))
