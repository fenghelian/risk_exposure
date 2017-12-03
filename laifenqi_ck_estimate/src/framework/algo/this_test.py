#coding=utf-8
'''
Created on 2017年9月19日

@author: huangpingchun
'''

from pandas import Series,DataFrame
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import math

import sys
reload(sys)
sys.setdefaultencoding('utf8')
import os
import time
import datetime
import calendar
import random
import json
import copy
import ConfigParser
from datetime import datetime


sys.path.append("./../../lib/")
sys.path.append("./../../app/")


from product_ck_estimator import ProductCKEstimator



def main():
    config_file     = sys.argv[1]
    this_end_month  = sys.argv[2]
    this_dt_type    = sys.argv[3]
    product={}
    product["bs"]           = 'laifenqi'
    product["type"]         = sys.argv[4]
    product["fenqi_cycle"]  = sys.argv[5]
    product["fenqi"]        = sys.argv[6]
    this_start      = sys.argv[7]
    this_end        = sys.argv[8]
    this_per        = sys.argv[9]
    product["param_flag"]   = sys.argv[10]
    #params_flag     = sys.argv[10]
    print 'config file:',config_file,',count end month:',this_end_month,',this date type:',this_dt_type,',type:',product["type"],',this_fenqi_cycle:',product["fenqi_cycle"],',this_fenqi:',product["fenqi"]
    if len(sys.argv) > 11:
        print 'len:',len(sys.argv),',sys:',sys.argv
        this_dt = sys.argv[11]
    else :
        this_dt = datetime.now().strftime("%Y-%m") if this_dt_type == "month"  else datetime.now().strftime("%Y-%m-%d")   #本月月份  本月及之后的月份账单表重新生成，之前的无需重新生成
    run = ProductCKEstimator(config_file, this_end_month, this_dt_type, this_dt)
    run.preprocess()
    run.process()
    this_value=[float(this_start), float(this_end), float(this_per)]
    ret_list = run.adjusting_best_parameters(product, this_value)
    print "result:",",".join(map(str, ret_list))

if __name__=="__main__":
    main()

