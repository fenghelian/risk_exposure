#!/usr/bin/python
#encoding=utf-8


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
import types
from datetime import datetime


class CountOrderExposure(object):
    ''' 按照规定粒度产生结合历史账单产生新的账单数据 '''
    def __init__(self):
        ''' 
        self.this_dt_type       = "month"
        self.this_dt            = datetime.now().strftime("%Y-%m")  if self.this_dt_type == "month"  else   datetime.now().strftime("%Y-%m-%d")
        self.this_path          = os.path.dirname(os.path.abspath(__file__))
        self.df_all_bill        = None
        self.this_end_month     = ""
        self.month_list         = []
        self.dict_order_rate    = {} # 订单创建日期、订单类型、分期数、账期，对应的金额占比
        self.dict_all_rate      = {} # 订单类型、分期数、账期，对应的金额占比
        print 'will count bill_items overdue.....'
        '''

    def get_type_cycle_fenqi_capital_loan_rate(self, this_list):
        ''' 根据订单创建时间、分期类型、期数，第几账期，获取本期逾期金额对应的敞口占比 '''
        order_dt          = this_list[0]  #订单创建日期  --- 月份
        deadline_dt       = this_list[1]  #账单到期日期  --- 月份
        this_type         = int(this_list[2])
        fenqi_cycle       = int(this_list[3])  #分期类型：1周；2月
        fenqi             = int(this_list[4])  #分期数
        order_month_diff  = int(this_list[5])  #账单到期月份 与 订单创建月份 的 月份差

        order_month_diff = 1 if order_month_diff == 0 else order_month_diff
        order_month_diff = fenqi if order_month_diff > fenqi else order_month_diff
        this_key = str(this_type) + "," + str(fenqi_cycle) + "," + str(fenqi) + "," + str(order_month_diff)
        #先获取订单创建日期的金额占比
        if self.dict_order_rate.has_key(str(order_dt) + "," + this_key) :
#begin = time.time()
            ret_rate    = self.dict_order_rate[str(order_dt) + "," + this_key]
#end = time.time()
#            print '******-----------*******-------*******------ dict_order_rate 耗时：',end-begin, 'seconds --------*********------*******------******'
        elif self.dict_all_rate.has_key(this_key) :
#            begin = time.time()
            ret_rate    = self.dict_all_rate[this_key]
#            end = time.time()
#            print '******-----------*******-------*******------ dict_all_rate 耗时：',end-begin, 'seconds --------*********------*******------******'
        else :
            print 'this :',this_list,',has no rate!!!'
            sys.exit()
        return ret_rate
        #若订单创建日期金额占比为空，不带订单创建日期金额占比
        


    def computer_overdue_loan_amount(self, this_line):
        ''' '''
        #['dt','deadline_dt','type','fenqi_cycle','fenqi','order_month_diff','capital_sum','all_orign_amount', deadline_dt]
        #2017-02', u'2017-08', 5, 2, 6, 6, 94473828.51, 540017300.0, 3240103800.0
        #print 'this line:',this_line.tolist()
        this_list         = this_line.tolist()
        order_dt          = this_line['dt']  #订单创建日期  --- 月份
        deadline_dt       = this_line['deadline_dt']  #账单到期日期  --- 月份
        this_type         = this_line['type']
        fenqi_cycle       = this_line['fenqi_cycle']  #分期类型：1周；2月
        fenqi             = int(this_line['fenqi'])  #分期数
        order_month_diff  = int(this_line['order_month_diff'])  #账单到期月份 与 订单创建月份 的 月份差
        capital_sum       = float(this_line['capital_sum']) #本期账单对应的本金
        all_orign_amount  = float(this_line['all_orign_amount']) #在这个创建日期内本订单类型的放贷总额
        this_overdue      = float(this_list[-1]) #本期账单逾期金额
        ret_money         = this_overdue  # 返回计算的敞口金额
        
        ##分期1期的，逾期金额即敞口 不论周月订单，order_month_diff 是1或0
        # 周订单每期的本金是均分的
        if fenqi == 1 :
            ret_money   = this_overdue
        elif int(fenqi_cycle) == 1:
            order_month_diff = 1 if order_month_diff == 0 else order_month_diff
            order_month_diff = fenqi if order_month_diff > fenqi else order_month_diff
            ret_money   = (fenqi + 1 - order_month_diff) * this_overdue
        else :
            ret_rate    = self.get_type_cycle_fenqi_capital_loan_rate(this_list[0:6])
            ret_money   = this_overdue *  float(ret_rate)
        pass
        return ret_money


