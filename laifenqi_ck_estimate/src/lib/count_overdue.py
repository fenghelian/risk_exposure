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

sys.path.append("./../lib/")
from public_lib  import PublicLib


class CountOverdue(object):
    ''' 按照规定粒度产生结合历史账单产生新的账单数据 '''
    def __init__(self, this_dt_type):
        '''
        self.dict_order_overdue = {}
        self.dict_cycle_overdue = {}
        self.this_dt_type       = this_dt_type
        self.this_dt            = datetime.now().strftime("%Y-%m-%d") 
        self.month_list         = []
        # 在计算日期内，计算各个账期的逾期金额，各参数调试配置
        self.bill_one_coefficient       = 1   #有一次还款行为系数调整
        self.bill_self_coefficient   = 1   #账单历史数据自身拟合diff系数调整
        self.bill_self_coefficient_bt   = 1   #账单历史数据自身拟合diff系数调整
        self.bill_self_coefficient_sw   = 1   #账单历史数据自身拟合diff系数调整
        self.bill_new_coefficient    = 1   #刚到期账期数据，历史逾期数据拟合,系数调整
        self.new_create_bill_coefficient     = 1 #新创建的订单，账期逾期率=历史逾期率 * 调整系数
        self.new_create_bill_coefficient_bt     = 1 #新创建的订单，账期逾期率=历史逾期率 * 调整系数
        self.new_create_bill_coefficient_sw     = 1 #新创建的订单，账期逾期率=历史逾期率 * 调整系数
        self.dict_day_order_coefficient  = {} #日期、订单类型、修改系数
        print 'will count bill_items overdue.....'
        '''


    def check_bill_items_overdue_data(self, this_value, last_value):
        ''' 检查逾期金额是否正常，若不正常，进行校正 '''
        if this_value <= 0.0 :
            if last_value <= 1000.0 :
                ret_value=0.0
            else :
                ret_value=last_value*0.5
        else :
            ret_value=this_value
        pass
        if last_value > 0.0 :
            ret_value = this_value if this_value < last_value  else last_value
        return ret_value
        pass

    def get_history_cycle_fenqi_diff_rate_fun(self, this_type, fenqi_cycle, fenqi, order_month_diff):
        '''  获取历史各个分期账单不同到期月份的逾期率  '''
        if int(order_month_diff) < 0:
            return []
        this_value=[]
        this_key=str(fenqi_cycle)+","+str(fenqi)+","+str(order_month_diff)
        if self.dict_cycle_overdue.has_key(this_key):
            this_value = copy.deepcopy(self.dict_cycle_overdue[this_key])
        pass
        return this_value

    def check_change_rate_roll_rate(self, order_list, rate_list):
        ''' '''
        order_dt          = order_list[0]  #订单创建月份
        deadline_dt       = order_list[1]  #账单到期月份
        this_type         = int(order_list[2])  # 订单类型 5:现金；6:实物; 8:趣先享
        fenqi_cycle       = int(order_list[3])  #分期类型：1周；2月
        fenqi             = int(order_list[4])  #分期数
        order_month_diff  = int(order_list[5])  #账单到期月份 与 订单创建月份 的 月份差
        this_column = 'd1_30'
        #return rate_list
        if deadline_dt < self.this_dt or len(self.df_tomorrow) < 1:
            return rate_list
        #product_index,type,fenqi_cycle,fenqi,bill_order,begin_dt,end_dt,orign_amount_sum,d1_30
        df_tmp = self.df_tomorrow[(self.df_tomorrow['type'] == this_type) & (self.df_tomorrow['fenqi_cycle']==fenqi_cycle) & (self.df_tomorrow['fenqi']==fenqi) & (self.df_tomorrow['bill_order']==order_month_diff) & (self.df_tomorrow['begin_dt'] <= deadline_dt) & (self.df_tomorrow['end_dt'] >= deadline_dt)]
        if len(df_tmp) < 1:
            df_tmp = self.df_tomorrow[(self.df_tomorrow['type'] == this_type) & (self.df_tomorrow['fenqi_cycle']==999) & (self.df_tomorrow['fenqi']==999) & (self.df_tomorrow['bill_order']==999) & (self.df_tomorrow['begin_dt'] <= deadline_dt) & (self.df_tomorrow['end_dt'] >= deadline_dt)]
        if len(df_tmp) < 1:
            return rate_list
        check_rate  = float(df_tmp[this_column].tolist()[0])
        if float(rate_list[29]) >= float(check_rate) :
            return rate_list
        ret_list    = [ check_rate ]
        check_len   = 30 - 1
        this_len    = len(rate_list)
        for i in range(check_len-1, -1, -1):
            this_rate = ( float(rate_list[i])/float(rate_list[i+1]) ) * ret_list[0]
            ret_list = [ this_rate ]  + ret_list
        for i in range(check_len+1, this_len, 1):
            this_rate = ( float(rate_list[i])/float(rate_list[i-1]) ) * ret_list[-1]
            ret_list = ret_list + [ this_rate ]
        return  map(str, ret_list)


    def get_special_coefficient(self, this_line):
        ''' '''
        ret_coefficient = 1.0
        order_dt          = this_line['dt']  #订单创建月份
        this_type         = int(this_line['type'])        # 订单类型 5:现金；6:实物; 8:趣先享
        fenqi_cycle       = int(this_line['fenqi_cycle'])  #分期类型：1周；2月
        fenqi             = int(this_line['fenqi'])        #分期数
        #order_key    =  str(order_dt) + "," + str(this_type) + "," + str(fenqi_cycle) + "," + str(fenqi)
        #if self.dict_day_order_coefficient.has_key(order_key):
        #    ret_coefficient = self.dict_day_order_coefficient[order_key]
        #else :
        # 查看天粒度 各分期系数
        #               天              月                  年
        order_list   = [ str(order_dt), str(order_dt)[0:7], str(order_dt)[0:4] ]
        #                类型       所有
        type_list    = [ this_type, 999 ]
        #                       周/月       所有
        fenqi_cycle_list  = [ fenqi_cycle, 999 ]
        #                期数   所有
        fenqi_list   = [ fenqi, 999 ]
        break_flag   = 0
        #for o_k, o_v in enumerate(order_list):
        for t_k, t_v in enumerate(type_list):
            for c_k, c_v in enumerate(fenqi_cycle_list):
                for f_k, f_v in enumerate(fenqi_list):
                    for o_k, o_v in enumerate(order_list):
                        this_key    = str(o_v) + "," + str(t_v) + "," + str(c_v) + "," + str(f_v)
                        if self.dict_day_order_coefficient.has_key(this_key):
                            ret_coefficient = self.dict_day_order_coefficient[this_key]
                            break_flag = 1
                            break
                        pass
                    pass
                    if break_flag == 1 :
                        break
                pass
                if break_flag == 1:
                    break
            pass
            if break_flag == 1:
                break
        pass
        return ret_coefficient



    def count_bill_items_overdue_parameters(self, this_line, mig_rate=1.0, risk_rate=1.0):
        ''' 根据历史配置逾期率参数,计算各个账期逾期金额 '''
        #print 'this_line:',this_line.tolist()
        #this_line: [u'2016-12-28', u'2017-07-29', 2, 9, 7, 213.0, 45755.46, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7953.69, 7373.63, 6702.87, 6528.86, 6528.86]
        # 获取对某天/月，对部分产品的调整系数
        this_special_coefficient    =   self.get_special_coefficient(this_line)
        # 版本选择  1:旧版本,不支持调整留存率  0: 非旧版本,支持调整留存率
        version_flag  =  0 
        this_list=this_line.tolist()
        #ret_str=""  # 返回month_list里面的逾期金额
        ret_str=[]  # 返回month_list里面的逾期金额
        order_dt          = this_line['dt']  #订单创建月份
        deadline_dt       = this_line['deadline_dt']  #账单到期月份
        this_type         = int(this_line['type'])        # 订单类型 5:现金；6:实物; 8:趣先享
        fenqi_cycle       = int(this_line['fenqi_cycle'])  #分期类型：1周；2月
        fenqi             = int(this_line['fenqi'])        #分期数
        order_month_diff  = int(this_line['order_month_diff'])  #账单到期月份 与 订单创建月份 的 月份差
        this_capital      = float(this_line['capital_sum'])
        #根据订单创建时间划分：1:本月之前的订单，使用订单创建时间不同订单、期数、账期逾期率，和自身拟合;  
        #                      2:本月之后的模拟订单，使用近半年年的逾期率，和自身拟合
        if version_flag == 1:
            bill_self_coefficient = float(mig_rate)
            new_create_bill_coefficient = float(risk_rate)
        else :
            # 留存率调整系数
            bill_self_migration   = float(mig_rate)
            # 未来风险调整系数
            new_bill_risk         = float(risk_rate)
            # 181天后，自身衰减系数
            bill_self_coefficient  = 0.3
        pass
        if len(this_list) > 200 :
            print 'bill  migration_rate:',bill_self_migration,',risk_rate:',new_bill_risk
            print 'this list:',this_list
        #this_day_order_coefficient=1.0
        #order_key    =  str(order_dt) + "," + str(this_type) + "," + str(fenqi_cycle) + "," + str(fenqi)
        #if self.dict_day_order_coefficient.has_key(order_key):
        #    this_day_order_coefficient = self.dict_day_order_coefficient[order_key]
        this_date_list=[]   # 本次需要计算的日期
        #print self.month_list
        for i in self.month_list:
            if deadline_dt > i:
                #ret_str += ",0"
                ret_str.append(0)
                continue
            this_date_list.append(i)
        #print this_date_list
        #账单到期月份 大于 month_list最大月份
        if deadline_dt > max(self.month_list):  # 或者直接用 self.month_list[-1]
           return ret_str
        #print 'this_list:',this_list
        #1:本月之前的订单
        if order_dt < self.this_dt :
            #1:若果最近一个月的逾期为空或者为0，后边所有日期均为0; 2:若果最近一个月逾期金额没有变化，后边所有日期逾期金额也不变化
            if  deadline_dt < self.this_dt and ( (this_list[-1] <= "0.0"  and  float(this_list[-1]) <= 0.0) or (this_list[-30] == this_list[-1]) ):
                for i in this_date_list :
                    #ret_str += "," + str(this_list[-1])
                    ret_str.append(this_list[-1])
            # 逾期表现数据，大于0次，小于三次的情况：根据订单创建时间不同订单、期数、账期逾期率进行填充到3个个月，后边使用自身拟合
            #elif float(this_list[-1]) > 0.0 and float(this_list[-3]) <= 0.0 :
            else :
                #获取历史逾期信息,按照历史逾期留存率对本次进行拟合
                #dt,deadline_dt,type,fenqi_cycle,fenqi,order_month_diff,capital_sum,num,all_num,all_orign_amount,d_181,d_180,d_179,d_178,d_177,d_176,d_175........
                this_key=str(order_dt) + "," + str(deadline_dt) + "," + str(this_type) + "," + str(fenqi_cycle) + "," + str(fenqi) + "," + str(order_month_diff)
                ret_list = self.dict_order_overdue[this_key]
                #print 'ret_list:',ret_list
                ret_list = ret_list[10:]
                ret_list = self.check_change_rate_roll_rate(this_list[0:6], ret_list)
                #  逾期表现数据次数
                this_begin_num = 0
                for j in range(len(this_list[8:])):
                    if float(this_list[(j+1)*(-1)]) > 0.0:
                        this_begin_num += 1
                    else :
                        break
                #this_begin_num = (datetime.strptime(this_date_list[0], "%Y-%m-%d")  - datetime.strptime(deadline_dt, "%Y-%m-%d")).days - 1 + this_begin_num 
                if this_begin_num >= 30 :
                    this_begin_num = (datetime.strptime(this_date_list[0], "%Y-%m-%d")  - datetime.strptime(deadline_dt, "%Y-%m-%d")).days  + this_begin_num
                for i,m_v in enumerate(this_date_list, 0):
                    #print 'i:',i,',m_v:',m_v,',i+this_begin_num:',i+this_begin_num,',value:',ret_list[i+this_begin_num]
                    if i+this_begin_num < len(ret_list) and len(ret_list[i+this_begin_num]) > 0:
                        #print 'this_capital:',this_capital,',ret_list[i+this_begin_num]:',ret_list[i+this_begin_num]
                        if version_flag == 1 :
                            value_tmp   = this_capital * float(ret_list[i+this_begin_num])  * new_create_bill_coefficient
                        else :
                            if deadline_dt < self.this_dt :   # 调整留存
                                value_tmp  = float(this_list[-1]) * (float(ret_list[i+this_begin_num])/float(ret_list[i+this_begin_num-1])) * bill_self_migration
                            else :  # 调整未来风险系数   和  留存
                                if this_begin_num == 0  and i == 0:
                                    #value_tmp = this_capital * float(ret_list[i+this_begin_num]) * new_bill_risk 
                                    if this_special_coefficient == 1.0 :
                                        value_tmp = this_capital * float(ret_list[i+this_begin_num]) * new_bill_risk
                                    else :
                                        value_tmp = this_capital * float(ret_list[i+this_begin_num]) * this_special_coefficient
                                    pass
                                else :
                                    value_tmp  = float(this_list[-1]) * (float(ret_list[i+this_begin_num])/float(ret_list[i+this_begin_num-1])) * bill_self_migration 
                                #value_tmp = value_tmp * new_bill_risk
                            pass
                        pass
                    else :
                        if float(this_list[-4]) <= 0.0 :
                            value_tmp = float(this_list[-1]) - (float(this_list[-2])-float(this_list[-1]))*bill_self_coefficient
                        else :
                            value_tmp = float(this_list[-1]) - (0.3*(float(this_list[-3]) - float(this_list[-2])) + 0.7*(float(this_list[-2]) - float(this_list[-1]))) * bill_self_coefficient
                    value_tmp = self.check_bill_items_overdue_data(value_tmp, float(this_list[-1]))
                    #value_tmp = self.check_bill_items_overdue_data(value_tmp*this_day_order_coefficient, float(this_list[-1]))
                    #ret_str += "," + str(value_tmp)
                    ret_str.append(value_tmp)
                    this_list.append(str(value_tmp))
                pass
            pass
        #2:本月之后的模拟订单
        else :
            #获取历史逾期信息,
            #dt,deadline_dt,type,fenqi_cycle,fenqi,order_month_diff,capital_sum,num,all_num,all_orign_amount,d_181,d_180,d_179,.....
            this_key = str(this_type) + "," + str(fenqi_cycle) + "," + str(fenqi)  + "," + str(order_month_diff)
            key_tmp  = str(this_type) + "," + str(fenqi_cycle) + "," + str(fenqi)  + "," + str(int(order_month_diff)-1)
            #ret_list = self.dict_cycle_overdue[this_key] if self.dict_cycle_overdue.has_key(this_key)  else self.dict_cycle_overdue[key_tmp] 
            #ret_list = ret_list[4:]
            if self.dict_cycle_overdue.has_key(this_key) :
                ret_list = self.dict_cycle_overdue[this_key][4:]
            elif self.dict_cycle_overdue.has_key(key_tmp) :
                ret_list = self.dict_cycle_overdue[key_tmp][4:]
            else : # 大盘数据
                #ret_list = self.all_bs_overdue["laifenqi"]
                ret_list = self.all_bs_overdue[self.this_bs]
            pass
            ret_list    = self.check_change_rate_roll_rate(this_list[0:6], ret_list)
            for i,m_v in enumerate(this_date_list):
                if i < len(ret_list) and len(ret_list[i]) > 0:
                    if version_flag == 1 :
                        value_tmp   = this_capital * float(ret_list[i]) * new_create_bill_coefficient 
                    else :
                        if i == 0 :
                            #value_tmp   = this_capital * float(ret_list[i]) * new_bill_risk
                            ##if deadline_dt < "2017-11-25":
                            ##    version_flag = -1
                            if this_special_coefficient == 1.0 :
                                value_tmp   = this_capital * float(ret_list[i]) * new_bill_risk
                            else :
                                value_tmp   = this_capital * float(ret_list[i]) * this_special_coefficient
                            pass
                        else :
                            value_tmp   = float(this_list[-1]) * (float(ret_list[i])/float(ret_list[i-1]))  * bill_self_migration
                        #value_tmp   = value_tmp * new_bill_risk
                else :
                    if float(this_list[-4]) <= 0.0 :
                        value_tmp = float(this_list[-1]) - (float(this_list[-2])-float(this_list[-1]))*bill_self_coefficient
                    else :
                        value_tmp = float(this_list[-1]) - (0.3*(float(this_list[-3]) - float(this_list[-2])) + 0.7*(float(this_list[-2]) - float(this_list[-1]))) * bill_self_coefficient
                value_tmp = self.check_bill_items_overdue_data(value_tmp, float(this_list[-1]))
                #value_tmp = self.check_bill_items_overdue_data(value_tmp*this_day_order_coefficient, float(this_list[-1]))
                #ret_str += "," + str(value_tmp)
                ret_str.append(value_tmp)
                this_list.append(str(value_tmp))
            pass
        pass
        #print 'ret_str:',ret_str
        #print 'migration_rate:',mig_rate,',risk_rate:',risk_rate
        #print 'this line:',this_line
        #print 'this_list:',this_list
        #print 'ret_list:',ret_list
        #print 'ret str:',ret_str
        ##sys.exit()
        return ret_str



    def estimatior_bill_overdue(self, df, mig_rate=1.0, risk_rate=1.0):
        ''' 根据传入的账单在计算各个日期逾期金额  '''
        begin = time.time()
        df['ret_string']  = df.apply(lambda x: self.count_bill_items_overdue_parameters(x, mig_rate, risk_rate), axis=1)
        #for j,m_v in enumerate(self.month_list, 1):
            #df[m_v]   = df['ret_string'].apply(lambda x: x.split(',')[j])
        for j,m_v in enumerate(self.month_list, 0):
            df[m_v]   = df['ret_string'].apply(lambda x: x[j])
        end = time.time()
        pass
        del df['ret_string']
        #return df

    #def count_date_bill_items_overdue_fun(self):
    def count_date_bill_overdue(self):
        ''' 计算各个计算日期逾期金额  '''
        mig_rate=self.bill_one_coefficient
        risk_rate=self.new_create_bill_coefficient
        self.estimatior_bill_overdue(self.df_all_bill, mig_rate, risk_rate)
        self.df_all_bill.to_csv(self.all_bill_items_overdue_file, sep=',', encoding='utf-8', index=False)



