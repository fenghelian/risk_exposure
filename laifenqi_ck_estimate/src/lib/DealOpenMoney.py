#!/usr/bin/python
#encoding=utf-8


from pandas import Series,DataFrame
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from lib import math_util

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
from datetime import datetime


class DealOpenMoney(object):
    '''   '''
    def __init__(self):
        ''' '''
        print 'will deal order open money.....'



    def create_new_bills_fun(self):
        ''' 结合现在的订单账单详情，merge未来预测交易，生成新的账期到账表 '''
        #merge_now_predict_amount_create_new_bill(this_now_file, this_predict_file, this_fenqi_file, this_result_file):
        #this_now_file  现在账单详情
        #this_predict_file 预测交易额文件
        #this_fenqi_file  各个账期占总体交易额占比
        #this_result_file  产出文件
    
        fenqi_dict={} #保存各个账期占总体交易额占比
    
        df = pd.read_csv(self.bill_data_now_file, sep=',', encoding='utf-8')
        #过滤未到月底的本月情况
        #dt=df[df['dt']<datetime.now().strftime("%Y-%m")]
        df=df[df['dt']< self.this_dt]
        #先把过滤后的新账单详情生成到产出文件
        #df[['dt','deadline_dt','fenqi_cycle','fenqi','order_month_diff','capital_sum','all_orign_amount']].to_csv(this_result_file, sep=',', encoding='utf-8', index=False)
        df.to_csv(self.bill_data_all_file, sep=',', encoding='utf-8', index=False)
    
        #生成未来预估的新账单
        fd = open(self.bill_data_all_file, 'a+')
    
        with open(self.fenqi_capital_rate_file, 'r')  as fd_in:
            lines=fd_in.readlines()
            for line in lines:
                this_line = line.strip('\n').split(',')
                fenqi_dict[this_line[0] + "," + this_line[1] + "," + this_line[2]] = line.strip('\n')
        pass
        with open(self.qudian_amount_data_file, "r") as fd_in:
            lines=fd_in.readlines()
            for line in lines:
                this_line = line.strip('\n').split(',')
                #print 'this_line:',this_line[0]
                if this_line[0] < self.this_dt:
                    continue
                for (k, v) in fenqi_dict.iteritems():
                    if k == "fenqi_cycle,fenqi,diff":
                        continue
                    fenqi_line = v.split(',')
                    deadline_dt=(datetime.strptime(this_line[0], "%Y-%m") + relativedelta(months=(int(fenqi_line[2])))).strftime("%Y-%m")
                    fenqi_amount_tmp=float(fenqi_line[3]) * float(fenqi_line[4]) * int(this_line[1])
                    cycle_ampunt_tmp=float(fenqi_line[3]) * int(this_line[1])
                    #new_amount_list +=[this_line[0], deadline_dt, fenqi_line[0], fenqi_line[1], fenqi_line[2], str(fenqi_amount_tmp), str(cycle_ampunt_tmp)]
                    str_tmp = ",".join([this_line[0], deadline_dt, fenqi_line[0], fenqi_line[1], fenqi_line[2], str(fenqi_amount_tmp), str(cycle_ampunt_tmp)])
                    fd.write(str_tmp+",,,,,,,,,,,,,,,,,,,,\n")
        fd.close()

    def get_all_new_order_bill_data_end(self, this_order_dt, this_deadline_dt, this_fenqi_cycle, this_fenqi, this_order_month_diff):
        ''' 根据订单创建时间、账单到期时间、分期类型、分期数、订单时间与账单到期时间diff  获取历史数据 返回list  '''
        this_order_tmp = self.check_month_change_last_month(this_order_dt)
        this_deadline_tmp = self.check_month_change_last_month(this_deadline_dt)
        #print 'new:',self.df_all[(self.df_all['dt']==this_order_tmp) & (self.df_all['deadline_dt'] == this_deadline_tmp) & (self.df_all['fenqi_cycle'] == int(float(this_fenqi_cycle))) & (self.df_all['fenqi'] == int(float(this_fenqi)))].head()
        #print '---------------------------start----------------------------------------------'
        #print 'df_all:',self.df_all.head(),',this_order_dt:',this_order_dt,',this_order_tmp:',this_order_tmp,',this_deadline_dt:',this_deadline_dt,',this_deadline_tmp:',this_deadline_tmp,',this_fenqi_cycle:',this_fenqi_cycle,',this_fenqi:',this_fenqi,',this_order_month_diff:',this_order_month_diff
        this_old_diff = self.df_all[(self.df_all['dt']==this_order_tmp) & (self.df_all['deadline_dt'] == this_deadline_tmp) & (self.df_all['fenqi_cycle'] == int(float(this_fenqi_cycle))) & (self.df_all['fenqi'] == int(float(this_fenqi)))]['order_month_diff'].max()
        this_diff = this_order_month_diff if this_old_diff > this_order_month_diff  else this_old_diff
        #print 'this_old_diff:',this_old_diff,',this_diff:',this_diff
        #print self.df_all[(self.df_all['dt']==this_order_tmp) & (self.df_all['deadline_dt'] == this_deadline_tmp) & (self.df_all['fenqi_cycle'] == int(float(this_fenqi_cycle))) & (self.df_all['fenqi'] == int(float(this_fenqi))) & (self.df_all['order_month_diff'] == this_diff)].head()
        #print 'dt:',this_order_dt,this_order_tmp,',this_order_month_diff',this_order_month_diff,this_diff,',this_deadline_tmp:',this_deadline_tmp,this_deadline_tmp,',this_old_diff:',this_old_diff,',this_fenqi_cycle:',this_fenqi_cycle,',this_fenqi:',this_fenqi,',this_order_month_diff:',this_diff
        #df_tmp=self.df_all[(self.df_all['dt'] == this_order_tmp) & (self.df_all['deadline_dt'] == this_deadline_tmp) & (self.df_all['fenqi_cycle'] == int(float(this_fenqi_cycle)) ) & (self.df_all['fenqi'] == int(float(this_fenqi)) ) & (self.df_all['order_month_diff'] == this_diff)]        
        #del df_tmp
        df_tmp=self.df_all[(self.df_all['dt']==this_order_tmp) & (self.df_all['deadline_dt'] == this_deadline_tmp) & (self.df_all['fenqi_cycle'] == int(float(this_fenqi_cycle))) & (self.df_all['fenqi'] == int(float(this_fenqi))) & (self.df_all['order_month_diff'] == this_diff)]
        #print 'df_tmp:',df_tmp.head()
        return_list=[]
        return_list.append(this_old_diff)
        if len(df_tmp) == 0:
            return [this_old_diff]
        else:
            this_columns_list=df_tmp.columns.tolist()
            for i in range(365):
                this_dt = (datetime.strptime("2015-11", "%Y-%m") + relativedelta(months=(i))).strftime("%Y-%m")
                if this_dt < this_deadline_tmp:
                    continue
                if this_dt not in this_columns_list:
                    continue
                if this_dt >= datetime.now().strftime("%Y-%m") :
                    break
                #if df_tmp[this_dt].isnull() or  float(df_tmp[this_dt].tolist()[0]) <= 0 :
                #print 'this_deadline_tmp:',this_deadline_tmp,',this_dt:',this_dt,',month_dt:',list(set(df_tmp['dt'].tolist())),''
                if df_tmp[this_dt].isnull().sum() == 1 :
                    #continue
                    return_list.append(0)
                else :
                    tmp_value=float(df_tmp[this_dt].tolist()[0])/float(df_tmp['capital_sum'].tolist()[0])
                    return_list.append(tmp_value)
            pass
        pass
        #print return_list
        #print '---------------------------end----------------------------------------------'
        return return_list
                                         



    def get_all_new_order_bill_data_fun(self, this_order_dt, this_deadline_dt, this_fenqi_cycle, this_fenqi, this_order_month_diff, diff_len):
        ''' 根据订单创建时间、账单到期时间、分期类型、分期数、订单时间与账单到期时间diff  获取历史数据 返回list  '''
        self.df_all = pd.read_csv(self.bill_month_overdue_data_file, 
                                  sep=',', encoding='utf-8')
        this_diff = int(float(this_order_month_diff)  +  diff_len)
        this_month = datetime.now().strftime("%Y-%m") 
        return_list=[]
        this_deadline_tmp=(datetime.strptime(this_deadline_dt, "%Y-%m") + 
                           relativedelta(months=(diff_len))).strftime("%Y-%m") 
        #print self.df_all.head()
        df_tmp=self.df_all[(self.df_all['dt'] < this_month) 
                           & (self.df_all['dt']==this_order_dt) 
                           & (self.df_all['deadline_dt'] == this_deadline_tmp) 
                           & (self.df_all['fenqi_cycle'] == int(float(this_fenqi_cycle)) ) 
                           & (self.df_all['fenqi'] == int(float(this_fenqi)) ) 
                           & (self.df_all['order_month_diff'] == this_diff)]        
        #print df_tmp.head()
        if len(df_tmp) == 0:
            return []
        else:
            for i in range(365):
                this_dt = (datetime.strptime("2015-11", "%Y-%m") + relativedelta(months=(i))).strftime("%Y-%m")
                if this_dt >= this_month :
                    break
                #if df_tmp[this_dt].isnull() or  float(df_tmp[this_dt].tolist()[0]) <= 0 :
                if df_tmp[this_dt].isnull().sum() == 1 :
                    continue
                else :
                    tmp_value=float(df_tmp[this_dt].tolist()[0])/float(df_tmp[this_dt].tolist()[0])
                    if tmp_value ==0 or tmp_value == 1:
                        continue
                    return_list.append(tmp_value)
            pass
        pass
        return return_list


    def get_next_month_overdue(self, this_line, month_list):
        ''' 补充month_list月份的月末逾期金额 '''
        #2015-11,2015-12,1,1,1,770500.0,816000.0,,5000.0,5000.0,5000.0,5000.0,4500.0,4500.0,3500.0,3500.0,3500.0,3500.0,3500.0,3500.0,3500.0,3500.0,3500.0,3500.0,3500.0,3500.0,3500.0
        ret_str=""  # 返回month_list里面的逾期金额
        this_len          = len(this_line)
        #this_dt           = datetime.now().strftime("%Y-%m")  #本月月份
        order_dt          = this_line[0]  #订单创建月份
        deadline_dt       = this_line[1]  #账单到期月份
        fenqi_cycle       = this_line[2]  #分期类型：1周；2月
        fenqi             = this_line[3]  #分期数
        order_month_diff  = this_line[4]  #账单到期月份 与 订单创建月份 的 月份差
        #print this_line,',month_list:',month_list
        #账单到期月份 小于 本月月份
        if deadline_dt < self.this_dt :
            #若果最近一个月的逾期为空或者为0，后边所有日期均为0
            if len(this_line[-1])  == 0  or  float(this_line[-1]) <= 0.0 :
                for i in month_list:
                    ret_str += ",0"
                return ret_str
            #若果最近半年逾期金额没有变化，后边所有日期逾期金额也不变化 
            #首次到期月份逾期金额，后边一直没有变化，认为后边所有日期逾期金额也不变化（表现月份>=3）
            elif len(this_line[-6]) > 0  and this_line[-6] == this_line[-1] and float(this_line[-1]) !=0 :
                for i in month_list :
                    ret_str += "," + this_line[-1]
            #首次到期月份逾期金额，后边一直没有变化，认为后边所有日期逾期金额也不变化（表现月份>=3）
            elif len(this_line[-3]) > 0  and (len(this_line[-4]) == 0 or float(this_line[-4]) <= 0.0) and this_line[-3] == this_line[-1] and float(this_line[-3]) >0:
                for i in month_list :
                    ret_str += "," + this_line[-1]
            #到期1至2个月账期情况，根据现有表现，及历史同类型表现，预估month_list日期所有逾期金额
            elif this_line[-3] == '' or this_line[-3] == '0.0'  or this_line[-3] == '0'  or float(this_line[-3]) <= 0.0:
                ret_list = self.get_month_rate_fun(fenqi_cycle, fenqi, order_month_diff)
                #print 'this_line:',this_line,',ret_list:',ret_list,',end:',ret_list[7:]
                ret_list = ret_list[7:]
                this_begin_num=1
                this_coefficient=1
                if len(this_line[-2]) > 0 and float(this_line[-2]) > 0.0:
                    this_coefficient = ( float(this_line[-1])/float(this_line[-2]) ) / ( float(ret_list[1])/float(ret_list[0]) )
                    this_begin_num=2
                for i,m_v in enumerate(month_list):
                    if (i + this_begin_num) <= len(ret_list):
                        value_tmp = (float(ret_list[i+this_begin_num]) / float(ret_list[i+this_begin_num-1])) * this_coefficient * float(this_line[-1])
                    else :
                        #根据最近几次还款情况，求出diff，对diff进行衰减，*0.7
                        value_tmp = (float(this_line[-1])  * 1.35 - float(this_line[-3])  * 0.35)
                    #value_tmp = 0 if value_tmp <= 0 else value_tmp
                    value_tmp = float(this_line[-1])*0.5 if value_tmp <= 0 else value_tmp
                    #if  len(this_line[-1]) > 0 and float(this_line[-1]) >1:
                    if this_line[-1] == '' or this_line[-1] == 0  or  this_line[-1]== "0" or this_line[-1]=="0.0":
                        pass
                    else:
                        value_tmp = value_tmp  if value_tmp < float(this_line[-1])  else  float(this_line[-1]) 
                    ret_str += "," + str(value_tmp)
                    this_line.append(value_tmp)
                #print 'ret_str:',ret_str
                #print 'here'
                pass
            #针对有还款3个月以上表现的数据，根据自身数据进行拟合，对最近三次的逾期情况求出相邻两次的diff，并求两个diff的均值，进行衰减（*0.7）作为下次diff
            else:
                for  i in month_list:
                    #value_tmp = float(this_line[len_tmp-3]) + 3 * (float(this_line[len_tmp-1]) - float(this_line[len_tmp-2]) )
                    if float(this_line[-4]) <= 0.0:
                        value_tmp = float(this_line[-1])  - (float(this_line[-2]) - float(this_line[-1])) * 0.6
                    else :
                        value_tmp = (float(this_line[-1])  * 1.35 - float(this_line[-3])  * 0.35)
                    #value_tmp = 0 if value_tmp <= 0 else value_tmp
                    value_tmp = float(this_line[-1])*0.5 if value_tmp <= 0 else value_tmp
                    #if  len(this_line[-1]) > 0 and float(this_line[-1]) >1 :
                    if this_line[-1] == '' or this_line[-1] == 0  or  this_line[-1]== "0" or this_line[-1]=="0.0":
                        pass
                    else:
                        value_tmp = value_tmp  if value_tmp <= float(this_line[-1])  else  float(this_line[-1]) 
                    ret_str += "," + str(value_tmp)
                    this_line.append(value_tmp)
                pass
        #账单到期月份 不小于 本月月份
        else:   #根据历史分期类型、账期类型，逾期表现预估接下来表现情况；  
            ''' 分两种情况：该类账单首期；该类账单非首期 '''
            ret_list = self.get_month_rate_fun(fenqi_cycle, fenqi, order_month_diff)
            ret_list = ret_list[7:]
            #print 'ret list:',ret_list
    
            this_tmp_rate=1.0
    
            for  i,m_v in enumerate(month_list):
                if deadline_dt > m_v:
                    ret_str += ",0"
                    this_line.append("0")
                    continue
                if int(float(order_month_diff)) == 0 :  #该类账单首期；  直接读取历史统计逾期率，进行预估逾期金额
                    #print 'here! 0'
                    if i < len(ret_list) :
                        value_tmp = float(ret_list[i]) * float(this_line[5])
                    else :
                        value_tmp = (float(this_line[-1])  * 1.35 - float(this_line[-3])  * 0.35)
                else :  #该类账单非首期    使用order_month_diff-1与 历史order_month_diff相对比，进行模型金额逾期率，进而求出逾期金额
                    #print 'here! !0'
                    #ret_last = self.get_all_new_order_bill_data_fun(order_dt, deadline_dt, fenqi_cycle, fenqi, order_month_diff, -1)
                    ret_last = self.get_all_new_order_bill_data_end(order_dt, deadline_dt, fenqi_cycle, fenqi, order_month_diff)
                    #print ret_last
                    this_diff_tmp=ret_last[0]
                    ret_list_tmp = self.get_month_rate_fun(fenqi_cycle, fenqi, str(this_diff_tmp))
                    ret_list_tmp = ret_list_tmp[7:]
                    #print 'ret_last:',ret_last,',this_line:',this_line,',ret_list:',ret_list,'len:',len(ret_list),len(ret_last),i
                    #print 'ret_last:',ret_last
                    if i < len(ret_list) and i < len(ret_last) :
                        #用上次还款的情况和历史统计情况来组合本次逾期率，进而计算逾期金额
                        #print 'ret_last:',ret_last,',ret_list_tmp:',ret_list_tmp,',ret_list:',ret_list
                        #print 'order_dt:',order_dt,',deadline_dt:',deadline_dt, ',fenqi_cycle:',fenqi_cycle, ',fenqi:', fenqi, ',order_month_diff:',order_month_diff,'ret_last:',ret_last,',ret_list_tmp:',ret_list_tmp,',ret_list:',ret_list
                        #this_tmp_rate = 1  if (float(ret_last[i]) / float(ret_list_tmp[i]) >= 1.0)  else (float(ret_last[i]) / float(ret_list_tmp[i]))  #2017-07-21
                        tmp_rate_one = (float(ret_last[i]) / float(ret_list_tmp[i]))
                        this_tmp_rate = 1  if (tmp_rate_one >= 1.0  or tmp_rate_one <= 0.0)  else tmp_rate_one
                        value_tmp = (float(ret_list[i]) * this_tmp_rate) * float(this_line[5])  #2017-07-24
                        #print 'order_dt:',order_dt,',deadline_dt:',deadline_dt, ',fenqi_cycle:',fenqi_cycle, ',fenqi:', fenqi, ',order_month_diff:',order_month_diff,'ret_last:',ret_last,',ret_list_tmp:',ret_list_tmp,',ret_list:',ret_list,',this_tmp_rate:',this_tmp_rate,',this rate:',float(ret_last[i]) / float(ret_list_tmp[i]),',i=',i,',ret_last[i]=',float(ret_last[i]),',ret_list_tmp[i]=',float(ret_list_tmp[i])
                        #value_tmp = (float(ret_last[i]) * 0.6 + float(ret_list[i]) * 0.4 ) * float(this_line[5])     #2017-07-21
                        #print 'this_tmp_rate:',this_tmp_rate,",value_tmp:",value_tmp
                    elif i < len(ret_list) and i >= len(ret_last) :
                        #历史统计情况来作为本次逾期率，进而计算逾期金额
                        this_tmp_rate=1.0
                        value_tmp = (float(ret_list[i]) * this_tmp_rate) * float(this_line[5])
                        #print 'check:',value_tmp,float(ret_list[i]),this_tmp_rate,value_tmp
                    else :
                        #根据自身情况，求出diff，对diff进行衰减，*0.7
                        value_tmp = (float(this_line[-1])  * 1.35 - float(this_line[-3])  * 0.35)
                #value_tmp = 0 if value_tmp <= 0 else value_tmp
                value_tmp = float(this_line[-1])*0.5  if value_tmp <= 0 else value_tmp
                #print 'this_line:',this_line
                #if  len(this_line[-1]) > 0 and float(this_line[-1]) >1:
                if this_line[-1] == '' or this_line[-1] == 0  or  this_line[-1]== "0" or this_line[-1]=="0.0":
                    pass
                else:
                    value_tmp = value_tmp  if value_tmp <= float(this_line[-1])  else  float(this_line[-1]) 
                ret_str += "," + str(value_tmp)
                this_line.append(str(value_tmp))
            pass
        return ret_str


    def get_month_rate_fun(self, fenqi_cycle, fenqi, order_month_diff):
        '''  获取历史各个分期账单不同到期月份的逾期率  '''
        #print fenqi_cycle, fenqi, order_month_diff,int(order_month_diff)
        #this_value=[]
        if int(order_month_diff) < 0:
            return []
        this_key=fenqi_cycle+","+fenqi+","+order_month_diff
        if self.dict_rate.has_key(this_key) :
            this_value = copy.deepcopy(self.dict_rate[this_key])
        else :
            this_value = self.get_month_rate_fun(fenqi_cycle, fenqi, str(int(order_month_diff)-1) )
        pass
        return this_value


    def  count_month_order_exposure(self, this_deal_dt):
        ''' #计算敞口  '''
        #计算敞口
        #dt ：需要计算敞口的月份末  exp dt="2017-07"
        #敞口分两部分：账单到账日期在dt之前的敞口  +   账单到账日期在dt的敞口
        #第一部分不需要考虑及计算在贷； 第二部分需要考虑该账单的在贷金额
        #根据预期时的逾期金额、账期、类型、时间等，倒退计算该预期金额对应的订单敞口金额
        #倒推在贷时，按时间分段：2016-08-01；2017-05-01。分成三段，由于2016-08-01~2017-05-01 变化不大，本次只考虑两段。
        #this_deal_dt="2017-06"
    
        #print 'self.df_all:',self.df_all_bill.head()
        df_1=self.df_all[(self.df_all['dt']<this_deal_dt) & (self.df_all['deadline_dt'] <this_deal_dt)]
        #把第二部分没有预期金额的过滤掉
        df_2=self.df_all[(self.df_all['dt']<=this_deal_dt) & (self.df_all['deadline_dt']==this_deal_dt) & (self.df_all[this_deal_dt] > 0)]
        #账单到账日期在dt之前的敞口
        #第一部分不需要考虑及计算在贷；
        old_changkou=0 if df_1[this_deal_dt].sum() is np.nan  or df_1[this_deal_dt].sum() == 0 else df_1[this_deal_dt].sum()
    
        #账单到账日期在dt的敞口
        #第二部分需要考虑该账单的在贷金额
        #当月到期敞口为空，或者为0
        if df_2[this_deal_dt].sum() is np.nan or df_2[this_deal_dt].sum() == 0:
            new_changkou=0
        else:
            df_2[['dt','deadline_dt','fenqi_cycle','fenqi','order_month_diff','capital_sum','all_orign_amount',this_deal_dt]].to_csv(self.project_temp_file, sep=',', encoding='utf-8', index=False)
            new_changkou = self.from_overdue_get_changkou_fun(self.project_temp_file)
        return [old_changkou,new_changkou]


    def  from_overdue_get_changkou_fun(self, this_deal_file):
        ''' 根据第二部分逾期金额计算第二部分敞口 '''
        ret_int=0.0
        with open(this_deal_file, 'r')  as  fd_in:
            lines=fd_in.readlines()
            for line in lines:
                this_line=line.strip('\n').split(',')
                if this_line[0] == "dt":
                    continue
                dt_tmp               = this_line[0]
                deadline_dt_tmp      = this_line[1]
                fenqi_cycle_tmp      = int(float(this_line[2]))
                fenqi_tmp            = int(float(this_line[3]))
                order_month_diff_tmp = int(float(this_line[4]))
                capital_sum_tmp      = this_line[5]
                all_orign_amount_tmp = this_line[6]
                this_overdue_capital = float(this_line[7])
                #分期1期的，逾期金额即敞口 不论周月订单，order_month_diff 是1或0
                if fenqi_tmp == 1:
                    ret_int += this_overdue_capital
                    #print this_line,',overdue changkou:',this_overdue_capital
                    continue
                #按照时间划分：2016-08-01之前，账单本金按照订单在贷/分期数
                order_month_diff_tmp = 1 if order_month_diff_tmp == 0 else order_month_diff_tmp
                order_month_diff_tmp = fenqi_tmp if order_month_diff_tmp > fenqi_tmp else order_month_diff_tmp
                if fenqi_tmp == 1:
                    ret_int +=  this_overdue_capital
                    #print this_line,',overdue changkou:',this_overdue_capital
                elif  fenqi_cycle_tmp == 1:
                    ret_int +=  (fenqi_tmp + 1 - order_month_diff_tmp) * this_overdue_capital
                    #print this_line,',overdue changkou:',(fenqi_tmp + 1 - order_month_diff_tmp) * this_overdue_capital
                else:
                    tp=str(fenqi_cycle_tmp) + "," + str(fenqi_tmp) + "," + str(order_month_diff_tmp)
                    ret_int += this_overdue_capital * float(self.dict_fenqi_rate[tp])
            pass
        pass
        return  ret_int 

