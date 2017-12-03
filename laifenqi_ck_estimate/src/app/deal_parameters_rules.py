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
import ConfigParser
from datetime import datetime


from sklearn import linear_model

sys.path.append("./../lib/")

from fit_model_lib  import FitModelLib




class  RulesParameters(FitModelLib):
    ''' 计算订单维度敞口 '''
    def __init__(self, config_file, this_dt, this_bs):
        ''' '''
        conf = ConfigParser.ConfigParser()
        conf.read(config_file)
        self.path                                               = os.path.dirname(os.path.abspath(__file__))
        # 需要拟合参数的数据
        self.order_creaded_deadline_overdue_rate_file           = ("%s/../../%s" % (self.path, conf.get("config_data", "order_creaded_deadline_overdue_rate_file")))      #统计历史数据 订单创建时间、还款时间、逾期率
        self.parameters_order_deadline_overdue_rate_file        = ("%s/../../%s" % (self.path, conf.get("config_data", "parameters_order_deadline_overdue_rate_file")))   #订单创建日期、账单到期日期、拟合后的逾期率
        self.order_bill_cycle_fenqi_mean_overdue_file           = ("%s/../../%s" % (self.path, conf.get("config_data", "order_bill_cycle_fenqi_mean_overdue_file")))      #不同订单类型、不同期数逾期率（由近半年统计情况获得）
        self.parameters_cycle_fenqi_mean_overdue_file           = ("%s/../../%s" % (self.path, conf.get("config_data", "parameters_cycle_fenqi_mean_overdue_file")))      #不同订单类型、不同期数逾期率（由近半年统计情况获得）
        self.all_overdue_flow_data_file                         = ("%s/../../%s" % (self.path, conf.get("config_data", "jituan_overdue_flow_file")))                    #集团某天大盘逾期数据
        # dataframe 
        self.df                                                 = pd.DataFrame()        #订单创建时间、还款时间、逾期率
        self.df_all                                             = pd.DataFrame()        #不同订单类型、不同期数逾期率（由近半年统计情况获得）
        self.df_old                                             = pd.DataFrame()        #
        self.fenqi_cycle_fenqi_diff_history_weight              = 0.2
        self.fenqi_cycle_fenqi_diff_last_weight                 = 0.8
        self.this_data_dt                                       = this_dt
        self.parameters_exp_dict                                = {}
        self.parameters_all_dict                                = {}
        self.dict_cycle_overdue                                 = {}
        self.dt_parameter_dict                                  = {}
        self.all_bs_overdue                                     = {}   # new
        self.this_bs                                            = this_bs
        print 'will count fitting parameters.....'


    def fitting_two_model_fun(self, model_flag, x_predict, this_dict, this_line):
        ''' 二次拟合参数 '''
        this_len=len(this_line)
        this_list=map(eval, this_line)
        for i in range(this_len+1, len(this_dict)+1, 1) :
            this_list.append(this_dict[i] * (1 + (this_dict[i-1] - this_dict[i])/this_dict[i-1]*(i-this_len+1)*0.005))
        y_data=np.array(this_list)
        return self.fitting_model_self(model_flag, x_predict, y_data, x_predict)

    def fit_less_data_parameters_fun(self, this_diff, fita, this_line, x_predict):
        ''' 根据上一个账期的数据表现，和这次进行加权拟合，上次数据参数  '''
        this_fit = fita[0:2]
        this_fit = np.array([fita[0],fita[1],fita[2]+this_diff])
        this_dict = self.fitting_model_predict("exp", x_predict, this_fit)
        ret_dict,fita = self.fitting_two_model_fun("exp", x_predict, this_dict, this_line)
        if len(ret_dict) > 1:
            this_dict=ret_dict
            this_fit =fita
        return this_dict,this_fit

    def this_dt_add_per_value(self, nm, dt_tmp=""):
        ''' 根据不同日期粒度，返回响应增加nm后的日期 '''
        dt_tmp= self.this_data_dt  if len(dt_tmp) < 4 else dt_tmp 
        this_tmp = (datetime.strptime(dt_tmp, "%Y-%m-%d")  + relativedelta(days=(nm))).strftime("%Y-%m-%d")
        return this_tmp


    def check_list_function(self, this_list):
        ''' 检查list 若出现后边大于前边拟合 '''
        if len(this_list) < 3:
            return this_list
        for i in range(len(this_list)-1):
            if float(this_list[i]) > float(this_list[i+1]):
                continue
            elif this_list[i] == this_list[i+1]:
                this_list[i+1] == float(this_list[i]) * 0.98
            elif float(this_list[i]) < float(this_list[i+1]):
                if i == 0 :
                    this_list[i] = float(this_list[i+1])/0.548
                else :
                    this_list[i+1] = (float(this_list[i]) * 0.98)
            else :
                pass
        return this_list



    def fit_history_linearregeression_fun(self, this_line, this_column):
        ''' '''
        #fenqi_cycle    fenqi   order_month_diff
        df_tmp  = self.df_all[(self.df_all['type'] == this_line['type']) & (self.df_all['fenqi_cycle'] == this_line['fenqi_cycle']) & (self.df_all['fenqi'] == this_line['fenqi']) & (self.df_all['order_month_diff'] > 0)]
        #print 'this df_tmp:',df_tmp
        x_data = [[x] for x in df_tmp[(~df_tmp[this_column].isnull())]['order_month_diff'].tolist()]
        y_data = [x for x in df_tmp[(~df_tmp[this_column].isnull())][this_column].tolist()]
        x_predict = [[x] for x in df_tmp[(df_tmp[this_column].isnull())]['order_month_diff'].tolist()]
        #调用后函数训练，预测，并返回结果
        #print 'this_column:',this_column,',x_data:',x_data,',y_data:',y_data,',x_predict:',x_predict
        this_begin=len(x_data)
        if len(x_predict) < 1:
            return
        ret_dict  = self.fitting_linear_model(x_data, y_data, x_predict)
        #
        ret_len = len(ret_dict)
        if ret_len < 3:
            this_per = 0.04
        elif ret_len >= 3  and ret_len < 5:
            this_per = 0.03
        else :
            this_per = 0.02
        for (k,v) in ret_dict.iteritems():
            ret_dict[k] = float(v) * (1.0 - (int(k) - this_begin) * this_per)
        #print 'this_line:',this_line.tolist(),',this_column:',this_column
        #print 'ret_dict:',ret_dict
        # 拟合后的结果，覆盖原来数据
        for (k,v) in ret_dict.iteritems():
            #print 'k:',k,',v:',v
            self.df_all.ix[(self.df_all['type'] == this_line['type']) & (self.df_all['fenqi_cycle'] == this_line['fenqi_cycle']) & (self.df_all['fenqi'] == this_line['fenqi']) & (self.df_all['order_month_diff'] == int(k)), this_column] = v
        pass



    def first_full_fillna_fun(self):
        '''  处理订单创建时间、还款时间、逾期率  '''
        # 使用后LinearRegeression 和 历史最近加权 拟合本月之前创建订单的逾期情况
        #this_list="d1_1,d1_2,d1_3,d1_4,d1_5,d1_6,d1_7,d1_8,d1_9,d1_10,d1_11,d1_12,d1_13,d1_14,d1_15,d1_16,d1_17,d1_18,d1_19,d1_20,d1_21,d1_22,d1_23,d1_24,d1_25,d1_26,d1_27,d1_28,d1_29,d1_30,d1_31".split(',')
        this_list=[]
        this_overdue_len=181
        for i in range(1,this_overdue_len+1):
            this_list.append("d1_" + str(i))
        this_last_coumns=this_list[-1]
        df_0 = pd.DataFrame()
        for i,m_v  in  enumerate(this_list, 1):
            #print 'i:',i,',m_v:',m_v
            this_dt = self.this_dt_add_per_value(-i)
            next_dt = self.this_dt_add_per_value(-(i+1))
            #print 'this_dt:',this_dt,',next_dt:',next_dt,',deadline_dt:',self.df['deadline_dt'].tolist()
            #df_tmp  = self.df[(self.df['deadline_dt'] == this_dt) & (self.df[m_v].isnull())].fillna(0)
            #df_tmp  = self.df[(self.df['deadline_dt'] == next_dt) & (self.df[m_v] == 0.0)].fillna(0)
            df_tmp  = self.df[(self.df['deadline_dt'] == this_dt) & (self.df[m_v] == 0.0)].fillna(0)
            if len(df_0) == 0:
                df_0 = df_tmp
            else :
                df_0 = pd.concat([df_0, df_tmp])
            del df_tmp
        pass
        #print 'end:',self.this_dt_add_per_value(-31)
        df_tmp    = self.df[(self.df['deadline_dt'] < self.this_dt_add_per_value(this_overdue_len * (-1)))].fillna(0)
        df_0    = pd.concat([df_0, df_tmp])
        df_1    = self.df.drop(df_0.index) 
        self.df = pd.concat([df_0, df_1]).sort_values(by=['order_dt','deadline_dt','type','fenqi_cycle','fenqi','order_month_diff'], axis=0, ascending=True, inplace=False, kind='quicksort', na_position='last')
        #print 'df:',self.df
        self.df_old = self.df[~(self.df[this_last_coumns].isnull())]
        self.df_new = self.df[(self.df[this_last_coumns].isnull())]
        #print 'df:',self.df[(self.df['order_dt']=="2016-09-19") & (self.df['deadline_dt']=="2017-08-18") & (self.df['type']==6) & (self.df['fenqi_cycle']==2) & (self.df['fenqi']==12)]
        del df_0,df_1
        #self.df = self.df[(self.df['deadline_dt'] <= self.this_end_month)]
        print 'df:',len(self.df)
        #self.df.to_csv("result.csv", sep=',', encoding='utf-8', index=False)
        #sys.exit()
        pass


    def check_select_max_list(self, one_list, two_list):
        ''' 对比两个list，返回最长一个 '''
        #print 'one_list:',one_list,',two_list:',two_list
        if len(one_list) * len(two_list) == 0 :
            return one_list if len(one_list) > 0 else two_list
        for i,v  in enumerate(one_list):
            if float(v) > 0 and len(two_list[i]) < 1:
                return one_list
            elif float(v) > 0 and len(two_list[i]) > 0:
                continue
            else :
                return one_list
            pass
        return one_list
        pass



    def download_to_file(self):
        ''' '''
        self.df.to_csv(self.parameters_order_deadline_overdue_rate_file, sep=',', encoding='utf-8', index=False)
        self.df_all.to_csv(self.parameters_cycle_fenqi_mean_overdue_file, sep=',', encoding='utf-8', index=False)

    #this_list = self.check_mean_list_data_fun(cycle_key, this_list)
    def check_mean_list_data_fun(self, this_line, this_list, mean_list):
        ''' 检查list数据，现金逾期, '''
        list_tmp =[]
        this_begin=10
        for i in range(32):
            if len(this_line[i+this_begin]) > 0:
                list_tmp.append(this_line[i+this_begin])
        if float(this_list[0]) > 0.1  and this_line[2] == '5' :
            if len(list_tmp) > 0 and (float(list_tmp[-1]) - float(this_list[0])) / float(list_tmp[-1])  > 0.5:
                pass
        this_num = this_line

    def get_list_rule_overdue_rate(self, this_line):
        ''' 按照规则特殊处理,并补充后期逾期率走势 '''
        this_num    = int(this_line[7])
        overdue_list    = this_line[10:]
        ret_list    = []
        value_len    = self.get_list_last_value_locat(overdue_list)  # 获取list，value 不为空的最后一个位置，从1开始计数
        #print 'this_line:',this_line
        #print 'overdue_list:',overdue_list
        #print 'value_len:',value_len
        if value_len < 1:
            pass
        #最后逾期为0，全部填充为0
        elif float(overdue_list[value_len-1]) == 0.0:
            ret_list = self.fillna_list_self_value(overdue_list)
        #  若订单人数 <= 10 ， 且2天没有变化，认为后期一直不变
        elif this_num <= 10 and value_len > 1:
            #按照最后一个值进行填充
            if overdue_list[value_len-2] == overdue_list[value_len-1]:
                ret_list = self.fillna_list_self_value(overdue_list)
        #  若订单人数 <= 50 ， 且4天没有变化，认为后期一直不变
        elif this_num <= 50 and value_len > 3:
            #按照最后一个值进行填充
            if overdue_list[value_len-4] == overdue_list[value_len-1]:
                ret_list = self.fillna_list_self_value(overdue_list)
        #  若订单人数 <= 100 ， 且6天没有变化，认为后期一直不变
        elif this_num <= 100 and value_len >5:
            #按照最后一个值进行填充
            if overdue_list[value_len-6] == overdue_list[value_len-1]:
                ret_list = self.fillna_list_self_value(overdue_list)
        else :
            pass
        #print 'this_line:',this_line
        #print 'fillna_list:',ret_list
        return ret_list


    def get_overdue_rate_near_date(self, this_line, this_len):
        ''' 从最近一段时间（this_len)获取相同产品的逾期率，求均值并返回 '''
        # 从begin_dt 开始向前取 this_len时间的数据，且用户数量大于5000
        # order_dt,deadline_dt,type,fenqi_cycle,fenqi,order_month_diff,capital_sum,num,all_num,all_orign_amount 
        ret_list=[]
        this_num = 100   # 取历史订单数量大于num的逾期率
        begin_dt=this_line[0]
        cycle_key  = this_line[2] + "," + this_line[3] +  "," + this_line[4] + "," + this_line[5]
        df_tmp = self.df[(self.df['order_dt'] < this_line[0]) & (self.df['type']==int(this_line[2])) & (self.df['fenqi_cycle']==int(this_line[3])) & (self.df['fenqi']==int(this_line[4])) & (self.df['order_month_diff']==int(this_line[5]))].tail(this_len+2)
        df_tmp = df_tmp[(df_tmp['num'] > this_num)]
        if len(df_tmp) < 1 :
            pass
        else :
            tmp_len = len(df_tmp)  if len(df_tmp) < this_len  else this_len
            this_array = [ v[10:] for v in df_tmp.tail(tmp_len).values.tolist() ]
            wight_list = [ 1.0/float(tmp_len) for i in range(tmp_len) ]
            ret_list = self.get_list_from_lists_wight(this_array, wight_list)
        return ret_list

    def get_same_type_fenqi_last_value(self, this_line):
        ''' 获取与传入line最相近的逾期数据 '''
        order_dt    =  this_line[0]
        deadline_dt =  this_line[1]
        this_type   =  int(this_line[2]) 
        fenqi_cycle =  int(this_line[3])
        fenqi       =  int(this_line[4])
        order_month_diff = int(this_line[5])
        ret_list=[]
        if order_month_diff >  1:
            df_tmp = self.df[(self.df['order_dt'] == order_dt) & (self.df['type']==this_type) & (self.df['fenqi_cycle']==fenqi_cycle) & (self.df['fenqi']==fenqi) & (self.df['order_month_diff']==(order_month_diff-1))]
        else :
            df_tmp = self.df[(self.df['order_dt'] < order_dt) &(self.df['type']==this_type) & (self.df['fenqi_cycle']==fenqi_cycle) & (self.df['fenqi']==fenqi) & (self.df['order_month_diff']==order_month_diff)]
        pass
        if len(df_tmp) < 1:
            df_tmp = self.df[(self.df['order_dt'] <= order_dt) & (self.df['type']==this_type) & (self.df['fenqi_cycle']==fenqi_cycle) & (self.df['fenqi']==fenqi)]
        if len(df_tmp) == 1:
            ret_list = df_tmp.values.tolist()[0][10:]
        elif len(df_tmp) > 1:
            this_array = [ v[10:] for v in df_tmp.tail().values.tolist() ]
            wight_list = [ 1.0/float(len(this_array)) for i in range(len(this_array)) ]
            ret_list = self.get_list_from_lists_wight(this_array, wight_list)
        else :
            df_tmp = self.df[(self.df['order_dt'] <= order_dt) & (self.df['type']==this_type) & (self.df['fenqi_cycle']==fenqi_cycle) & (self.df['fenqi']==(fenqi - 1))]
            this_array = [ v[10:] for v in df_tmp.tail().values.tolist() ]
            wight_list = [ 1.0/float(len(this_array)) for i in range(len(this_array)) ]
            ret_list = self.get_list_from_lists_wight(this_array, wight_list)
        return ret_list


    def rewrite_dataframe_value(self, this_line, value_list):
        ''' 根据line数据，把value list 值写回df中  '''
        order_dt    =  this_line[0]
        deadline_dt =  this_line[1]
        this_type   =  int(this_line[2]) 
        fenqi_cycle =  int(this_line[3])
        fenqi       =  int(this_line[4])
        order_month_diff = int(this_line[5])
        for i,v in enumerate(value_list, 1):
            this_column="d1_" + str(i)
            self.df.ix[(self.df['order_dt'] == order_dt) & (self.df['deadline_dt'] == deadline_dt)  & (self.df['type'] == this_type) & (self.df['fenqi_cycle'] == fenqi_cycle) & (self.df['fenqi'] == fenqi) & (self.df['order_month_diff'] == order_month_diff), this_column] = float(v)
        pass
        #print 'df:',self.df[(self.df['order_dt'] == order_dt) & (self.df['deadline_dt'] == deadline_dt)  & (self.df['type'] == this_type) & (self.df['fenqi_cycle'] == fenqi_cycle) & (self.df['fenqi'] == fenqi) & (self.df['order_month_diff'] == order_month_diff)]


    def rules_fillna_overdue_data_fun(self):
        ''' '''
        #self.df_new.to_csv("result.csv", sep=',', encoding='utf-8', index=False)
        #self.df_new.to_csv("result.csv_test", sep=',', encoding='utf-8', index=False)
        #self.df_old.to_csv("this_test_order_overdue.csv", sep=',', encoding='utf-8', index=False)
        #fd = open("this_test_order_overdue.csv", 'a+')
        self.df_old.to_csv(self.parameters_order_deadline_overdue_rate_file, sep=',', encoding='utf-8', index=False)
        fd = open(self.parameters_order_deadline_overdue_rate_file, 'a+')

        #order_dt,deadline_dt,type,fenqi_cycle,fenqi,order_month_diff,capital_sum,num,all_num,all_orign_amount,d1_0,d1_1,d1_2,d1_3,d1_4,d1_5,d1_6,d1_7,d1_8,d1_9
        #columns_list="d1_0,d1_1,d1_2,d1_3,d1_4,d1_5,d1_6,d1_7,d1_8,d1_9".split(',')
        #columns_list="d1_1,d1_2,d1_3,d1_4,d1_5,d1_6,d1_7,d1_8,d1_9,d1_10,d1_11,d1_12,d1_13,d1_14,d1_15,d1_16,d1_17,d1_18,d1_19,d1_20,d1_21,d1_22,d1_23,d1_24,d1_25,d1_26,d1_27,d1_28,d1_29,d1_30,d1_31".split(',')
        columns_list=[ "d1_" + str(i) for i in range(1,32)]
        #with open("result.csv_test", 'r')   as fd_in :
        #    lines   = fd_in.readlines()
        #    for line in lines:
        for line in self.df_new.fillna('').values.tolist():
            #this_line   =  line.strip('\n').split(',')
            this_line   =  map(str, line)
            order_dt    =  this_line[0]
            deadline_dt =  this_line[1]
            this_type   =  this_line[2]
            fenqi_cycle =  this_line[3]
            fenqi       =  this_line[4]
            order_month_diff = this_line[5]
            capital_sum =  this_line[6]
            num         =  this_line[7]
            all_num     =  this_line[8]
            all_orign_amount  = this_line[9]
            #print 'order_dt:',order_dt,',deadline_dt:',deadline_dt,',type:',this_type,',fenqi_cycle:',fenqi_cycle,',fenqi:',fenqi,',order_month_diff:',order_month_diff,',capital_sum:',capital_sum,',num:',num,',all_num:',all_num,',all_orign_amount:',all_orign_amount

            if "type" in this_line:
                continue
            # 规则填充处理
            fillna_list = self.get_list_rule_overdue_rate(this_line)
            if len(fillna_list) > 0:
                #print 'here!'
                #self.rewrite_dataframe_value(this_line, fillna_list)
                fd.write(",".join(this_line[0:10]) + "," + ",".join(map(str,fillna_list)) + "\n")
                continue
            this_list  = this_line[10:]
            # 其他方法填充
            cycle_key  = this_type + "," + fenqi_cycle +  "," + fenqi + "," + order_month_diff 
            last_cycle_key  = this_type + "," + fenqi_cycle +  "," + fenqi + "," + str(int(order_month_diff) -1 )
            #print 'cycle_key:',cycle_key
            near_list=[]
            last_overdue=[]
            ## 根据前n段时间的逾期数据，同产品，同账期，拟合一条曲线
            #near_list = self.get_overdue_rate_near_date(this_line[0:10], 7)
            ## 获取同产品，上期填充逾期数据
            #last_overdue = self.get_same_type_fenqi_last_value(this_line[0:10])
            # 获取同产品同期历史逾期数据
            if self.dict_cycle_overdue.has_key(cycle_key):
                mean_list  = self.dict_cycle_overdue[cycle_key]
            else :
                mean_list  = []
            # 获取同产品上期历史逾期数据
            if self.dict_cycle_overdue.has_key(last_cycle_key):
                last_mean  = self.dict_cycle_overdue[last_cycle_key]
            else :
                last_mean  = []
            pass
            last_list=[]
            if len(mean_list) < 1:
                if float(order_month_diff) > 1.0 :
                    #print 'test:',self.df[(self.df['order_dt'] == order_dt) & (self.df['type'] == int(this_type)) & (self.df['fenqi_cycle'] == int(fenqi_cycle)) & (self.df['fenqi'] == int(fenqi)) & (self.df['order_month_diff'] == int(order_month_diff)-1)][columns_list].values.tolist()[0]
#                    for i in range(int(order_month_diff)-1):
#                        this_diff = int(order_month_diff) - 1 -i
#                        if this_diff < 1 :
#                            break
                    df_tmp = self.df[(self.df['order_dt'] == order_dt) & (self.df['type'] == int(this_type)) & (self.df['fenqi_cycle'] == int(fenqi_cycle)) & (self.df['fenqi'] == int(fenqi)) & (self.df['order_month_diff'] == int(order_month_diff)-1) & (~self.df['d1_1'].isnull())][columns_list]
                    if len(df_tmp) > 0:
                        last_list=df_tmp.values.tolist()[0]
                        #last_list = self.df[(self.df['order_dt'] == order_dt) & (self.df['type'] == int(this_type)) & (self.df['fenqi_cycle'] == int(fenqi_cycle)) & (self.df['fenqi'] == int(fenqi)) & (self.df['order_month_diff'] == int(order_month_diff)-1) & (~self.df['d1_0'].isnull())][columns_list].values.tolist()[0]
#                        if len(last_list) > 0 and last_list[0] > 0 :
#                            break
                    if  len(last_list) > 0 and last_list[0] > 0 :
                        pass
                    else :
#                        for i in range(int(order_month_diff)-1):
#                            this_diff = int(order_month_diff) - i
#                            if this_diff < 1 :
#                                break
                            #dt_tmp = self.df_old[(self.df_old['deadline_dt'] < self.this_data_dt) & (self.df_old['type'] == int(this_type)) & (self.df_old['fenqi_cycle'] == int(fenqi_cycle)) & (self.df_old['fenqi'] == int(fenqi)) & (self.df_old['order_month_diff'] == int(order_month_diff)-1) & (~self.df_old['d1_2'].isnull())][columns_list]
                        dt_tmp = self.df[(self.df['deadline_dt'] < self.this_data_dt) & (self.df['type'] == int(this_type)) & (self.df['fenqi_cycle'] == int(fenqi_cycle)) & (self.df['fenqi'] == int(fenqi)) & (self.df['order_month_diff'] == int(order_month_diff)-1) & (~self.df['d1_2'].isnull())][columns_list]
                        if len(dt_tmp) > 0:
                            last_list=dt_tmp.values.tolist()[-1]
                            #print self.df[(self.df['deadline_dt'] < self.this_data_dt) & (self.df['type'] == int(this_type)) & (self.df['fenqi_cycle'] == int(fenqi_cycle)) & (self.df['fenqi'] == int(fenqi)) & (self.df['order_month_diff'] == int(order_month_diff)-1)][columns_list].values.tolist()
                            #print 'last_list:',last_list
#                           if len(last_list) > 0 and last_list[0] > 0 :
#                                break
                    #last_list = self.df[(self.df['order_dt'] == order_dt) & (self.df['type'] == this_type) & (self.df['fenqi_cycle'] == fenqi_cycle) & (self.df['fenqi'] == fenqi) & (self.df['order_month_diff'] == str(int(order_month_diff)-1))][columns_list].tolist()
                #if  len(last_list) < 1 and len(mean_list) < 1:
                #    end_list  = self.dict_cycle_overdue[cycle_key] 
            pass
            #duibi_list  = mean_list  if len(mean_list) > 0 else last_list
            duibi_list  = map(str,self.check_select_max_list(last_list, mean_list))
            if len(duibi_list) < 1 :
                duibi_list = self.all_bs_overdue[self.this_bs]
            #print 'mean_list:',mean_list,',last_list:',last_list,',last_mean:',last_mean,',duibi list:',duibi_list
            #print 'this_list:',this_list,',near_list:',near_list,',last_overdue:',last_overdue
            this_diff=0.0
            # 有d1的数据，根据历史迁徙率补全数据
            if len(this_list[0]) > 0 :
                this_diff=0.0
                this_coefficient=1.0
                #print this_list
                for i,v in enumerate(this_list):
                    #if len(v) > 0 and i < len(duibi_list) and len(duibi_list[i]) > 0:
                    #    this_diff = float(v) - float(duibi_list[i])
                    if i == 0 or len(v) > 0:
                        continue
                        #this_list[i] = float(v)
                    elif i > 0 and i < len(duibi_list) and len(duibi_list[i]) > 0:
                        this_coefficient=float(duibi_list[i])/float(duibi_list[i-1])
                        this_list[i] = float(this_list[i-1]) * this_coefficient
                    else :
                        this_list[i] = float(this_list[i-1]) - (float(this_list[i-1])  - float(this_list[i-2])) * 0.90
                        #continue
                    #print 'this_diff:',this_diff,',diff value:',float(duibi_list[i]) + this_diff  if  len(duibi_list[i]) > 0  else v
                    #this_list[i] = float(duibi_list[i]) + this_diff  if  len(duibi_list[i]) > 0  else v 
                    #this_list[i] = float(duibi_list[i]) + this_diff  if  len(duibi_list[i]) > 0  else v 
                pass
            # 没有d1的数据，根据最近均值、同产品上个逾期数据，补全数据
            else :
                if len(last_mean) > 0 and len(last_list) > 0 :
                    this_diff=0.0
                    #print 'mean_list:',mean_list,',last_list:',last_list,',last_mean:',last_mean,',duibi list:',duibi_list,',last list:',last_list[0],',duibi :',last_list[0] > 0
                    for i,v in enumerate(mean_list):
                        if last_list[i] > 0 and len(last_mean[i]) > 0 :
                            this_diff = (float(last_list[i]) - float(last_mean[i]))
                        if len(v) >0 :
                            this_list[i] = float(v) + this_diff # (float(last_list[i]) - float(last_mean[i]))
                # 用最近7天的拟合逾期率，代替未来
                elif len(near_list) > 0 :
                    this_list = near_list
                # 用最近相近产品的拟合逾期率，代替未来
                elif len(last_overdue) > 0:
                    this_list = last_overdue
                # 用最近一段时间的拟合逾期率，代替未来
                elif len(duibi_list) > 0 :
                    this_list = duibi_list
                # 用大盘逾期率数据，代替未来
                else :
                    this_list = self.all_bs_overdue[self.this_bs]
                pass
            pass
            #sys.exit()
            #if  float(num) > 1000 and len(mean_list) > 1:
            #    this_list = self.check_mean_list_data_fun(cycle_key, this_list, mean_list)
            #    sys.exit()
            #self.rewrite_dataframe_value(this_line, this_list)
            fd.write(",".join(this_line[0:10]) + "," + ",".join(map(str,this_list)) + "\n")
        pass


    def reload_file_data(self):
        ''' 读取文件  '''
        # 订单创建时间、还款时间、逾期率
        #test_file="this_test.txt"
        #self.df                     =   pd.read_csv(test_file, sep=',', encoding='utf-8')
        #print 'df:',self.df
        self.df                     =   pd.read_csv(self.order_creaded_deadline_overdue_rate_file, sep=',', encoding='utf-8')
        #不同订单类型、不同期数逾期率（由近半年统计情况获得）
        #type,fenqi_cycle,fenqi,order_month_diff,d1_0,d1_1,d1_2,d1_3,d1_4,d1_5
        with open(self.parameters_cycle_fenqi_mean_overdue_file, 'r')   as fd_in:
            self.dict_cycle_overdue = {",".join(line.strip('\n').split(',')[0:4]):line.strip('\n').split(',')[4:] for line in fd_in if line[0:4] != "type"}
        pass
        # 读取大盘逾期数据
        with open(self.all_overdue_flow_data_file, 'r')     as fd_in:
            self.all_bs_overdue  = { ",".join(line.strip('\n').split(',')[0:1]):line.strip('\n').split(',')[1:]  for line in fd_in  if line[0:2] != "bs" }
        pass


    def deal_rules_main(self):
        '''  主要依靠规则集计算逾期参数  '''
        #读取文件
        self.reload_file_data()
        #初步处理补充
        self.first_full_fillna_fun()
        #规则集处理数据
        self.rules_fillna_overdue_data_fun()
        #




def main():
    ''' '''
    if len(sys.argv) < 2 :
        print  "python2.7 deal_parameters_rules.py  config_file "
        sys.exit()
    config_file         = sys.argv[1]
    this_bs             = sys.argv[2]
    print 'deal parameter rules: config file:',config_file
    if len(sys.argv) > 3:
        print 'len:',len(sys.argv),',sys:',sys.argv
        this_dt = sys.argv[3]
    else :
        this_dt = datetime.now().strftime("%Y-%m-%d")   #本月月份  本月及之后的月份账单表重新生成，之前的无需重新生成
    print 'this_dt:',this_dt
    run = RulesParameters(config_file, this_dt, this_bs)
    run.deal_rules_main()

if __name__=="__main__":
    main()



