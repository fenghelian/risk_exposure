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

"""
计算逾期率
"""
class  FitParameters(FitModelLib):
    ''' 计算订单维度敞口 '''
    def __init__(self, config_file, this_bs):
        ''' '''
        conf = ConfigParser.ConfigParser()
        conf.read(config_file)
        self.path                                               = os.path.dirname(os.path.abspath(__file__))
        # 需要拟合参数的数据
        self.order_creaded_deadline_overdue_rate_file           = ("%s/../../%s" % (self.path, conf.get("config_data", "order_creaded_deadline_overdue_rate_file")))      #统计历史数据 订单创建时间、还款时间、逾期率
        self.parameters_order_deadline_overdue_rate_file        = ("%s/../../%s" % (self.path, conf.get("config_data", "parameters_order_deadline_overdue_rate_file")))   #订单创建日期、账单到期日期、拟合后的逾期率
        self.order_bill_cycle_fenqi_mean_overdue_file           = ("%s/../../%s" % (self.path, conf.get("config_data", "order_bill_cycle_fenqi_mean_overdue_file")))      #不同订单类型、不同期数逾期率（由近半年统计情况获得）
        self.parameters_cycle_fenqi_mean_overdue_file           = ("%s/../../%s" % (self.path, conf.get("config_data", "parameters_cycle_fenqi_mean_overdue_file")))      #不同订单类型、不同期数逾期率（由近半年统计情况获得）
        self.all_overdue_flow_data_file                         = ("%s/../../%s" % (self.path, conf.get("config_data", "jituan_overdue_flow_file")))                      #集团某天大盘逾期数据
        # dataframe
        self.all_bs_overdue                                     = {}   # new
        self.df                                                 = pd.DataFrame()        #订单创建时间、还款时间、逾期率
        self.df_all                                             = pd.DataFrame()        #不同订单类型、不同期数逾期率（由近半年统计情况获得）
        self.fenqi_cycle_fenqi_diff_history_weight              = 0.2
        self.fenqi_cycle_fenqi_diff_last_weight                 = 0.8
        self.parameters_exp_dict                                = {}
        self.parameters_all_dict                                = {}
        self.parameters_model_dict                              = {}
        self.this_bs                                            = this_bs
        print 'will count fitting parameters.....'

    def read_config_file_fun(self, this_file):
        ''' '''
        with open(this_file, 'r')  as fd_in:
            ret_list = [line.strip('\n').split(',')[0]  for line in fd_in if line[0] != '#' ]
        return ret_list

    def fitting_two_model_fun(self, model_flag, x_predict, this_dict, this_line):
        ''' 二次拟合参数 '''
        this_len=len(this_line)
        this_list=map(eval, this_line)
        for i in range(this_len+1, len(this_dict)+1, 1) :
            this_list.append(this_dict[i] * (1 + (this_dict[i-1] - this_dict[i])/this_dict[i-1]*(i-this_len+1)*0.005))
        y_data=np.array(this_list)
        return self.fitting_model_self(model_flag, x_predict, y_data, x_predict)



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



    def from_dict_to_list(self, ret_dict):
        ''' 把dict转化为list，并返回 '''
        return [t_v for t_k,t_v in ret_dict.iteritems()]

    def from_list_get_train_data(self, this_list):
        ''' 把list中数据进行过滤，返回训练数 '''
        #去掉空数据、负数据、小于下一个数据
        x_data=[]
        y_data=[]
        for i,v in enumerate(this_list,1):
            if len(v) > 0 :
                x_data.append(i)
                y_data.append(float(v))
            else :
                continue
        return np.array(x_data),np.array(y_data)
        pass

    def merge_two_list_line_one(self, base_list, new_list):
        ''' 根据base_list 生成new_list '''
        ret_list=[new_list[0]]
        for i,v in enumerate(base_list):
            if i == 0 :
                continue
            if len(v) < 1 :
                break
            this_value = (float(ret_list[i-1])/float(base_list[i-1])) * float(base_list[i])
            #print 'i:',i,',ret_list:',ret_list[i-1],',base_list:',base_list[i-1],',new:',base_list[i],',end:',this_value
            ret_list.append(this_value)
        return ret_list

    def from_dict_key_get_value_list(self, this_key, this_order):
        ''' '''
        x_data=[]
        y_list=[]
        for i in range(1,this_order):
            key_tmp = this_key + "," + str(i)
            #print 'key_tmp:',key_tmp,',i:',i
            if self.parameters_all_dict.has_key(key_tmp) : #   [",".join(this_line[0:4])]=ret_list
                #print self.parameters_all_dict[key_tmp]
                y_list.append(float(self.parameters_all_dict[key_tmp][0]))
                x_data.append([i])
        return x_data,y_list


    def fit_history_parameters_fun(self, this_key, this_order):
        ''' 针对没有表现的数据进行处理,多期数据，若只有一期返回空 '''
        #根据前几期d1表现情况，拟合出这期d1
        x_predict=np.arange(1,32,1)
        last_key=this_key + "," + str(this_order-1)
        #print 'this_key:',this_key,',this_order:',this_order,',last_order:',(this_order-1),',last_key:',last_key
        #print 'last_key:',self.parameters_model_dict[last_key]
        if this_order < 2 :
            return ['' for line in x_predict]
        if self.parameters_model_dict.has_key(last_key) :
            pass
        else :
            return ['' for line in x_predict]
        pass
        x_data,y_data = self.from_dict_key_get_value_list(this_key, this_order)
        x_predict = [[this_order]]
        #print 'this_key:',this_key,',this_order:',this_order,',x_data:',x_data,',y_data:',y_data,',x_predict:',x_predict
        ret_dict  = self.fitting_linear_model(x_data, y_data, x_predict)
        #print 'ret_dict:',ret_dict
        this_value = ret_dict[this_order] * (1.0 - this_order/120.0)
        fita = self.parameters_model_dict[last_key]
        last_list = self.parameters_all_dict[last_key]
        this_bias = (float(this_value) - float(last_list[0])) * 0.05 + fita[2]
        fita[2] = (this_bias  if this_bias > 0 else 0.0 )
        y_predict=np.arange(1,32,1)
        ret_dict = self.fitting_model_predict("inverse", y_predict, fita)
        self.parameters_model_dict[this_key + "," + str(this_order)] = fita
        ret_list    =  self.from_dict_to_list(ret_dict)

        if len(ret_list) < 1:
            return ['' for line in x_predict]
        return map(str, ret_list)


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



    def deal_null_data_fun(self):
        '''  处理订单创建时间、还款时间、逾期率  '''
        # 使用linearRegeression 拟合 历史总体统计情况
        this_columns=['d1_1', 'd1_2','d1_3']
        for i,v  in enumerate(this_columns):
            df_tmp = self.df_all[(self.df_all[v].isnull())].groupby(['type','fenqi_cycle','fenqi'])['order_month_diff'].min().reset_index()
            df_tmp.apply(lambda x: self.fit_history_linearregeression_fun(x, v), axis=1)
        self.df_all.sort_values(by=['type','fenqi_cycle','fenqi','order_month_diff'], axis=0, ascending=True, inplace=True, kind='quicksort', na_position='last')
        self.df_all.to_csv(self.parameters_cycle_fenqi_mean_overdue_file, sep=',', encoding='utf-8', index=False)
        # 使用后LinearRegeression 和 历史最近加权 拟合本月之前创建订单的逾期情况
        #self.df = self.df[(self.df['deadline_dt'] <= self.this_end_month)]
        print 'df:',len(self.df_all)
        #self.df.to_csv("result.csv", sep=',', encoding='utf-8', index=False)
        #sys.exit()
        pass


    def download_to_file(self):
        ''' '''
        self.df_all.sort_values(by=['type','fenqi_cycle','fenqi','order_month_diff'], axis=0, ascending=True, inplace=True, kind='quicksort', na_position='last')
        self.df_all.to_csv(self.parameters_cycle_fenqi_mean_overdue_file, sep=',', encoding='utf-8', index=False)

    def reload_file_dataframe(self, this_file):
        ''' '''
        df = pd.read_csv(this_file, sep=',', encoding='utf-8', error_bad_lines=False)
        return df

    def get_dataframe_columns_value(self, df, value_columns):
        ''' '''
        df_tmp = df[value_columns].sort_values(by=value_columns, axis=0, ascending=True, inplace=False, kind='quicksort', na_position='last').drop_duplicates(subset=value_columns).reset_index()
        return df_tmp[value_columns]

    def check_value(self, this_value):
        ''' '''
        this_value = this_value if this_value  < 1.0  else 1.0
        this_value = this_value if this_value  > 0.0  else 0.00001
        return this_value


    def get_product_overdue_list(self, df, this_len):
        ''' 拟合各产品、分期逾期率 '''
        # type,fenqi_cycle,fenqi,order_month_diff,deadline_dt,d1_1,d1_2
        ret_list    = []
        d1_columns=[]
        for i in range(1,182):
            d1_columns.append("d1_" + str(i))
        df.sort_values(by=['deadline_dt'], axis=0, ascending=False, inplace=True, kind='quicksort', na_position='last')
        #values_array  = df[(~df['d1_1'].isnull())][d1_columns].values.tolist()
        #d1_rate     = sum( [ float(row[0]) for row in values_array[0:this_len] ] ) / this_len
        #ret_list.append(d1_rate)
        #value_list    = [ row[1]/row[0]  for  row  in  df[(~df['d1_2'].isnull())][['d1_1','d1_2']].values.tolist()[0:this_len] ]
        #migration_rate = self.get_history_values(value_list)
        for k,v in enumerate(d1_columns):
            if k < 1 :
                tt = df[(~df[v].isnull()) & (df[v] > 0) & (df[v] < 1)][[v]].values.tolist()[0:this_len]
                value_list  = [ self.check_value(float(row[0])) for row in tt]
                if len(value_list) < 1:
                    break
                #value_list  = [ self.check_value(float(row[0])) for row in df[(~df[v].isnull()) & (df[v] > 0) & (df[v] < 1)][[v]].values.tolist()[0:this_len] ]
            else :
                last_v = d1_columns[k-1]
                value_array  = df[(~df[v].isnull()) & (df[v] > 0) & (df[v] < 1)][[last_v, v]].values.tolist()[0:this_len]
                if len(value_array) < 1 :   #若没有，用大盘数据填充
                    bs_list     = self.all_bs_overdue[self.this_bs]
                    value_list  = [ self.check_value(float(bs_list[k]) / float(bs_list[k-1])) ]
                else :
                    value_list  = [ self.check_value(float(row[1])/float(row[0])) for row in df[(~df[v].isnull()) & (df[v] > 0) & (df[v] < 1)][[last_v, v]].values.tolist()[0:this_len] ]
                pass
            pass
            this_rate = self.get_history_values(value_list)
            this_rate = self.check_value(this_rate)
            if k < 1 :
                ret_list.append(this_rate)
            else :
                ret_list.append( ret_list[k-1] * this_rate )
            pass
        pass
        return ret_list

    def get_model_fit_parameters(self, this_product, this_order):
        ''' '''
        this_list = this_product.split(',')
        this_type   = this_list[0]
        this_cycle  = this_list[1]
        this_fenqi  = this_list[2]
        if this_order < 3 :
            return []
        last_key    = this_product + "," + str(this_order-1)
        x_data,y_data = self.from_dict_key_get_value_list(this_product, this_order)
        x_predict = [[this_order]]
        ret_dict  = self.fitting_linear_model(x_data, y_data, x_predict)
        this_value = ret_dict[this_order] * 0.9998
        last_value = self.parameters_all_dict[last_key]
        this_coefficient    = this_value/last_value[0]
        return [ v * this_coefficient  for v in last_value ]


    def get_parameters_fit(self, this_product, this_order):
        ''' '''
        this_list   = this_product.split(',')
        this_type   = this_list[0]
        this_cycle  = this_list[1]
        this_fenqi  = this_list[2]

        ret_list=[]
        for i in range(this_order-1, this_order-3, -1):
            if i < 1 :
                break
            this_key = this_product + "," + str(i)
            if self.parameters_all_dict.has_key(this_key):
                ret_list.append( self.parameters_all_dict[this_key] )
        pass
        if len(ret_list) > 0 :
            wight_list  = [ 1.0/float(len(ret_list)) for i in range(len(ret_list)) ]
            ret_list    = self.get_list_from_lists_wight(ret_list, wight_list)
        else : #若为空用大盘数据代替
            ret_list    = self.all_bs_overdue[self.this_bs]
        #ret_list    = self.get_model_fit_parameters(this_product, this_order)
        #if len(ret_list) < 1 :
        #    last_key    = this_product + "," + str(this_order-1)
        #    if self.parameters_all_dict.has_key(last_key):
        #        ret_list = self.parameters_all_dict[last_key]
        #    pass
        #pass
        return ret_list

    def deal_abnormal_overdue_rate(self, t_v, product_list):
        this_type   = t_v[0]
        this_cycle  = t_v[1]
        this_fenqi  = t_v[2]
        this_order  = t_v[3]
        this_key = ','.join(map(str, t_v[0:3]))
        if self.parameters_all_dict.has_key(this_key + "," + str(this_order-1)) :
            pre_product_list = self.parameters_all_dict[this_key + "," + str(this_order-1)]
        else:
            return product_list
        for i in range(len(product_list)):
            if pre_product_list[i] > product_list[i]:
                if i <= 10:
                    product_list[i] = pre_product_list[i]*1.0
                elif i <= 50:
                    product_list[i] = pre_product_list[i]*1.02
                else:
                    product_list[i] = pre_product_list[i]*1.05
        return product_list

        


    def deal_mean_overdue_rate(self):
        ''' '''
        this_columns=['type','fenqi_cycle','fenqi','order_month_diff']
        d1_columns=[]
        for i in range(1,182):
            d1_columns.append("d1_" + str(i))
        fenqi_dict={}
        this_len    =  8
        fd = open(self.parameters_cycle_fenqi_mean_overdue_file, 'w+')
        fd.write(",".join(this_columns+d1_columns) + "\n")
        df_type_fenqi   = self.get_dataframe_columns_value(self.df_all, this_columns)
        for k,t_v   in enumerate(df_type_fenqi.values.tolist()):
            this_type   = t_v[0]
            this_cycle  = t_v[1]
            this_fenqi  = t_v[2]
            this_order  = t_v[3]
            df_rate  = self.get_product_overdue_dataframe(self.df_all, this_columns, t_v)
            if len(df_rate) < 1:
                continue
            product_list    = self.get_product_overdue_list(df_rate, this_len)
            if len(product_list) < 1:
                continue
            this_key = str(this_type) + "," + str(this_cycle) + "," + str(this_fenqi)
            if fenqi_dict.has_key(this_key):
                fenqi_dict[this_key].append(int(this_order))
            else :
                fenqi_dict[this_key] = [int(this_order)]
            if this_cycle == 2:
                if this_order > 1 :
                    product_list  = self.deal_abnormal_overdue_rate(t_v[0:4], product_list)
            self.parameters_all_dict[this_key + "," + str(this_order)]   = product_list
            fd.write(",".join(map(str, t_v)) + "," + ",".join(map(str,product_list)) + "\n")
        pass
        for  k,v  in fenqi_dict.iteritems():
            this_list = k.split(',')
            this_type   = this_list[0]
            this_cycle  = this_list[1]
            this_fenqi  = this_list[2]
            if this_fenqi == "1" :
                continue
            if max(v) >= int(this_fenqi) :
                continue
            for i in range(max(v)+1, int(this_fenqi) + 1, 1):
                #print 'i:',i,',k:',k
                ret_list = self.get_parameters_fit(k, i)
                if this_cycle == 2:
                    ret_list  = self.deal_abnormal_overdue_rate(this_list+[str(i)], ret_list)
                self.parameters_all_dict[k + "," + str(i)]=ret_list
                fd.write(k + "," + str(i) + "," + ",".join(map(str, ret_list))  + "\n")
        fd.close()
        pass



    def fit_parameter_main(self):
        ''' 拟合参数主函数  '''
        #读取文件
        self.df_all     = self.reload_file_dataframe(self.order_bill_cycle_fenqi_mean_overdue_file)
        self.all_bs_overdue  = self.read_file_to_dict(self.all_overdue_flow_data_file, 0, 1, 1, 999999999)
        self.deal_mean_overdue_rate()
        # 使用后LinearRegeression 和 历史最近加权 拟合本月之前创建订单的逾期情况
        #order_dt,deadline_dt,fenqi_cycle,fenqi,order_month_diff,capital_sum,num,all_num,all_orign_amount,d1_0,d1_1,d1_2,d1_3,d1_4,d1_5,d1_6,d1_7,d1_8,d1_9
        # 拟合后的参数写回到文件
        #self.download_to_file()





def main():
    ''' '''
    if len(sys.argv) < 2 :
        print  "python2.7 deal_mean_parameters.py  config_file  \"laifenqi/qudian\""
        sys.exit()
    config_file         = sys.argv[1]
    this_bs             = sys.argv[2]
    print 'deal mean parameters: config file:',config_file
    run = FitParameters(config_file, this_bs)
    run.fit_parameter_main()

if __name__=="__main__":
    main()



