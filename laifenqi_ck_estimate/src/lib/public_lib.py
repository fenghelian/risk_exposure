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




class  PublicLib(object):
    ''' 自己实现的函数库 '''
    def __init__(self):
        ''' '''
        self.this_type                      = "all"
        self.this_fenqi_cycle               = "all"
        self.this_fenqi                     = "all"
        print 'will deal list dict init .....'

    def get_dealdt_type(self, nm, dt_tmp=""):
        ''' 根据不同日期粒度，返回相应日期 '''
        # 1 月粒度，返回月末一天
        dt_tmp= self.this_dt  if len(dt_tmp) < 4 else dt_tmp
        this_tmp=""
        if self.deal_dt_type == "month" :
            this_tmp = (datetime.strptime(dt_tmp[0:7], "%Y-%m")  + relativedelta(months=(nm+1)) + relativedelta(days=(-1))).strftime("%Y-%m-%d")
        elif self.deal_dt_type == "week":
            this_tmp = (datetime.strptime(dt_tmp, "%Y-%m-%d")  + relativedelta(weeks=(nm))).strftime("%Y-%m-%d")
        elif self.deal_dt_type == "day" :
            this_tmp = (datetime.strptime(dt_tmp, "%Y-%m-%d")  + relativedelta(days=(nm))).strftime("%Y-%m-%d")
        else :
            print '计算日期粒度(跨度)有问题，只支持：day、week、month三种'
            sys.exit()
        return this_tmp





    def this_dt_add_per_value(self, nm, dt_tmp=""):
        ''' 根据不同日期粒度，返回相应增加nm后的日期 '''
        dt_tmp= self.this_dt  if len(dt_tmp) < 4 else dt_tmp
        this_tmp = (datetime.strptime(dt_tmp, "%Y-%m-%d")  + relativedelta(days=(nm))).strftime("%Y-%m-%d")
        #if self.this_dt_type == "month" :
        #    this_tmp = (datetime.strptime(dt_tmp, "%Y-%m")  + relativedelta(months=(nm))).strftime("%Y-%m")
        #elif self.this_dt_type == "week":
        #    this_tmp = (datetime.strptime(dt_tmp, "%Y-%m-%d")  + relativedelta(weeks=(nm))).strftime("%Y-%m-%d")
        #elif self.this_dt_type == "day" :
        #    this_tmp = (datetime.strptime(dt_tmp, "%Y-%m-%d")  + relativedelta(days=(nm))).strftime("%Y-%m-%d")
        #else :
        #    print '计算日期粒度(跨度)有问题，只支持：day、week、month三种'
        #    sys.exit()
        return this_tmp

    def check_type_cycle_fenqi_fun(self, this_line):
        ''' 检查是否是需要跳过的产品  '''
        # 返回0：相同产品，不需要跳过
        ret_flag = 1
        if self.this_type == "all" or self.this_type == str(this_line[0]) :
            ret_flag = 0
        if self.this_fenqi_cycle == "all" or self.this_fenqi_cycle == str(this_line[1]) :
            ret_flag = 0
        if self.this_fenqi == "all" or self.this_fenqi == str(this_line[2]):
            ret_flag = 0
        return ret_flag


    def get_list_from_lists_wight(self, this_array, wight_list):
        ''' 根据传入的array，并按照权重求出响应的值 '''
        # 若权重长度小于 传入的二维数组，返回空
        if len(this_array) == 1:
            return this_array[0]
        if len(this_array) != len(wight_list):
            return []
        ret_list=[]
        #print 'this_array:',this_array,',wight_list:',wight_list
        for k,l_v in enumerate(this_array):
            for i,v in enumerate(l_v):
                if len(ret_list) <= i:
                    ret_list.append( float(v) * float(wight_list[k]) )
                else :
                    ret_list[i] += float(v) * float(wight_list[k])
                pass
        pass
        #return [ float(kv)/float(len(wight_list)) for kv in ret_list ]
        return ret_list


    def get_list_last_value_locat(self, this_list):
        ''' 返回list中最后一个非空值的位置 '''
        for i,v in enumerate( map(str, this_list) ):
            if len(v) > 0 and v != ' ':
                continue
            else :
                break
        return i

    def pivot_table_list(self, origin_list, targe_array):
        ''' 把list横行转换为列 '''
        for k,v in enumerate(origin_list):
            targe_array[k].append(v)
        pass


    def merge_list_to_dict(self, key_list, value_list):
        ''' 把两个list转换成dict '''
        ret_dict={}
        for i,v in enumerate(key_list):
            ret_dict[v]=value_list[i]
        return ret_dict


    def fillna_list_self_value(self, this_list):
        ''' 填充list，根据自身最后一个非空value '''
        ret_list=[]
        fillna_value=this_list[0]
        for i,v in enumerate(this_list):
            if len(v) > 0 and v != ' ':
                fillna_value=v
            ret_list.append(fillna_value)
        pass
        return ret_list

    def compute_predict_lose(self, pre_value, true_value):
        ''' 返回误差---均方误差 '''
        this_lose=0.0
        for i,v  in enumerate(pre_value):
            this_lose += abs((float(v) - float(true_value[i]))) #* (float(v) - float(true_value[i]))
        return math.sqrt(float(this_lose/float(len(pre_value))))

    def merge_index_value_array(self, index_value, value_array):
        ''' merge index array  '''
        return [ index_value + l_v for l_v in value_array ]

    def get_df_arrars(self, index_column, index_value, column_list, value_array):
        '''根据传入的index column value生成DataFrame格式数据，并返回 '''
        # check
        if len(index_column) != len(index_value)   or  len(column_list) != len(value_array[0]) :
            #print 'columns length != values length'
            #print 'index_column:',index_column,',index_value:',index_value
            #print 'column_list:',column_list,',value_array:',value_array
            return pd.DataFrame()
        columns_list  = index_column + column_list
        this_value    = self.merge_index_value_array(index_value, value_array)
        return pd.DataFrame(np.array(this_value), columns=list(columns_list))

    def download_df_to_file(self, df, this_file):
        ''' DataFrame数据写入文件 '''
        df.to_csv(this_file, sep=',', encoding='utf-8', index=False)

    def concat_two_df(self, df, df_targe):
        ''' 融合两个DataFrame '''
        if len(df_targe) < 1:
            df_targe = df
        else :
            df_targe = pd.concat([df_targe, df])
        pass


    def get_product_overdue_dataframe(self, df, value_columns, value_list):
        ''' '''
        df_tmp  = df
        if len(value_columns) != len(value_list) :
            print 'columns len != value len'
            return pd.DataFrame()
        for k,v in enumerate(value_list):
            df_tmp = df_tmp[(df_tmp[value_columns[k]] == int(v))]
        return df_tmp


    def get_history_values(self, values_list):
        ''' '''
        return sum(values_list) / len(values_list)
        #return np.percentile(values_list, 75)

    def read_file_to_dict(self, this_file, k_b, k_e, v_b, v_e):
        '''  '''
        #return { ",".join(line.strip('\n').split(',')[k_b:k_e]) : line.strip('\n').split(',')[v_b:v_e]  for line in fd_in }
        ret_dict={}
        if os.path.isfile(this_file):
            with open(this_file, 'r')   as fd_in :
                line = fd_in.readline()
                line = fd_in.readline()
                while line:
                    this_line   = line.strip('\n').split(',')
                    this_key    = ",".join(this_line[k_b:k_e])
                    this_value  = this_line[v_b:v_e]
                    ret_dict[this_key]  = this_value
                    line = fd_in.readline()
        else :
            pass
        pass
        return  ret_dict

    def get_month_date_list(self, this_month):
        ''' 根据传入月份,返回该月份各天日期 '''
        #2017-09
        days_list=[]
        for i in range(66):
            this_day    = (datetime.strptime(this_month, "%Y-%m") + relativedelta(days=(i))).strftime("%Y-%m-%d")
            if this_day[0:7] > this_month :
                break
            days_list.append(this_day)
        pass
        return days_list

