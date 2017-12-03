#coding=utf-8
'''
Created on 2017年9月19日

@author: huangpingchun
'''

from pandas import Series,DataFrame
import pandas as pd
import numpy as np
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
from datetime import datetime,timedelta

sys.path.append("./../lib/")
sys.path.append("./../framework/algo/")

from base_ck_estimator import BaseCKEstimator
from deal_data_format  import DealDataFormat
from grid_search import *
from base_init_parameters import BaseInitParameters

class ProductCKEstimator(BaseCKEstimator):
    '''
        description
    '''
    def __init__(self, config_file, this_end_month, this_dt_type, this_dt):
        '''
            构造函数指定产品，账期，分期等等各产品参数
        '''
        BaseCKEstimator.__init__(self, config_file, this_end_month, this_dt_type, this_dt)
        self.adjusting_bill     = pd.DataFrame()
        self.adjusting_param_flag =""

    def preprocess(self):
        '''
            调用父类方法
        '''
        #处理下载数据，格式化、补充填充值
        #self.this_format_data_main()
        BaseCKEstimator.preprocess(self)
        pass

    def process(self):
        '''
            调用父类方法
        '''
        BaseCKEstimator.process(self)
        #处理逾期数据  均值
        #处理各订单账单逾期数据

    def get_product_bill_dataframe(self, df_bill, this_line):
        ''' '''
        this_bs     = this_line[0]    # laifenqi, 及后期用户分层，过滤使用
        this_type   = this_line[1]
        this_cycle  = this_line[2]
        this_fenqi  = this_line[3]
        df = self.get_filter_preduct_bill(df_bill, this_type, this_cycle, this_fenqi)
        return df


    def adjusting_parameter_rate(self, param_rate):
        ''' 调整迁徙率参数 '''
        #该参数逾期金额
        #print 'self.adjusting_param_flag:',self.adjusting_param_flag
        migration_rate=1.0
        risk_rate=1.0
        if self.adjusting_param_flag == "migration" :
            migration_rate = param_rate
        elif self.adjusting_param_flag == "risk" :
            risk_rate = param_rate
        else :
            return 99999
        pass
        #print 'param_rate:',param_rate,',migration_rate:',migration_rate,',risk_rate:',risk_rate
        this_bill = self.adjusting_bill.copy(deep=True)
        #print  '++++++++++++++++++++++++++++bill :',this_bill
        return  self.adjusting_parameter_overdue(this_bill, migration_rate, risk_rate)

    def adjusting_parameter_overdue(self, df, migration_rate, risk_rate):
        ''' 调整逾期金额参数 '''
        #该参数逾期金额   --- 需要调整
        #print 'migration_rate:',migration_rate,',risk_rate:',risk_rate
        this_value = self.get_bill_overdue_parameters(df, migration_rate, risk_rate)
        if len(this_value) < 1:
            return 99999
        #print '===================this_value:',this_value
        this_lose  = self.compute_predict_lose(this_value, self.this_ture_value)
        #this_lose  = self.compute_predict_lose(this_value)
        return this_lose

    def get_this_type_true_value(self, param_flag, product_list):
        ''' '''
        #print '+++++++++++++++:',self.df_true_bill.head()
        this_true_bill  = self.get_product_bill_dataframe(self.df_true_bill, product_list)
        return self.get_this_true_value(this_true_bill, param_flag)

    def get_change_best_value_str(self, this_value, this_per):
        ''' '''
        this_start  = this_value - this_per
        this_end    = this_value + this_per
        new_per     = float(this_per / 10.0)
        return '-c '+str(this_start)+','+str(this_end)+','+str(new_per)

    def adjusting_best_parameters(self, param_dict, value_list):
        ''' '''
        #print 'param_dict:',param_dict,',value_list:',value_list
        param_flag = param_dict["param_flag"]
        product_list = [param_dict["bs"], param_dict["type"], param_dict["fenqi_cycle"], param_dict["fenqi"] ]
        #print 'product_list:',product_list
        # 获取该产品订单
        this_bill    = self.get_product_bill_dataframe(self.df_all_bill, product_list)
        #print 'this_bill:',this_bill.head()
        #print 'this bill columns:',this_bill.columns.tolist()
        # 获取该产品真实际逾期数据
        self.this_ture_value = self.get_this_type_true_value(param_flag, product_list)
        if len(self.this_ture_value) < 1:
            return 0
        start_time = time.time()
        this_per    = float(value_list[2])
        this_str = '-c '+ ",".join(map(str, value_list))

        self.adjusting_param_flag = param_flag
        if param_flag == "migration":
            self.adjusting_bill = this_bill[(this_bill['deadline_dt'] < self.this_dt)]
        elif param_flag == "risk" :
            self.adjusting_bill = this_bill[(this_bill['deadline_dt'] >= self.this_dt) & (this_bill['deadline_dt'] <= self.this_end_month)]
        else :
            pass
        pass
        #print '--------------self.this_ture_value:',self.this_ture_value
        #print 'this_bill:',len(self.adjusting_bill)
        #print 'true value:',self.this_ture_value
        min_lose,best_value = find_parameters(this_str, self.adjusting_parameter_rate)
        if len(best_value) < 1:
            return 0
        ret_str = self.get_change_best_value_str(float(best_value['c']), this_per)
        min_lose,best_value = find_parameters(ret_str, self.adjusting_parameter_rate)
        return [ param_dict["bs"], param_dict["type"], param_dict["fenqi_cycle"], param_dict["fenqi"], param_flag, best_value['c'], min_lose ]
        #print ("type:%s,fenqi_cycle:%s,fenqi:%s,min lose:%f, %s value:%f" % (self.this_type,self.this_fenqi_cycle, self.this_fenqi, min_lose, param_flag, best_value['c']))


    def computer_product_exposure(self, df):
        ''' '''
        #all_overdue_amount,before_overdue,current_loan,(before_overdue+current_loan)
        date_list   = ['exposure_type']
        #value_array = [['overdue'], ['D1+']]
        if self.output_type  == "month" :
            value_array=[['M0+'], ['M0+_overdue'], ['M1+'], ['M1+_overdue'], ['M2+'], ['M2+_overdue'], ['M3+'], ['M3+_overdue'], ['M4+'], ['M4+_overdue'], ['M5+'], ['M5+_overdue'], ['M6+'], ['M6+_overdue']]
        else :
            value_array=[['D1+'], ['D1+_overdue'], ['D31+'], ['D31+_overdue'], ['D61+'], ['D61+_overdue'], ['D91+'], ['D91+_overdue'], ['D121+'], ['D121+_overdue'], ['D151+'], ['D151+_overdue'], ['D181+'], ['D181+_overdue']]
        for i in range(366789):
            #deadline_dt = (datetime.strptime("2015-11", "%Y-%m") + relativedelta(months=(i))).strftime("%Y-%m")  if  self.this_dt_type == "month" else (datetime.strptime(self.this_dt, "%Y-%m-%d") + relativedelta(days=(i))).strftime("%Y-%m-%d")
            #deadline_dt = (datetime.strptime(self.this_dt, "%Y-%m-%d") + relativedelta(days=(i))).strftime("%Y-%m-%d")
            deadline_dt = self.get_dealdt_type(i, self.this_dt)
            if deadline_dt > self.this_end_month:
                break
            date_list.append(deadline_dt)
            if deadline_dt not  in self.deal_date_list:
                self.deal_date_list.append(deadline_dt)
            #ret_list = self.estimator_order_exposure(df, deadline_dt)
            #print 'dealine_dt:',deadline_dt
            df_tmp  = df[(df['deadline_dt'] <= deadline_dt)].copy(deep=True)
            ret_list = self.estimator_order_exposure(df_tmp, deadline_dt)
            self.pivot_table_list(ret_list, value_array)
        pass
        return date_list,value_array


    def estimator_product_overdue(self, product_key, this_migration, this_risk):
        ''' '''
        print '-------------------总账单表数量:',len(self.df_all_bill)
        print '-------------------剩余账单表数量:',len(self.df_bill_rest)
        this_bill       = self.get_product_bill_dataframe(self.df_bill_rest, product_key)
        #this_bill       = self.get_product_bill_dataframe(self.df_all_bill, product_key)
        print '-------------------该产品账单表数量:',len(this_bill)
        self.df_bill_rest  = self.df_bill_rest.drop(this_bill.index)
        print '-------------------剩余账单表数量:',len(self.df_bill_rest)
        this_deal_bill  = this_bill.copy(deep=True)
        if len(this_deal_bill) < 1:
            return pd.DataFrame()
        self.estimatior_bill_overdue(this_deal_bill, this_migration, this_risk)
        return this_deal_bill



    def run(self):
        '''
            整体流程执行
        '''
        self.preprocess()
        self.process()


