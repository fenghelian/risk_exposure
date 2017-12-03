#coding=utf-8
'''
Created on 2017年9月26日

@author: luobaowei
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
from deal_mean_parameters import FitParameters
from deal_parameters_rules import RulesParameters
from count_order_exposure import CountOrderExposure
from create_new_bill      import CreateNewBill
from count_overdue  import CountOverdue
from reload_file    import ReloadFile
from product_ck_estimator   import ProductCKEstimator
from grid_search import *


class EstimatorExposure(ProductCKEstimator):
    '''
        description
    '''
    def __init__(self, config_file, this_end_month, this_dt_type, this_dt):
        '''
            构造函数指定产品，账期，分期等等各产品参数
        '''
        print 'will begin estimator order exposure.....',datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ProductCKEstimator.__init__(self, config_file, this_end_month, this_dt_type, this_dt)
        self.this_config_file                   = config_file

        self.df_bill_rest                       = pd.DataFrame()  # 剩余没有计算的账单数据
        self.df_bill_overdue                    = pd.DataFrame()  # 账单表逾期金额
        self.product_parameter_dict             = {}              # 各个产品参数dictionary
        self.df_product_exporsure               = pd.DataFrame()  # 各个产品敞口
        pass

    def __del__(self):
        ''' '''
        print 'estimator order exposure over',datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        pass


    def preprocess(self):
        '''  '''
        #处理下载数据，格式化、补充填充值
        #格式化
        deal_format = DealDataFormat(self.this_config_file, self.this_dt)
        deal_format.this_format_data_main()
        #逾期率填充, 各个产品，平均逾期率
        this_bs = self.this_bs
        fit_mean = FitParameters(self.this_config_file, this_bs)
        fit_mean.fit_parameter_main()
        #填充已经产生订单的逾期率
        fit_run = RulesParameters(self.this_config_file, self.this_dt)
        fit_run.deal_rules_main()
        pass

    def process(self):
        '''
        '''
        #1: 生成账单表;2: 初始化，读文件；
        bill_flag = 1  #是否重新生成账单表. 1:重新生成; 0:无需重新生成,直接读取账单表
        if bill_flag == 0:
            self.df_all_bill    = pd.read_csv(self.bill_data_all_file, sep=',', encoding='utf-8')
            #self.df_all_bill = pd.read_csv("this_rest_bill.csv", sep=',', encoding='utf-8')
        else :
            create_bill  = CreateNewBill(self.this_config_file, self.this_dt_type, self.this_dt)
            self.df_all_bill = create_bill.get_merge_now_new_bills()
            #self.download_df_to_file(self.df_all_bill, self.bill_data_all_file)
        pass
        print 'bill over!',datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #2: 初始化，读文件
        self.reload_all_global_dict()
        self.first_init_variable()
        print 'init dict over!',datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        pass

    def merge_computer_exposure(self, df, index_column, index_value):
        ''' '''
        begin = time.time()
        date_list,value_array    = self.computer_product_exposure(df)
        end = time.time()
        #print '-------------------- self.computer_product_exposure 耗时：',end-begin,'seconds --------------------'
        begin = time.time()
        df_value    = self.get_df_arrars(index_column, index_value, date_list, value_array)
        end = time.time()
        #print '-------------------- self.get_df_arrars 耗时：',end-begin,'seconds --------------------'
        #print 'df value:',df_value.head()
        begin = time.time()
        if len(self.df_product_exporsure) < 1:
            self.df_product_exporsure = df_value
        else :
            self.df_product_exporsure = pd.concat([self.df_product_exporsure, df_value])
        end = time.time()
        #print '-------------------- concat 耗时：',end-begin,'seconds --------------------'
        pass


    def estimator_all_product_exposure(self):
        ''' 统计敞口 '''
        #1：计算各个产品分类的逾期金额;2: 汇总各个产品逾期金额；3：计算敞口
        #bs,type,fenqi_cycle,fenqi,migration_rate,migration_lose,risk_rate,risk_lose
        index_column="bs,type,fenqi_cycle,fenqi".split(',')
        self.df_bill_rest = self.df_all_bill
        #self.df_all_bill.to_csv("check_all_bill.csv", sep=',', encoding='utf-8')
        #self.df_bill_rest.to_csv("check_last_bill.csv", sep=',', encoding='utf-8')
        print 'product_parameter_dict:',self.product_parameter_dict
        print '账单数量:',len(self.df_all_bill)
        for k,v in self.product_parameter_dict.iteritems():
            this_key = k.split(',')
            #if this_key[2] == '999' or this_key[1] == '999':
            if '999'  in this_key[0:3] or 'all' in this_key[0:3]:
                continue
            this_migration = float(v[0]) * self.bill_self_coefficient
            this_risk      = float(v[2]) * self.new_create_bill_coefficient
            # 过滤产品，计算逾期金额
            #this_migration=0.95
            #this_risk=1.35
            print '产品:',k,',migration rate;risk rate,before adjustment:',v[0],v[2],',after adjustment:',this_migration,this_risk,datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print '产品:',k,',migration rate:',this_migration,',risk rate:',this_risk,datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            begin = time.time()
            df_ret      = self.estimator_product_overdue(this_key, this_migration, this_risk)
            end = time.time()
            #去掉趣先享订单
            if int(this_key[1]) == 8 :
                continue
            #print '******************************************************************************************************************************'
            print '**************************************** 计算产品',k,'的逾期金额耗时：',end-begin,' seconds **********************************'
            #print '******************************************************************************************************************************'
            if len(df_ret) < 1 :
                continue
            #self.concat_two_df(df_ret, self.df_bill_overdue)
            if len(self.df_bill_overdue) < 1:
                self.df_bill_overdue = df_ret
            else :
                begin = time.time()
                self.df_bill_overdue = pd.concat([self.df_bill_overdue, df_ret])
                end = time.time()
                #print '*************************************** concat 表耗时：',end-begin,' seconds *****************************************'
            begin = time.time()
            self.merge_computer_exposure(df_ret, index_column, this_key)
            end = time.time()
            #print '*************************************** merge computer exposure 耗时：',end-begin,' seconds *****************************************'
        pass
        #print self.df_product_exporsure
        #sys.exit()
        other_type=[5, 6, 8, 999]
        for i in other_type:
            if len(self.df_bill_rest) < 1 :
                break
            key_value = self.this_bs + ","  + str(i) + ",999,999"
            this_key = key_value.split(',')
            if self.product_parameter_dict.has_key(key_value) :
                this_value = self.product_parameter_dict[key_value]
                this_migration = this_value[0]
                this_risk      = this_value[2]
            else :
                this_migration=1.00
                this_risk=1.05
            pass
            print '产品:',key_value,',migration rate;risk rate,before adjustment:',this_migration,this_risk
            this_migration  = float(this_migration)  * self.bill_self_coefficient
            this_risk       = float(this_risk)  * self.new_create_bill_coefficient
            print 'after adjustment:',this_migration,this_risk,datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df_ret      = self.estimator_product_overdue(this_key, this_migration, this_risk)
            # 去掉空 或 趣先享
            if len(df_ret) < 1 or i == 8 :
                continue
            #self.concat_two_df(df_ret, self.df_bill_overdue)
            if len(self.df_bill_overdue) < 1:
                self.df_bill_overdue = df_ret
            else :
                self.df_bill_overdue = pd.concat([self.df_bill_overdue, df_ret])
            self.merge_computer_exposure(df_ret, index_column, this_key)
        pass
        print 'all product bills over!',datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def putout_exposure_type(self, this_column):
        ''' '''
        #print 'product exporsure:',self.df_product_exporsure.head()
        this_value=self.df_product_exporsure[(self.df_product_exporsure['exposure_type'] == this_column)][[ i for i in self.deal_date_list ]].astype(np.float64).sum().tolist()
        print this_column,':',",".join(map(str, this_value))
        return [this_column]+map(str, this_value)

    def print_exposure(self):
        ''' 输出 '''
        print 'Day : ',",".join(self.deal_date_list)
        if self.output_type == "month" :
            exposure_columns  = ['M0+', 'M1+', 'M2+', 'M3+', 'M4+', 'M5+', 'M6+']
            overdue_columns   = ['M0+_overdue', 'M1+_overdue', 'M2+_overdue', 'M3+_overdue', 'M4+_overdue', 'M5+_overdue', 'M6+_overdue']
        else :
            exposure_columns  = ['D1+', 'D31+', 'D61+', 'D91+', 'D121+', 'D151+', 'D181+']
            overdue_columns   = ['D1+_overdue', 'D31+_overdue', 'D61+_overdue', 'D91+_overdue', 'D121+_overdue', 'D151+_overdue', 'D181+_overdue']
        # 订单敞口
        res = []
        for k,v in enumerate(exposure_columns):
            res.append(self.putout_exposure_type(v))
        # 订单逾期金额
        for k,v in enumerate(overdue_columns):
            res.append(self.putout_exposure_type(v))
        res_df = pd.DataFrame(res,columns = ['day']+self.deal_date_list)
        #print res_df 
        res_df = res_df.set_index('day')
        res_df = res_df.T
        res_df['predicted_date'] = res_df.index 
        res_df.to_csv("./../../data/output/laifenqi_exposure_overdue_result.csv", index = False)
        pass

    def down_output_file(self):
        ''' 保存产出文件 '''
        # 账单数据
        #self.download_df_to_file(self.df_all_bill, "this_check_bill.csv")
        self.download_df_to_file(self.df_all_bill, self.bill_data_all_file)
        #逾期金额文件数据
        self.download_df_to_file(self.df_bill_overdue, self.all_bill_items_overdue_file)
        #敞口数据文件数据
        self.download_df_to_file(self.df_product_exporsure, self.predict_order_exposure_result)


    def run(self):
        '''
            整体流程执行
        '''
        #一：1: 下载数据；2: 处理数据
        #self.preprocess()
        #二：1: 初始化，读文件；2: 生成账单表
        begin = time.time()   
        self.process()
        end = time.time()
        #print '***********************************************************************************************************'
        print '********************************* 初始化读文件耗时：', end-begin, ' seconds *******************************'
        #print '***********************************************************************************************************'
        #三：1：计算各个产品分类的逾期金额;2: 汇总各个产品逾期金额；3：计算敞口
        begin = time.time()
        self.estimator_all_product_exposure()
        end = time.time()
        #print '***********************************************************************************************************'
        print '********************************* 计算各逾期金额、敞口的总耗时：', end-begin, ' seconds *******************'
        #print '***********************************************************************************************************'
        #四：汇总输出
        self.print_exposure()
        #五：文件保存
        begin = time.time()
        self.down_output_file()
        end = time.time()
        #print '***********************************************************************************************************'
        print '********************************* 保存文件耗时：', end-begin, ' seconds ***********************************'
        #print '***********************************************************************************************************'
        


def main():
    ''' '''
    config_file     = sys.argv[1]
    this_end_month  = sys.argv[2]
    this_dt_type    = sys.argv[3]
    bill_migration  = float(sys.argv[4])
    create_risk_coefficient     = float(sys.argv[5])
    output_type     = sys.argv[6]
    this_bs         = sys.argv[7]
    print 'config file:',config_file,',count end month:',this_end_month,',this date type:',this_dt_type,',this migration coefficient:',bill_migration,',create risk coefficient:',create_risk_coefficient
    if len(sys.argv) > 8:
        print 'len:',len(sys.argv),',sys:',sys.argv
        this_dt = sys.argv[8]
    else :
        this_dt = datetime.now().strftime("%Y-%m-%d")   #本月月份  本月及之后的月份账单表重新生成，之前的无需重新生成
    print 'this_dt:',this_dt
    run     = EstimatorExposure(config_file, this_end_month, this_dt_type, this_dt)
    run.bill_self_coefficient   = bill_migration
    run.this_bs = this_bs
    run.new_create_bill_coefficient = create_risk_coefficient
    run.output_type    = output_type
    run.run()



if __name__=="__main__":
    main()

