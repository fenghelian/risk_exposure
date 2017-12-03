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


from public_lib import PublicLib



class  ReloadFile(PublicLib):
    ''' 加载文件 '''
    def __init__(self):
        '''
        print datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.qudian_amount_data_file                            = ""
        self.fenqi_cycle_fenqi_diff_amount_rate_old_file        = ""
        self.fenqi_cycle_fenqi_diff_amount_rate_new_file        = ""
        self.fenqi_cycle_amount_rate_old_file                   = ""
        self.fenqi_cycle_amount_rate_new_file                   = ""
        self.bill_data_now_file                                 = ""
        self.type_fenqi_cycle_fenqi_capital_rate                = ""
        self.all_product_parameter_data_file                    = ""
        self.df_amount                      = pd.DataFrame()     #交易额数据
        self.df_fenqi_diff_amount           = pd.DataFrame()     #历史数据中：不同分期在相应订单类型放贷额中占比
        self.df_fenqi_diff_amount_new       = pd.DataFrame()     #新配置数据：不同分期在相应订单类型放贷额中占比
        self.df_cycle_amount_old            = pd.DataFrame()     #历史数据中：不同订单类型在总放贷额中占比
        self.df_cycle_amount_new            = pd.DataFrame()     #新配置数据：不同订单类型在总放贷额中占比
        self.df_now_bill                    = pd.DataFrame()     #读取现在账单数据
        self.df_fenqi_capital_day           = pd.DataFrame()     #订单不同期的本金分布
        self.this_end_month                 = datetime.now().strftime("%Y-%m-%d")
        self.month_list                     = []    #需要计算敞口的月份
        self.this_type                      = "all"
        self.this_fenqi_cycle               = "all"
        self.this_fenqi                     = "all"
        self.dict_day_order_coefficient                         = {}                    #日期、订单类型、修改系数
        self.dict_cycle_overdue                                 = {}                    #不同订单类型、不同期数逾期率（由近半年统计情况获得）
        self.dict_order_rate                                    = {}                    #生成 订单创建时间、月订单不同分期、不同期数在贷系数, 订单创建日期、订单类型、分期数、账期，对应的金额占比
        self.dict_all_rate                                      = {}                    #生成 订单不同分期、不同期数在贷系数, 订单类型、分期数、账期，对应的金额占比
        self.dict_order_overdue                                 = {}                    #订单创建日期、账单到期日期、拟合后的逾期率
        self.product_parameter_dict                             = {}                    #配置各产品的留存率、未来风险系数参数


        print '加载文件......'

        '''


    def __del__(self):
        print '加载完毕.',datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
                    #if  self.check_type_cycle_fenqi_fun(this_line[0:3]) == 1:
                    #    continue
                    this_key    = ",".join(this_line[k_b:k_e])
                    this_value  = this_line[v_b:v_e]
                    ret_dict[this_key]  = this_value
                    line = fd_in.readline()
        else :
            pass
        pass
        return  ret_dict

    def get_fenqi_capital_distribution(self, this_file, this_len):
        '''' '''
        ret_dict={}
        with open(this_file, 'r')   as  fd_in :
            line = fd_in.readline()
            line = fd_in.readline()
            while line :
                this_line   = line.strip('\n').split(',')
                all_rate=0.0
                key_pro=""
                for j in range(this_len):
                    key_pro += this_line[j] + ","
                for i in range(this_len, len(this_line)):
                    if  len(this_line[i]) < 1:
                        break
                    ret_dict[key_pro + str(i+1-this_len)] = (1.0 - all_rate)/float(this_line[i])
                    all_rate += float(this_line[i])
                line = fd_in.readline()
        pass
        return ret_dict

    def reload_overdue_dict(self):
        ''' '''
        #this_dt 之前创建的订单，前3个月的逾期率
        #订单创建日期、账单到期日期、拟合后的逾期率
        #order_dt,deadline_dt,type,fenqi_cycle,fenqi,order_month_diff,capital_sum,num,all_num,all_orign_amount,d1_0,d1_1,d1_2,d1_3,d1_4,d1_5,d1_6,d1_7,d1_8,d1_9
        if os.path.isfile(self.parameters_order_deadline_overdue_rate_file):
            with  open(self.parameters_order_deadline_overdue_rate_file, 'r')   as fd_in:
                #self.dict_order_overdue = {",".join(line.strip('\n').split(',')[0:6]):line.strip('\n').split(',') for line in fd_in if line[0:8] != "order_dt" }
                line = fd_in.readline()
                line = fd_in.readline()
                while line :
                    this_line   = line.strip('\n').split(',')
                    if  self.check_type_cycle_fenqi_fun(this_line[2:5]) == 1 :
                        line = fd_in.readline()
                        continue
                    this_key    = ",".join(this_line[0:6])
                    this_value  = this_line
                    self.dict_order_overdue[this_key]   = this_value
                    line = fd_in.readline()
        pass
        #不同订单类型、不同期数逾期率（由近半年统计情况获得）
        #type,fenqi_cycle,fenqi,order_month_diff,d1_0,d1_1,d1_2,d1_3,d1_4,d1_5
        if os.path.isfile(self.parameters_cycle_fenqi_mean_overdue_file):
            with open(self.parameters_cycle_fenqi_mean_overdue_file, 'r')   as fd_in:
                #self.dict_cycle_overdue = {",".join(line.strip('\n').split(',')[0:4]):line.strip('\n').split(',') for line in fd_in if line[0:4] != "type"}
                line = fd_in.readline()  # 跳过首行
                line = fd_in.readline()
                while line :
                    this_line   = line.strip('\n').split(',')
                    if self.check_type_cycle_fenqi_fun(this_line[0:3]) == 1 :
                        line = fd_in.readline()
                        continue
                    this_key    = ",".join(this_line[0:4])
                    this_value  = this_line
                    self.dict_cycle_overdue[this_key] = this_value
                    line = fd_in.readline()
        pass
        #不同日期不同产品，逾期率单独调整
        #order_dt,type,fenqi_cycle,fenqi,coefficient
        if os.path.isfile(self.day_order_type_cycle_fenqi_coefficient_file):
            with open(self.day_order_type_cycle_fenqi_coefficient_file, 'r')    as fd_in:
                #self.dict_day_order_coefficient = {",".join(line.strip('\n').split(',')[0:4]):float(line.strip('\n').split(',')[-1]) for line in fd_in if line[0:8] != "order_dt"}
                lines = fd_in.readlines()
                for line in lines:
                    this_line = line.strip('\n').split(',')
                    if self.check_type_cycle_fenqi_fun(this_line[1:4]) == 1  or "order_dt" in this_line:
                        continue
                    this_key    = ",".join(this_line[0:4])
                    this_value  = float(this_line[-1])
                    self.dict_day_order_coefficient[this_key] = this_value
        pass
        #大盘逾期数据
        with open(self.all_overdue_flow_data_file, 'r')     as fd_in:
            self.all_bs_overdue  = { ",".join(line.strip('\n').split(',')[0:1]):line.strip('\n').split(',')[1:]  for line in fd_in  if line[0:2] != "bs" }
        pass

    def reload_product_dict(self):
        ''' 加载各产品详细参数 '''
        #read_file_to_dict(self, this_file, k_b, k_e, v_b, v_e):
        #bs,type,fenqi_cycle,fenqi,migration_rate,migration_lose,risk_rate,risk_lose
        self.product_parameter_dict    = self.read_file_to_dict(self.all_product_parameter_data_file, 0, 4, 4, 8)


    def reload_dict_df(self):
        ''' '''
        if os.path.isfile(self.product_tomorrow_risk_rate_file):
            self.df_tomorrow                    = pd.read_csv(self.product_tomorrow_risk_rate_file, sep=',', encoding='utf-8', error_bad_lines=False)
        pass


    def reload_all_global_dict(self):
        ''' '''
        #type,fenqi_cycle,fenqi,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
        this_len=3
        self.dict_all_rate  = self.get_fenqi_capital_distribution(self.order_cycle_fenqi_capital_rate_config, this_len)
        #order_dt,type,fenqi_cycle,fenqi,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
        this_len=4
        self.dict_order_rate    = self.get_fenqi_capital_distribution(self.order_create_cycle_fenqi_capital_rate, this_len)
        #加载逾期数据
        self.reload_overdue_dict()
        #加载各产品详细参数
        self.reload_product_dict()
        #加载词典数据
        self.reload_dict_df()


    def reload_file_to_dataframe(self):
        ''' 读取文件---DataFrame,pandas '''
        #读取交易额数据
        self.df_amount                      =  pd.read_csv(self.qudian_amount_data_file, sep=',', encoding='utf-8', error_bad_lines=False)
        #读取不同订单类型、账期交易额占比
        self.df_fenqi_diff_amount           =  pd.read_csv(self.fenqi_cycle_fenqi_diff_amount_rate_old_file, sep=',', encoding='utf-8', error_bad_lines=False)
        self.df_fenqi_diff_amount_new       =  pd.read_csv(self.fenqi_cycle_fenqi_diff_amount_rate_new_file, sep=',', encoding='utf-8', error_bad_lines=False)
        self.df_cycle_amount_old            =  pd.read_csv(self.fenqi_cycle_amount_rate_old_file, sep=',', encoding='utf-8', error_bad_lines=False)
        self.df_cycle_amount_new            =  pd.read_csv(self.fenqi_cycle_amount_rate_new_file, sep=',', encoding='utf-8', error_bad_lines=False)
        #读取现在账单数据
        self.df_now_bill                    =  pd.read_csv(self.bill_data_now_file, sep=',', encoding='utf-8', error_bad_lines=False)
        #订单不同账期的本金分布
        self.df_fenqi_capital_day           = pd.read_csv(self.type_fenqi_cycle_fenqi_capital_rate, sep=',', encoding='utf-8', error_bad_lines=False)
        self.df_fenqi_capital_old           = pd.read_csv(self.type_cycle_fenqi_capital_rate_old_file, sep=',', encoding='utf-8', error_bad_lines=False)
        #
        self.df_product_amount_old          = pd.read_csv(self.product_type_amount_rate_old_file,  sep=',', encoding='utf-8', error_bad_lines=False)
        self.df_product_amount_new          = pd.read_csv(self.product_type_amount_rate_new_file,  sep=',', encoding='utf-8', error_bad_lines=False)
        #
        if os.path.isfile(self.order_bill_now_file):
            self.df_true_bill               = pd.read_csv(self.order_bill_now_file, sep=',', encoding='utf-8', error_bad_lines=False)
        pass
        self.df_days_amount                 = pd.read_csv(self.qudian_amount_days_new_data_file, sep=',', encoding='utf-8', error_bad_lines=False)


    def reload_file_to_self(self):
        ''' 读取文件---自处理 '''
        #读取全局变量
        self.reload_all_global_dict()
        pass

    def first_init_variable(self):
        ''' 初始化相关变量 '''
        #生成month_list
        for i in range(3698):
            month_tmp = self.this_dt_add_per_value(i)
            if month_tmp > self.this_end_month :
                break
            self.month_list.append(month_tmp)
        pass


    def reload_file_main(self):
        ''' '''
        #读取相关文件---DataFrame格式
        print '\t读取相关文件,初始化相关变量,',datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.reload_file_to_dataframe()
        #读取文件---自定义格式
        print '\t读取文件---自定义格式,',datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.reload_file_to_self()
        #初始化变量
        print '\t初始化变量,',datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.first_init_variable()
        print 'end!',datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def this_init_parameters(self, config_file):
        ''' 初始化变量 '''
        conf                    = ConfigParser.ConfigParser()
        conf.read(config_file)
        self.path                                               = os.path.dirname(os.path.abspath(__file__))
        self.qudian_amount_data_file                            = ("%s/../../%s" % (self.path, conf.get("config_data", "qudian_amount_data_file")))       #集团来分期交易额数据
        self.fenqi_cycle_fenqi_diff_amount_rate_old_file        = ("%s/../../%s" % (self.path, conf.get("config_data", "fenqi_cycle_fenqi_diff_amount_rate_old_file")))   #历史数据中：不同分期在相应订单类型放贷额中占比
        self.fenqi_cycle_fenqi_diff_amount_rate_new_file        = ("%s/../../%s" % (self.path, conf.get("config_data", "fenqi_cycle_fenqi_diff_amount_rate_new_file")))   #新配置数据：不同分期在相应订单类型放贷额中占比
        self.fenqi_cycle_amount_rate_old_file                   = ("%s/../../%s" % (self.path, conf.get("config_data", "fenqi_cycle_amount_rate_old_file")))              #历史数据中：不同订单类型在总放贷额中占比
        self.fenqi_cycle_amount_rate_new_file                   = ("%s/../../%s" % (self.path, conf.get("config_data", "fenqi_cycle_amount_rate_new_file")))              #新配置数据：不同订单类型在总放贷额中占比
        self.bill_data_now_file                                 = ("%s/../../%s" % (self.path, conf.get("config_data", "order_bill_now_data_file")))                      #到现在为止集团的账单数据
        self.type_fenqi_cycle_fenqi_capital_rate                = ("%s/../../%s" % (self.path, conf.get("config_data", "type_fenqi_cycle_fenqi_capital_rate_file")))      #不同产品，各个账期资金占比
        self.all_product_parameter_data_file                    = ("%s/../../%s" % (self.path, conf.get("config_data", "all_product_parameter_data_file")))      #不同产品，参数文件
        self.this_end_month                                     = (datetime.now() + relativedelta(months=(3))).strftime("%Y-%m-%d")
        self.this_type                                          = "all"
        self.this_fenqi_cycle                                   = "all"
        self.this_fenqi                                         = "all"


    def this_test_main(self, config_file):
        ''' 调试调用调试'''
        self.this_init_parameters(config_file)
        #读取相关文件---DataFrame格式
        print '\t读取相关文件,初始化相关变量,',datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.reload_file_to_dataframe()
        print '\t读取相关文件,初始化相关变量,',datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #读取文件---自定义格式
        self.reload_file_to_self()
        #初始化变量
        self.first_init_variable()
        print 'end!',datetime.now().strftime("%Y-%m-%d %H:%M:%S")






def main():
    ''' '''
    if len(sys.argv) < 2 :
        print  "python2.7 ReloadFile.py  config_file"
        sys.exit()
    config_file     = sys.argv[1]
    print 'config file:',config_file
    run = ReloadFile()
    run.this_test_main(config_file)

if __name__=="__main__":
    main()



