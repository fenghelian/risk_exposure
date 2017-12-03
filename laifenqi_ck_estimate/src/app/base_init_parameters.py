#coding=utf-8
'''
Created on 2017年9月25日

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


class BaseInitParameters(object):
    '''
    初始化参数变量基础类
    '''
    def __init__(self, config_file, this_end_month, this_dt_type, this_dt):
        '''
        '''
        conf = ConfigParser.ConfigParser()
        conf.read(config_file)
        self.path                                               = os.path.dirname(os.path.abspath(__file__))
        # 额度文件
        self.qudian_amount_data_file                            = ("%s/../../%s" % (self.path, conf.get("config_data", "qudian_amount_data_file")))       #集团来分期交易额数据
        # 额度分配
        #new
        self.fenqi_cycle_fenqi_diff_amount_rate_old_file        = ("%s/../../%s" % (self.path, conf.get("config_data", "fenqi_cycle_fenqi_diff_amount_rate_old_file")))   #历史数据中：不同分期在相应订单类型放贷额中占比
        self.fenqi_cycle_fenqi_diff_amount_rate_new_file        = ("%s/../../%s" % (self.path, conf.get("config_data", "fenqi_cycle_fenqi_diff_amount_rate_new_file")))   #新配置数据：不同分期在相应订单类型放贷额中占比
        self.fenqi_cycle_amount_rate_old_file                   = ("%s/../../%s" % (self.path, conf.get("config_data", "fenqi_cycle_amount_rate_old_file")))              #历史数据中：不同订单类型在总放贷额中占比
        self.fenqi_cycle_amount_rate_new_file                   = ("%s/../../%s" % (self.path, conf.get("config_data", "fenqi_cycle_amount_rate_new_file")))              #新配置数据：不同订单类型在总放贷额中占比
        # 账单表
        self.bill_data_now_file                                 = ("%s/../../%s" % (self.path, conf.get("config_data", "order_bill_now_data_file")))                      #到现在为止集团的账单数据
        self.bill_data_all_file                                 = ("%s/../../%s" % (self.path, conf.get("config_data", "all_bill_data_put_file")))                        #merge以前和模拟未来的账单，产生新的账单数据
        # 生成各个账期在各个时间点的逾期金额
        self.all_bill_items_overdue_file                        = ("%s/../../%s" % (self.path, conf.get("config_data", "all_bill_data_overdue_file")))                    #根据新生成的账单表，计算出各个日期内的逾期金额
        # 不同订单类型、不同期数、不同账期，本金分布
        #self.fenqi_capital_rate_data_file                       = ("%s/../../%s" % (self.path, conf.get("config_data", "fenqi_capital_rate_data_file")))                  #新配置数据：不同订单类型、不同期数、不同账期的本金分布
        #self.fenqi_capital_rate_order_data_file                 = ("%s/../../%s" % (self.path, conf.get("config_data", "fenqi_capital_rate_order_data_file")))            #历史数据中：订单创建时间、不同订单类型、不同期数、不同账期的本金分布
        #self.project_temp_file                                  = ("%s/../../%s" % (self.path, conf.get("config_data", "project_temp_out_file")))
        #this_dt 之前创建的订单，前3个月的逾期率
        self.parameters_order_deadline_overdue_rate_file        = ("%s/../../%s" % (self.path, conf.get("config_data", "parameters_order_deadline_overdue_rate_file")))   #订单创建日期、账单到期日期、拟合后的逾期率
        #不同订单类型、期数、账期，各自的逾期率（近半年的统计及拟合后的数据）
        self.parameters_cycle_fenqi_mean_overdue_file           = ("%s/../../%s" % (self.path, conf.get("config_data", "parameters_cycle_fenqi_mean_overdue_file")))      #不同订单类型、不同期数逾期率（由近半年统计情况获得）
        # 历史创建订单，各账期本金分布
        self.order_create_cycle_fenqi_capital_rate              = ("%s/../../%s" % (self.path, conf.get("config_data", "order_create_cycle_fenqi_capital_rate_file")))    #
        # 订单各个账期本金分布 配置
        self.order_cycle_fenqi_capital_rate_config              = ("%s/../../%s" % (self.path, conf.get("config_data", "order_cycle_fenqi_capital_rate_config_file")))    #
        #不同产品，各个账期资金占比
        self.type_fenqi_cycle_fenqi_capital_rate                = ("%s/../../%s" % (self.path, conf.get("config_data", "type_fenqi_cycle_fenqi_capital_rate_file")))    #
        #不同日期不同产品，逾期率单独调整
        self.day_order_type_cycle_fenqi_coefficient_file        = ("%s/../../%s" % (self.path, conf.get("config_data", "day_order_type_cycle_fenqi_coefficient_file")))    #
        self.all_product_parameter_data_file                    = ("%s/../../%s" % (self.path, conf.get("config_data", "all_product_parameter_data_file")))      #不同产品，参数文件
        #新账单表数据，用于寻找参数使用
        self.order_bill_now_file                                = ("%s/../../%s" % (self.path, conf.get("config_data", "all_order_bill_now_file")))    #
        # 各月各产品放款额预算 分布到各天
        self.qudian_amount_days_new_data_file                   = ("%s/../../%s" % (self.path, conf.get("config_data", "qudian_amount_days_new_data_file")))
        #
        self.type_cycle_fenqi_capital_rate_old_file             = ("%s/../../%s" % (self.path, conf.get("config_data", "type_fenqi_cycle_fenqi_capital_rate_old_file")))
        self.product_type_amount_rate_old_file                  = ("%s/../../%s" % (self.path, conf.get("config_data", "qudian_xj_sw_amount_data_old_file")))
        self.product_type_amount_rate_new_file                  = ("%s/../../%s" % (self.path, conf.get("config_data", "qudian_xj_sw_amount_data_new_file")))
        self.predict_order_exposure_result                      = ("%s/../../%s" % (self.path, conf.get("config_data", "predict_order_exposure_result_file")))
        self.all_overdue_flow_data_file                         = ("%s/../../%s" % (self.path, conf.get("config_data", "jituan_overdue_flow_file")))
        self.product_tomorrow_risk_rate_file                    = ("%s/../../%s" % (self.path, conf.get("config_data", "product_tomorrow_risk_rate_file")))
        

        self.this_end_month                                     = this_end_month
        self.this_dt_type                                       = "day"
        self.output_type                                        = "day"
        self.deal_dt_type                                       = this_dt_type
        self.this_dt                                            = this_dt
        self.month_list                                         = []    #需要计算敞口的月份
        self.deal_date_list                                     = []    #输出日期
        self.df_amount                                          = None  #交易额数据
        self.df_fenqi_diff_amount                               = None
        self.df_true_bill   = None
        self.this_ture_value = []
        self.all_bs_overdue  = []


        self.df_tomorrow                                        = pd.DataFrame()        #未来逾期率风险
        self.df_all                                             = pd.DataFrame()
        self.df_all_bill                                        = pd.DataFrame()        #融合历史账单和新产生账单的新账单表
        self.df_now_bill                                        = pd.DataFrame()        #历史账单表数据
        self.df_new_bill                                        = pd.DataFrame()        #根据预计交易额产生未来新的账单表数据
        self.df_cycle_fenqi_rate                                = None
        self.df_fenqi_rate                                      = None                  #新拟合的全部月份的同类型、分期，对应各个账期的金额占比
        self.df_fenqi_now_rate                                  = None                  #历史上各个不同类型、分期，对应各个账期的金额占比
        self.dict_rate                                          = {}                    #历史上不同订单类型、不同期数对应的逾期率
        self.dict_order_rate                                    = {}                    #生成 订单创建时间、月订单不同分期、不同期数在贷系数, 订单创建日期、订单类型、分期数、账期，对应的金额占比
        self.dict_all_rate                                      = {}                    #生成 订单不同分期、不同期数在贷系数, 订单类型、分期数、账期，对应的金额占比
        self.dict_order_overdue                                 = {}                    #订单创建日期、账单到期日期、拟合后的逾期率
        self.dict_cycle_overdue                                 = {}                    #不同订单类型、不同期数逾期率（由近半年统计情况获得）
        self.df_fenqi_capital_day                               = None                  #订单不同期的本金分布
        self.dict_day_order_coefficient                         = {}                    #日期、订单类型、修改系数
        self.df_days_amount                                     = pd.DataFrame()        #各天各产品放款额分布

        self.this_type                                          = "all"
        self.this_fenqi_cycle                                   = "all"
        self.this_fenqi                                         = "all"
        self.this_bs                                            = "laifenqi"

        # 在计算日期内，计算各个账期的逾期金额，各参数调试配置
        self.bill_one_coefficient   = 0.6   #有一次还款行为系数调整
        #self.bill_self_coefficient  = 0.5   #账单历史数据自身拟合diff系数调整
        #bt_bill_coefficient, sw_bill_coefficient, bt_create_coefficient, sw_create_coefficient
        self.bill_self_coefficient  = 1   #账单历史数据自身拟合diff系数调整
        self.bill_new_coefficient   = 1   #刚到期账期数据，历史逾期数据拟合,系数调整
        self.new_create_bill_coefficient     = 1 #新创建的订单，账期逾期率=历史逾期率 * 调整系数

        self.dict_columns               = { 'dt':'dt',
                                            'deadline_dt':'deadline_dt',
                                            'fenqi_cycle':'fenqi_cycle',
                                            'fenqi':'fenqi',
                                            'order_month_diff':'order_month_diff',
                                            'capital':'capital_sum',
                                            'all_orign_amount':'all_orign_amount',
                                            'capital_rate':'capital_rate',
                                            'type':'type', 
                                            'amount_rate':'amount_rate', 
                                            'capital_sum':'capital_sum', 
                                            'type':'type',
                                            'order_num':'num',
                                            'orign_amount':'orign_amount',
                                            'type_rate':'type_rate',
                                            'user_num':'all_num',
                                            'all_amount':'all_amount'}
        pass


    
