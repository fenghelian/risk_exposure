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
from datetime import datetime
import ConfigParser

sys.path.append("./../app/")

from public_lib import PublicLib
from reload_file import ReloadFile
from base_init_parameters  import BaseInitParameters

class CreateNewBill(BaseInitParameters, ReloadFile):
    ''' 按照规定粒度产生结合历史账单产生新的账单数据 '''
    def __init__(self, config_file, this_dt_type, this_dt):
        ''' '''
        print '创建账单表.....',datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        this_end_month   =  datetime.now().strftime("%Y-%m-%d")
        BaseInitParameters.__init__(self, config_file, this_end_month, this_dt_type, this_dt)



    def __del__(self):
        print '账单表创建完毕。',datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def filter_preduct_bill(self, this_column, this_value):
        if this_value != "all" :
            if len(self.df_all_bill) > 1:
                self.df_all_bill = self.df_all_bill[(self.df_all_bill[this_column] == int(this_value))]
            if len(self.df_now_bill) > 1:
                self.df_now_bill = self.df_now_bill[(self.df_now_bill[this_column] == int(this_value))]
            if len(self.df_new_bill) > 1:
                self.df_new_bill = self.df_new_bill[(self.df_new_bill[this_column] == int(this_value))]
            pass
        pass


    def deal_now_bill_items_data(self):
        ''' 处理现在账单数据 '''
        this_columns_list = self.df_now_bill.columns.tolist()
        for i in range(365):
            this_tmp = self.this_dt_add_per_value(i)
            if this_tmp > this_columns_list[-1]:
                break
            if this_tmp in this_columns_list:
                del self.df_now_bill[this_tmp]
        pass
        #print self.df_now_bill.head()


    def get_date_order_amount(self, this_dt):
        ''' 返回日期对应的交易额 '''
        #m_v = this_dt[:7]
        #this_month_days = int(((datetime.strptime(m_v, '%Y-%m') + relativedelta(months=(1))) - datetime.strptime(m_v, '%Y-%m')).days)
        ##this_amount = float(self.df_amount[(self.df_amount[self.dict_columns['dt']] == m_v)][self.dict_columns['orign_amount']].sum())/this_month_days
        ##return this_amount
        #df = self.df_amount[(self.df_amount[self.dict_columns['dt']] == m_v)]
        #df[self.dict_columns['all_amount']] = df[self.dict_columns['orign_amount']]/this_month_days
        #df[self.dict_columns['dt']]=this_dt
        df = self.df_days_amount[(self.df_days_amount[self.dict_columns['dt']] == this_dt)].copy(deep=True)
        df.rename(columns={'orign_amount':self.dict_columns['all_amount']}, inplace=True)
        df  = df[[self.dict_columns['dt'], self.dict_columns['type'], self.dict_columns['all_amount']]]
        return df

    def get_near_one_config(self, df_old, df_new, this_dt):
        ''' '''
        dt='dt'
        if ('dt' not in df_old.columns.tolist())  and ('order_dt' in df_old.columns.tolist()) :
            dt='order_dt'
        if this_dt in df_old.columns.tolist():   # 检查日期是否在已经发生的日期内
            df    =  df_old[( df_old[dt] == this_dt)]
        elif len(df_new) > 0:
            #使用配置里面日期里面，日期小于this_dt 最大的一个日期配置
            new_dt = df_new[( df_new[dt] <= this_dt )][dt].max()
            df    =  df_new[( df_new[dt] == new_dt)]
            ## 检查日期是否在配置的日期内
            #if this_dt in df_new.columns.tolist():
            #    df    =  df_new[( df_new[dt] == this_dt)]
            ## 使用配置最大的日期
            #else :
            #    df    =  df_new[( df_new[dt] == df_new[dt].max() )]
            #pass
        else :
            # 以上均无，使用已经发生过的，最大的日期占比
            df    =  df_old[( df_old[dt] == df_old[dt].max() )]
        pass
        df[dt] = this_dt
        return df


    def get_product_cycle_fenqi_amount_rate(self, this_dt):
        ''' 根据传入的日期,返回对应的各个产品、分期的交易额占比 '''
        merge_list  = [ self.dict_columns['dt'],
                        self.dict_columns['type'],
                        self.dict_columns['fenqi_cycle'],
                        self.dict_columns['fenqi'],
                        self.dict_columns['amount_rate']]
        # 现金、实物、趣先享在总放贷额的占比
        # dt,type,type_rate
        df_type      = self.get_near_one_config(self.df_product_amount_old, self.df_product_amount_new, this_dt)
        # 各分期 在现金、实物等产品放款额的占比
        # dt,type,fenqi_cycle,fenqi,amount_rate
        df_cycle     = self.get_near_one_config(self.df_cycle_amount_old, self.df_cycle_amount_new, this_dt)
        # 各账期的资金分布
        # order_dt,type,fenqi_cycle,fenqi,order_month_diff,capital_rate
        df_fenqi     = self.get_near_one_config(self.df_fenqi_capital_old, self.df_fenqi_capital_day, this_dt)
        df_fenqi.rename(columns={'order_dt':'dt'}, inplace=True)
        # merge
        df_all       = pd.merge(df_type, df_cycle, how='left', on=['dt','type'])
        df_all       = pd.merge(df_all, df_fenqi, how='left', on=['dt','type','fenqi_cycle','fenqi'])
        pass
        df_all       = df_all[['dt','type','fenqi_cycle','fenqi','order_month_diff','type_rate','amount_rate','capital_rate']]
        #print 'df_all columns:',df_cycle.columns.tolist()
        return df_all



    def get_date_product_new_bill_items(self, this_dt):
        ''' 根据传入的日期,返回对应的各个产品的交易额分布  '''
        return_list   = [ self.dict_columns['dt'],
                          self.dict_columns['type'],
                          self.dict_columns['fenqi_cycle'],
                          self.dict_columns['fenqi'],
                          self.dict_columns['order_month_diff'],
                          self.dict_columns['capital_sum'],
                          self.dict_columns['order_num'],
                          self.dict_columns['user_num'],
                          self.dict_columns['all_orign_amount']]
        #print 'this_dt:',this_dt
        # dt type, all_amount
        df_amount = self.get_date_order_amount(this_dt)   #日期: this_dt  对应的放款额
        # 获取各个产品各分期,在各自上级款额占比
        df_all      = self.get_product_cycle_fenqi_amount_rate(this_dt)
        df_all  = pd.merge(df_amount, df_all, how='left', on=[self.dict_columns['dt'], self.dict_columns['type']])
        # 现金、实物等总放款额
        #df_all[self.dict_columns['all_amount']] = this_amount   #当日总放款额
        #df_all['product_type_amount'] = df_all.apply(lambda x: (x[self.dict_columns['type_rate']] * x[self.dict_columns['all_amount']]), axis=1)
        # 各分期类型、期数的总交易额
        #if len(df_all) < 1:
        #    return pd.DataFrame()
        df_all[self.dict_columns['all_orign_amount']] = df_all.apply(lambda x: (x[self.dict_columns['amount_rate']] * x[self.dict_columns['all_amount']]), axis=1)
        # 各账期应还
        df_all[self.dict_columns['capital_sum']] = df_all.apply(lambda x: (x[self.dict_columns['capital_rate']] * x[self.dict_columns['all_orign_amount']]), axis=1)
        # 订单数量  num,all_num
        df_all[self.dict_columns['order_num']] = 999999
        # 人数
        df_all[self.dict_columns['user_num']] = 999999
        df_all = df_all[return_list]
        #if this_dt == "2017-12-01":
        #    print 'dt:',this_dt,',amount:',df_amount,',sum:',df_amount['all_amount'].sum()
        #    print 'day sum:',df_all[self.dict_columns['capital_sum']].sum()
        #print 'end df_cycle:',df_cycle.head()
        return  df_all



    def get_order_deadline_dt(self, this_line):
        ''' 根据订单创建时间、类型、分期数、账期，生成账期时间 '''
        order_dt            = this_line[ self.dict_columns['dt'] ]
        order_type          = this_line[ self.dict_columns['type'] ]
        order_cycle         = this_line[ self.dict_columns['fenqi_cycle'] ]
        order_fenqi         = this_line[ self.dict_columns['fenqi'] ]
        order_month_diff    = this_line[ self.dict_columns['order_month_diff'] ]
        #
        deadline_dt=""
        #print this_line
        if order_cycle  == 2 :
            deadline_dt = (datetime.strptime(order_dt, '%Y-%m-%d') + relativedelta(months=( int(order_month_diff) )) + relativedelta(days=(-1)) ).strftime("%Y-%m-%d")
        elif order_cycle  == 1 :
            deadline_dt = (datetime.strptime(order_dt, '%Y-%m-%d') + relativedelta(weeks=( int(order_month_diff) )) + relativedelta(days=(-1)) ).strftime("%Y-%m-%d")
        else :
            pass
        return deadline_dt



    def create_new_bill_items(self):
        ''' 根据交易额、各个产品占比,生成账单数据 '''
        month_tmp = list(set(self.df_amount[self.dict_columns['dt']].tolist()))   # 交易额配置文件中的月份
        month_tmp.sort()
        #返回字段
        return_list     = [ self.dict_columns['dt'],
                            self.dict_columns['deadline_dt'],
                            self.dict_columns['type'],
                            self.dict_columns['fenqi_cycle'],
                            self.dict_columns['fenqi'],
                            self.dict_columns['order_month_diff'],
                            self.dict_columns['capital_sum'],
                            self.dict_columns['order_num'],
                            self.dict_columns['user_num'],
                            self.dict_columns['all_orign_amount']]
        #一个月一个月的生成新账单
        for m_v in month_tmp :
            if m_v < self.this_dt[:7]:  #若月份小于当月跳过
                continue
            #计算一个月有多少天, 返回整数
            this_month_days = int(((datetime.strptime(m_v, '%Y-%m') + relativedelta(months=(1))) - datetime.strptime(m_v, '%Y-%m')).days)
            #对一个月的每一天生成一个新账单
            #if m_v == "2017-12":
            #    print 'month_tmp:',m_v,',month len:',this_month_days
            for i in range(this_month_days):
                this_deadline   = (datetime.strptime(m_v, '%Y-%m') + relativedelta(days=(i))).strftime("%Y-%m-%d")
                # 获得 this deadline 日期下的各个产品交易额
                df_tmp  =  self.get_date_product_new_bill_items(this_deadline)
                df_tmp[self.dict_columns['dt']] = this_deadline
                df_tmp[self.dict_columns['deadline_dt']] = df_tmp.apply(lambda x:  self.get_order_deadline_dt(x), axis=1)
                #df_tmp[self.dict_columns['deadline_dt']] = df_tmp.apply(lambda x: ( datetime.strptime(x[self.dict_columns['dt']], '%Y-%m-%d') + relativedelta(months=( int(x[self.dict_columns['order_month_diff']]) )) + relativedelta(days=(-1)) ).strftime("%Y-%m-%d") if x[self.dict_columns['fenqi_cycle']] == 2 else ( datetime.strptime(x[self.dict_columns['dt']], '%Y-%m-%d') + relativedelta(weeks=( int(x[self.dict_columns['order_month_diff']]) )) + relativedelta(days=(-1)) ).strftime("%Y-%m-%d") , axis=1)
                df_tmp = df_tmp[return_list]
                if len(self.df_new_bill) == 0:
                    self.df_new_bill = df_tmp
                else:
                    self.df_new_bill = pd.concat([self.df_new_bill,df_tmp])
                del df_tmp
        pass
        #对新产生的账单补充历史数据,保证column与现在的账单表保持一致,方便后期merge
        #this_columns = self.df_now_bill.columns.tolist()[len(self.df_new_bill.columns.tolist()):]
        bill_now_columns = self.df_now_bill.columns.tolist()
        bill_new_columns = self.df_new_bill.columns.tolist()
        # 根据现在的账单表，补充新产生的账单表columns
        for i in bill_now_columns :
            if i in bill_new_columns :
                continue
            self.df_new_bill[i]=0
        pass
        #根据现在的账单表，删除现在的账单表中不存在的columns
        for i in bill_new_columns :
            if i in bill_now_columns:
                continue
            del self.df_new_bill[i]
        pass


    def merge_new_now_bill_items(self):
        ''' '''
        df_all_bill = pd.concat([self.df_now_bill[(self.df_now_bill['dt'] < self.this_dt)], self.df_new_bill[(self.df_new_bill['dt'] >= self.this_dt)]])
        df_all_bill.fillna("0", inplace=True)
        df_all_bill = df_all_bill[self.df_now_bill.columns.tolist()]
        return df_all_bill


    def filter_preduct_now_bill(self):
        ''' 对现在账单数据，过滤掉其他产品 '''
        #订单类型 :现金、实物、趣先享
        self.filter_preduct_bill(self.dict_columns['type'], self.this_type)
        #分期类型：月、周
        self.filter_preduct_bill(self.dict_columns['fenqi_cycle'], self.this_fenqi_cycle)
        #期数
        self.filter_preduct_bill(self.dict_columns['fenqi'], self.this_fenqi)


    def get_merge_now_new_bills(self):
        ''' 把现在的账单表和未来的账单表merge，并返回 '''
        #reload file
        self.reload_file_to_dataframe()
        # create new bill data
        self.create_new_bill_items()
        #过滤不匹配的产品
        self.filter_preduct_now_bill()
        # merge  now and new bill data
        df = self.merge_new_now_bill_items()
        df = df.reset_index()
        del df['index']
        return df


    def create_new_bill_main(self):
        '''  生成新账单表 '''
        #生成新账单
        self.df_all_bill    = self.get_merge_now_new_bills()
        #保存文件
        self.download_df_to_file(self.df_all_bill, self.bill_data_all_file)
        pass




def main():
    ''' '''
    if len(sys.argv) < 3 :
        print  "python2.7 CreateNewBill.py  config_file   \"month/day\""
        sys.exit()
    print sys.argv
    config_file     = sys.argv[1]
    this_dt_type    = sys.argv[2]
    if len(sys.argv) > 3:
        this_dt     = sys.argv[3]
    else :
        this_dt     = datetime.now().strftime("%Y-%m")  if this_dt_type  == "month"  else datetime.now().strftime("%Y-%m-%d")
    pass
    print 'config file:',config_file,',this date type:',this_dt_type,',this dt:',this_dt
    run=CreateNewBill(config_file, this_dt_type, this_dt)
    run.create_new_bill_main()

if __name__=="__main__":
    main()



