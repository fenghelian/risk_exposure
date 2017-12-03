#!/usr/bin/python
#encoding=utf-8


from pandas import Series,DataFrame
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import math

from sqlalchemy import create_engine
import MySQLdb
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

sys.path.append("./../lib/")
import unittest
from amount_estimator import EstimateAmount
from public_lib import PublicLib




class DealDataFormat(object):
    ''' 检测并，处理数据格式 '''
    def __init__(self, config_file, amount_flag,  version, this_dt):
        ''' '''
        print datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conf                    = ConfigParser.ConfigParser()
        conf.read(config_file)
        self.path                                               = os.path.dirname(os.path.abspath(__file__))
        self.fenqi_cycle_amount_rate_old_file                   = ("%s/../../%s" % (self.path, conf.get("config_data", "fenqi_cycle_amount_rate_old_file")))              #历史数据中：不同订单类型在总放贷额中占比
        self.fenqi_cycle_amount_rate_new_file                   = ("%s/../../%s" % (self.path, conf.get("config_data", "fenqi_cycle_amount_rate_new_file")))              #新配置数据：不同订单类型在总放贷额中占比
        self.fenqi_cycle_fenqi_diff_amount_rate_old_file        = ("%s/../../%s" % (self.path, conf.get("config_data", "fenqi_cycle_fenqi_diff_amount_rate_old_file")))   #历史数据中：不同分期在相应订单类型放贷额中占比
        self.fenqi_cycle_fenqi_diff_amount_rate_new_file        = ("%s/../../%s" % (self.path, conf.get("config_data", "fenqi_cycle_fenqi_diff_amount_rate_new_file")))   #新配置数据：不同分期在相应订单类型放贷额中占比
        self.bill_data_now_file                                 = ("%s/../../%s" % (self.path, conf.get("config_data", "order_bill_now_data_file")))                      #到现在为止集团的账单数据
        #
        self.order_create_cycle_fenqi_capital_rate_input        = ("%s/../../%s" % (self.path, conf.get("config_data", "order_create_cycle_fenqi_capital_rate_input")))    #历史创建订单，各账期本金分布
        self.order_create_cycle_fenqi_capital_rate              = ("%s/../../%s" % (self.path, conf.get("config_data", "order_create_cycle_fenqi_capital_rate_file")))    #历史创建订单，各账期本金分布
        self.order_cycle_fenqi_capital_rate_config              = ("%s/../../%s" % (self.path, conf.get("config_data", "order_cycle_fenqi_capital_rate_config_file")))    #订单各个账期本金分布 配置
        self.type_fenqi_cycle_fenqi_capital_rate                = ("%s/../../%s" % (self.path, conf.get("config_data", "type_fenqi_cycle_fenqi_capital_rate_file")))      #不同产品，各个账期资金占比
        #各月各产品放款额预算
        self.qudian_amount_data_new_file                        = ("%s/../../%s" % (self.path, conf.get("config_data", "qudian_amount_data_file")))
        self.qudian_amount_history_file                         = ("%s/../../%s" % (self.path, conf.get("config_data", "qudian_amount_history_file")))
        self.qudian_amount_days_new_data_file                   = ("%s/../../%s" % (self.path, conf.get("config_data", "qudian_amount_days_new_data_file")))
        #现金、实物、趣先享等资金在当日放款额分布
        self.qudian_xj_sw_amount_data_new_file                  = ("%s/../../%s" % (self.path, conf.get("config_data", "qudian_xj_sw_amount_data_new_file")))
        self.qudian_xj_sw_amount_data_old_file                  = ("%s/../../%s" % (self.path, conf.get("config_data", "qudian_xj_sw_amount_data_old_file")))
        self.type_cycle_fenqi_capital_rate_old_file             = ("%s/../../%s" % (self.path, conf.get("config_data", "type_fenqi_cycle_fenqi_capital_rate_old_file")))
        self.this_dt_type       = "day"
        self.this_dt            = this_dt
        self.amount_flag        = amount_flag
        self.version            = version
        print '加载文件......'

    def __del__(self):
        print '加载完毕.',datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    def deal_bill_capital_distribution(self):
        ''' 处理各账期资金分布 '''
        this_order='this_order'
        this_order_diff='order_month_diff'
        this_columns=['type', 'fenqi_cycle', 'fenqi',this_order_diff,'capital_rate']
        # 读取相关文件
        df  = pd.read_csv(self.order_create_cycle_fenqi_capital_rate_input, sep=',', encoding='utf-8', error_bad_lines=False)
        df.rename(columns={this_order:this_order_diff}, inplace=True)
        #df_0 = df[['order_dt'] + this_columns]
        #df_0.rename(columns={'this_order':'order_month_diff'}, inplace=True)
        #df_0.to_csv(self.type_cycle_fenqi_capital_rate_old_file, sep=',', encoding='utf-8', index=False)
        #order_dt,type,fenqi_cycle,fenqi,order_month_diff,capital_rate
        df[['order_dt'] + this_columns].to_csv(self.type_cycle_fenqi_capital_rate_old_file, sep=',', encoding='utf-8', index=False)
        # 数据转换格式, 并写回原文件
        df_tmp = pd.pivot_table(df, values='capital_rate', index=['order_dt','type','fenqi_cycle','fenqi'], columns=this_order_diff).reset_index()
        df_tmp.to_csv(self.order_create_cycle_fenqi_capital_rate, sep=',', encoding='utf-8', index=False)
        # 生成配置的 各个账期资金分布
        # 统计近两个月的所有产品 各个账期资金分布
        #begin_dt    = (datetime.strptime(self.this_dt, "%Y-%m") + relativedelta(months=(-2))).strftime("%Y-%m")  if  self.this_dt_type == "month" else (datetime.strptime(self.this_dt, "%Y-%m-%d") + relativedelta(months=(-2))).strftime("%Y-%m-%d")
        begin_dt    = (datetime.strptime(self.this_dt, "%Y-%m-%d") + relativedelta(months=(-2))).strftime("%Y-%m-%d")
        #
        df_tmp = df[(df['order_dt'] >= begin_dt)].sort_values(by=['type','fenqi_cycle','fenqi',this_order_diff,'order_dt'], axis=0, ascending=False, inplace=False, kind='quicksort', na_position='last').drop_duplicates(subset=['type','fenqi_cycle','fenqi',this_order_diff])
        df_0 = df.sort_values(by=['type','fenqi_cycle','fenqi',this_order_diff,'order_dt'], axis=0, ascending=False, inplace=False, kind='quicksort', na_position='last').drop_duplicates(subset=['type','fenqi_cycle','fenqi',this_order_diff])[this_columns]
        pd.pivot_table(df_0, values='capital_rate', index=['type','fenqi_cycle','fenqi'], columns=this_order_diff).reset_index().to_csv(self.order_cycle_fenqi_capital_rate_config, sep=',', encoding='utf-8', index=False)
        # 查看周订单交易额本金分布是否包含
        if  len(df_tmp[(df_tmp['type']==5) & (df_tmp['fenqi_cycle']==1) & (df_tmp['fenqi_cycle']==1)]) < 1:
            df_0 = pd.DataFrame([[5, 1, 1, 1, 1.0], [6, 1, 1, 1, 1.0]], columns=this_columns)
            df_tmp = pd.concat([df_0, df_tmp[this_columns]])
        pass
        df_tmp['order_dt'] = self.this_dt
        #df_tmp.rename(columns={'this_order':'order_month_diff'}, inplace=True)
        # 写入配置文件
#        print len(df_tmp)
        df_tmp_all = pd.read_csv("./../../data/config/type_fenqi_cycle_fenqi_capital_rate_file_all.csv", sep=',', encoding='utf-8')
        df_tmp_all['product_key'] = [str(t)+str(fc)+str(f) for t,fc,f in zip(df_tmp_all['type'] , df_tmp_all['fenqi_cycle'] , df_tmp_all['fenqi'])]
        target_product = [str(t)+str(fc)+str(f) for t,fc,f in zip(df_tmp['type'] , df_tmp['fenqi_cycle'] , df_tmp['fenqi'])]
        remove_index = []
        for i in range(len(df_tmp_all)):
            if df_tmp_all.loc[i,'product_key'] in target_product:
                remove_index.append(i)
        df_tmp_part = df_tmp_all.drop(df_tmp_all.index[remove_index])
        del df_tmp_part['product_key']
        df_tmp_part['order_dt'] = self.this_dt
#        print len(df_tmp_all)
#        print len(df_tmp_part)
        df_tmp = df_tmp[['order_dt','type','fenqi_cycle','fenqi','order_month_diff','capital_rate']]
        df_total = pd.concat([df_tmp, df_tmp_part])
        df_total.to_csv(self.type_fenqi_cycle_fenqi_capital_rate, sep=',', encoding='utf-8', index=False)
        pass

    def get_time_section_dataframe(self, df, this_dt):
        ''' 统计一段时间数据 '''
        # 统计起始时间
        #begin_dt    = (datetime.strptime(self.this_dt, "%Y-%m") + relativedelta(months=(-2))).strftime("%Y-%m")  if  self.this_dt_type == "month" else (datetime.strptime(self.this_dt, "%Y-%m-%d") + relativedelta(months=(-2))).strftime("%Y-%m-%d")
        # 统计终止时间
        #end_dt      = (datetime.strptime(self.this_dt, "%Y-%m") + relativedelta(months=(-1))).strftime("%Y-%m")  if  self.this_dt_type == "month" else (datetime.strptime(self.this_dt, "%Y-%m-%d") + relativedelta(months=(-1))).strftime("%Y-%m-%d")
        begin_dt    = (datetime.strptime(self.this_dt, "%Y-%m-%d") + relativedelta(months=(-2))).strftime("%Y-%m-%d")
        # 统计终止时间
        end_dt      = (datetime.strptime(self.this_dt, "%Y-%m-%d") + relativedelta(months=(-1))).strftime("%Y-%m-%d")
        return df[(df[this_dt] >= begin_dt) & (df[this_dt] <= end_dt)]


    def deal_order_amount_distribution(self):
        '''处理各个产品放款额占比 '''
        order_dt='dt'
        # 读取相关文件
        df = pd.read_csv(self.fenqi_cycle_amount_rate_old_file, sep=',', encoding='utf-8', error_bad_lines=False)
        # 统计一段时间数据
        df_ts   = self.get_time_section_dataframe(df, order_dt)
        df_tmp = df_ts.groupby(['type','fenqi_cycle','fenqi'])['orign_amount', 'all_orign_amount'].sum().reset_index()
        df_tmp['amount_rate'] = df_tmp['orign_amount']/df_tmp['all_orign_amount']
        df_tmp['dt'] = self.this_dt
        df_tmp[['dt','type','fenqi_cycle','fenqi','amount_rate']].to_csv(self.fenqi_cycle_amount_rate_new_file, sep=',', encoding='utf-8', index=False)
        pass


    def get_xj_sw_dataframe_groupby(self, df):
        '''    '''
        order_dt='dt'
        order_type='type'
        orign_amount='orign_amount'
        type_rate   ='type_rate'
        df_amount   = df.groupby([order_dt])[orign_amount].sum().reset_index()
        df_amount.rename(columns={orign_amount:'all_orign_amount'}, inplace=True)
        df_type     = df.groupby([order_dt, order_type])[orign_amount].sum().reset_index()
        df_all      = pd.merge(df_type, df_amount, how='left', on=[order_dt])
        df_all[type_rate]   = df_all[orign_amount]/df_all['all_orign_amount']
        return df_all[[order_dt, order_type, type_rate]]

    def deal_xj_sw_amount_distribution(self):
        ''' 处理现金、实物在当日放款额资金占比 '''
        # dt,type,fenqi_cycle,fenqi,amount_rate,order_num_rate,orign_amount,all_orign_amount   usecols=['car_id','buyer_phone']
        order_dt='dt'
        order_type='type'
        orign_amount='orign_amount'
        type_rate   ='type_rate'
        df = pd.read_csv(self.fenqi_cycle_amount_rate_old_file, sep=',', encoding='utf-8', usecols=[order_dt, order_type, orign_amount], error_bad_lines=False)
        df_old  = self.get_xj_sw_dataframe_groupby(df)
        #
        df_tmp  = self.get_time_section_dataframe(df, order_dt)
        df_tmp[order_dt] = self.this_dt
        df_new  = self.get_xj_sw_dataframe_groupby(df_tmp)
        #
        df_new.to_csv(self.qudian_xj_sw_amount_data_new_file, sep=',', encoding='utf-8', index=False)
        df_old.to_csv(self.qudian_xj_sw_amount_data_old_file, sep=',', encoding='utf-8', index=False)

    def deal_qudian_amount_distribution(self):
        ''' 把月粒度的各产品的放款额，打散到每一天 '''
        this_columns=['dt','type','orign_amount']
        check_rate  = 0.99995
        public_fun  = PublicLib()
        df = pd.read_csv(self.qudian_amount_data_new_file, sep=',', encoding='utf-8', usecols=this_columns, error_bad_lines=False)
        estimator = EstimateAmount(self.qudian_amount_history_file, 5)
        estimator.run(self.qudian_amount_data_new_file)
        fd = open(self.qudian_amount_days_new_data_file, 'w+')
        fd.write(",".join(this_columns) + "\n")
        #2017-09,5,7053109429.01
        for k,v in enumerate(df.values.tolist()):
            this_month  = v[0]
            this_type   = v[1]
            this_amount = v[2]
            #print v
            this_month_day  = public_fun.get_month_date_list(this_month)
            result = estimator.get_estimated_money(str(this_month), str(this_type))
            total_money = 0.0
            total_rate  = 0.0
            for j,entity in enumerate(result):
                total_money += entity._money
                total_rate += entity._rate
                this_day    = this_month_day[j]
                this_str    = str(this_day) + "," + str(this_type) + "," + str(entity._money) + "\n"
                fd.write(this_str)
            pass
            if total_rate < check_rate  or total_money < (check_rate * float(this_amount)) :
                print 'error:月到天额度分配有问题!!!'
        pass
        fd.close()
        
    
    
    def process_loan_data(self, df):
        """
        配置未来月份的总放款额，和各产品放款额的占比
        """
        
        #prediction = pd.read_csv(origin_file, sep = ",", header=0, encoding="gb2312")
        prediction = df[['dt','business','type','fenqi_cycle','fenqi','origin_amount']]
        tmp_amount = prediction.groupby(['dt','type'], as_index=False).sum()[['dt','type','origin_amount']]
        tmp_fenqi_cycle_amount = prediction.groupby(['dt','type','fenqi_cycle','fenqi'], as_index=False).sum()
        qudian_amount_data_file = tmp_amount.copy()
        qudian_amount_data_file['dt'] = qudian_amount_data_file['dt'].apply(lambda x : x[0:7])
        tmp_fenqi_rate = pd.merge(tmp_fenqi_cycle_amount,tmp_amount,on=['dt','type'])
        tmp_fenqi_rate['amount_rate'] = tmp_fenqi_rate['origin_amount_x']/tmp_fenqi_rate['origin_amount_y']
        tmp_fenqi_rate = tmp_fenqi_rate[tmp_fenqi_rate['amount_rate']!=0][['dt','type','fenqi_cycle','fenqi','amount_rate']]
        qudian_amount_data_file.columns = ['dt','type','orign_amount']
        qudian_amount_data_file.to_csv("./../../data/config/qudian_amount_data_file.csv", index = False)
        tmp_fenqi_rate.to_csv("./../../data/config/fenqi_cycle_amount_new_rate.txt", sep=',',index = False)

    def read_data_from_sql(self):
        
        # generate configure dataset
        db = MySQLdb.connect(host='192.168.4.198',user='risk',passwd='qudianrisk',db='changkou',charset="utf8")
        cur = db.cursor()
#        print self.version
        if self.version == "NULL":
            sql = 'select * from origin_amount where business="laifenqi" and activation=1;'
        else:
            sql = 'select * from origin_amount where business="laifenqi" and version="%s";' % self.version
        df = pd.read_sql(sql, con=db)
        self.process_loan_data(df)
        cur.close()
    
    def this_format_data_main(self):
        ''' '''
        tag = 1
        if tag == 1:
            self.read_data_from_sql()
        #处理各账期资金分布
        self.deal_bill_capital_distribution()
        #处理各个产品放款额占比
        if int(self.amount_flag)  == 1:
            self.deal_order_amount_distribution()
        #处理现金、实物在当日放款额资金占比
        self.deal_xj_sw_amount_distribution()
        #处理放款额在各日期内的分布问题
        self.deal_qudian_amount_distribution()





def main():
    ''' '''
    # data formatting
    if len(sys.argv) < 4 :
        print  "python2.7 deal_data_format.py  config_file "
        sys.exit()
    config_file       = sys.argv[1]
    amount_flag       = sys.argv[2]
    version           = sys.argv[3]
    if len(sys.argv) > 4:
        this_dt = sys.argv[4]
    else :
        this_dt = datetime.now().strftime("%Y-%m-%d")   #
    print 'deal data format: config file:',config_file,'version: ', version, ',this_dt:',this_dt
    run = DealDataFormat(config_file, amount_flag,  version, this_dt)
    run.this_format_data_main()
    pass


if __name__=="__main__":
    main()



