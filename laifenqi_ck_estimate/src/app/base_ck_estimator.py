#coding=utf-8
'''
Created on 2017年9月19日

@author: huangpingchun
'''
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pandas import Series,DataFrame
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import time


sys.path.append("./../lib/")

from reload_file            import ReloadFile
from create_new_bill        import CreateNewBill
from count_overdue          import CountOverdue
from count_order_exposure   import CountOrderExposure
from base_init_parameters   import BaseInitParameters



class BaseCKEstimator(BaseInitParameters, ReloadFile, CountOverdue, CountOrderExposure):
    '''
    classdocs
    '''

    def __init__(self, config_file, this_end_month, this_dt_type, this_dt):
        '''
        Constructor
        '''
        BaseInitParameters.__init__(self, config_file, this_end_month, this_dt_type, this_dt)
        self.this_config_file       = config_file
        pass


    def preprocess(self):
        ''' 预处理 '''
        #读取数据
        self.reload_file_main()
        #print self.df_true_bill.columns.tolist()
        this_columns = self.df_true_bill.columns.tolist()
        #this_columns=['dt','deadline_dt','type','fenqi_cycle','fenqi','order_month_diff','capital_sum','num','all_num','all_orign_amount'] + [ (datetime.strptime(self.this_end_month, "%Y-%m-%d") + relativedelta(days=((i-30)))).strftime("%Y-%m-%d") for i in range(31)]
        len_all = len(this_columns)
        len_column = 10
        len_date = len_all - len_column
        this_columns=this_columns[0:len_column] + [ (datetime.strptime(self.this_end_month, "%Y-%m-%d") + relativedelta(days=((i-len_date+1)))).strftime("%Y-%m-%d") for i in range(len_date)]
        #print this_columns
        self.df_true_bill.columns = this_columns
        pass


    def process(self):
        ''' '''
        #merge现在的账单数据和未来数据生成新的账单数据
        #1: 生成账单表;2: 初始化，读文件；
        bill_flag = 0
        if  bill_flag == 0 :
            self.df_all_bill = pd.read_csv(self.bill_data_all_file, sep=',', encoding='utf-8')
        else :
            create_bill  = CreateNewBill(self.this_config_file, self.this_dt_type, self.this_dt)
            self.df_all_bill = create_bill.get_merge_now_new_bills()
        #self.df_all_bill = self.get_merge_now_new_bills()
        #self.create_new_bill_main()
        #self.filter_preduct_now_bill()
        pass

    def select_one_condition_bill(self, df, this_column, this_value):
        ''' '''
        if this_value == "all"  or int(this_value) == 999 or len(df) < 1:
            df_tmp = df
        else :
            df_tmp = df[(df[this_column] == int(this_value))]
        pass
        return df_tmp

    def get_filter_preduct_bill(self, df_bill, this_type, this_cycle, this_fenqi):
        ''' '''
        #订单类型 :现金、实物、趣先享
        df  =   self.select_one_condition_bill(df_bill, self.dict_columns['type'], this_type)
        #分期类型：月、周
        df  =   self.select_one_condition_bill(df, self.dict_columns['fenqi_cycle'], this_cycle)
        #期数
        df  =   self.select_one_condition_bill(df, self.dict_columns['fenqi'], this_fenqi)
        return df


    def count_overdue(self):
        ''' '''
        #根据merge的账单表，逾期率数据计算逾期金额
        self.count_date_bill_overdue()
        pass

    def get_bill_true_overdue_data(self, df, this_flag):
        ''' '''
        #print 'dt:',self.this_dt,',end dt:',self.this_end_month,',type:',self.this_type,',fenqi_cycle:',self.this_fenqi_cycle,',fenqi:',self.this_fenqi
        if this_flag == "migration" :  #type,fenqi_cycle,fenqi
            df_tmp  = self.get_filter_preduct_bill(df[(df['deadline_dt'] < self.this_dt)], self.this_type, self.this_fenqi_cycle, self.this_fenqi)
        elif this_flag == "risk":
            df_tmp  = self.get_filter_preduct_bill(df[(df['deadline_dt'] >= self.this_dt) & (df['deadline_dt'] <= self.this_end_month)], self.this_type, self.this_fenqi_cycle, self.this_fenqi)
        else:
            df_tmp  = pd.DataFrame()
        #print 'df after:',df_tmp.head()
        return df_tmp
        #df_tmp[[ i for i in self.month_list ]].sum().tolist()
        #return this_value

    def get_this_true_value(self, df, this_flag):
        ''' '''
        df_tmp = self.get_bill_true_overdue_data(df, this_flag)
        this_value = df_tmp[[ i for i in self.month_list ]].astype(np.float64).sum().tolist()
        return this_value

    def get_bill_overdue_parameters(self, df, migration_rate, risk_rate):
        ''' 根据传入迁徙率和未来风险系数，预估账单逾期金额 '''
        #根据merge的账单表，逾期率数据计算逾期金额
        #print 'migration_rate:',migration_rate,',risk_rate:',risk_rate,',df:',df.head()
        self.estimatior_bill_overdue(df, migration_rate, risk_rate)
        this_value  = df[[ i for i in self.month_list ]].astype(np.float64).sum().tolist()
        return this_value

    def split_bill_by_month(self, df, count_deadline):
        ''' '''
        this_month_dt   =  (datetime.strptime(count_deadline, "%Y-%m-%d") + relativedelta(days=(1)) + relativedelta(months=(-1)) + relativedelta(days=(-1)) ).strftime("%Y-%m-%d")
        this_day_dt   =  (datetime.strptime(count_deadline, "%Y-%m-%d") + relativedelta(weeks=(-1))  + relativedelta(days=(0))).strftime("%Y-%m-%d")
        #print 'count_deadline:',count_deadline,',this_month_dt:',this_month_dt,',before:deadline_dt<=',this_month_dt,',current: deadline_dt>',this_month_dt,'  and deadline_dt<=',count_deadline,',this_day_dt:',this_day_dt
        #df_before   = df[((df['deadline_dt'] <= this_month_dt) & (df['fenqi_cycle'] == 2.0)) | ( (df['deadline_dt'] <= this_day_dt) & (df['fenqi_cycle'] == 1.0))]
        #df_current  = df[((df['deadline_dt'] > this_month_dt) & (df['deadline_dt'] <= count_deadline) & (df['fenqi_cycle'] == 2.0)) | ((df['deadline_dt'] > this_day_dt) & (df['deadline_dt'] <= count_deadline) & (df['fenqi_cycle'] == 1.0))]
        # 周订单
        df_before_week  = df[(df['deadline_dt'] <= this_day_dt) & (df['fenqi_cycle'] == 1.0)]
        df_current_week = df[(df['deadline_dt'] > this_day_dt) & (df['deadline_dt'] <= count_deadline) & (df['fenqi_cycle'] == 1.0)]
        # 月订单
        df_before_month  = df[(df['deadline_dt'] <= this_month_dt) & (df['fenqi_cycle'] == 2.0)]
        df_current_month = df[(df['deadline_dt'] > this_month_dt) & (df['deadline_dt'] <= count_deadline) & (df['fenqi_cycle'] == 2.0)]
        df_before   =  pd.concat([df_before_month, df_before_week])
        df_current  =  pd.concat([df_current_week, df_current_month])
        return df_before,df_current


    def computer_bill_exposure(self, df, deadline_dt, count_deadline):
        ''' '''
        if len(df) < 1:
            return 0.0,0.0
        df_before, df_current = self.split_bill_by_month(df, count_deadline)
        #print '----------------------------split_bill_by_month 耗时：',end-begin,'seconds--------------------------'
        #print 'df_before:',df_before['deadline_dt'].head()
        #print 'df_current:',df_current['deadline_dt'].head()
        if len(df_current) < 1 :
            print 'Warning !!!!!!!!!!!!!!!\nall len:',len(df),'before len:',len(df_before),',current len:',len(df_current)
            df_current['overdue_loan'] = 0.0
        else :
            #print len(df_current)
            df_current['overdue_loan']  = df_current.apply(lambda x: self.computer_overdue_loan_amount(x), axis=1)
            #print '----------------------------computer_bill_exposure 耗时：',end-begin,'seconds--------------------------'
        this_overdue        = df[deadline_dt].astype(np.float64).sum()
        this_exposure       = df_before[deadline_dt].astype(np.float64).sum() + df_current['overdue_loan'].astype(np.float64).sum()
        #print 'this_overdue:',this_overdue,',this_exposure:',this_exposure,',before overdue:',df_before[deadline_dt].astype(np.float64).sum(),',overdue_loan:',df_current['overdue_loan'].astype(np.float64).sum()
        #before_file="check_file/before_" + deadline_dt + "_" + count_deadline
        #current_file="check_file/current_" + deadline_dt + "_" + count_deadline
        #df_before.to_csv(before_file, sep=',', encoding='utf-8', index=False)
        #df_current.to_csv(current_file, sep=',', encoding='utf-8', index=False)
        #print '----------------------------astype 耗时：',end-begin,'seconds--------------------------'
        return this_overdue,this_exposure



    def estimator_order_exposure(self, df, deadline_dt):
        ''' 计算账单在deadline_dt日期下的订单敞口 '''
        #['D1+'],['D1+_overdue'], ['D31+'], ['D31+_overdue'], ['D61+'], ['D61+_overdue'], ['D91+'], ['D91+_overdue'], ['D121+'], ['D121+_overdue'], ['D151+'], ['D151+_overdue'], ['D181+'], ['D181+_overdue']]
        #all_overdue_amount,before_overdue,current_loan  = self.computer_bill_exposure(df, deadline_dt)
        #return all_overdue_amount,before_overdue,current_loan,(before_overdue+current_loan)
        columns_list=['dt','deadline_dt','type','fenqi_cycle','fenqi','order_month_diff','capital_sum','all_orign_amount', deadline_dt]
        ret_list=[]
        for i in range(0, 7, 1):
            begin = time.time()
            #day_num = (-1) * 30 * i
            #count_deadline_dt = (datetime.strptime(deadline_dt, "%Y-%m-%d") + relativedelta(days=(day_num))).strftime("%Y-%m-%d")
            if self.output_type  == "month":
                day_num = (-1) * i
                count_deadline_dt = (datetime.strptime(deadline_dt,"%Y-%m-%d") + relativedelta(days=(1)) + relativedelta(months=(day_num)) + relativedelta(days=(-1))).strftime("%Y-%m-%d")
            else :
                day_num = (-1) * 30 * i 
                count_deadline_dt = (datetime.strptime(deadline_dt, "%Y-%m-%d") + relativedelta(days=(day_num))).strftime("%Y-%m-%d")
            #print 'i:',i,',day_num:',day_num
            #print 'deadline_dt:',deadline_dt,',count_deadline_dt:',count_deadline_dt
            df_tmp = df[(df['deadline_dt'] <= count_deadline_dt)][columns_list]
            this_overdue,this_exposure  = self.computer_bill_exposure(df_tmp, deadline_dt, count_deadline_dt)
            ret_list.extend([this_exposure,this_overdue])
            end = time.time()
            #print '-------------------------第',i,'次循环的耗时为：',end-begin,'seconds---------------------------'
        pass
        return ret_list


    def base_ck_estimator(self):
        ''' '''
        self.preprocess()
        self.process()
        self.count_overdue()
        self.count_ck()



