import pandas as pd
import numpy as np
import math
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


class OptimizeParams(object):

    def __init__(self, path_predict, path_true, path_changkou_predict, path_changkou_true, output_path):

        self.target_dt    = ''
        self.path_predict = path_predict
        self.path_true    = path_true
        self.path_changkou_predict = path_changkou_predict
        self.path_changkou_true = path_changkou_true
        self.output_path = output_path
        #self.merged_df    = pd.DataFrame()
        self.predict      = pd.DataFrame()
        self.true         = pd.DataFrame()
        self.changkou_predict     = pd.DataFrame()
        self.changkou_true     = pd.DataFrame()
        #self.predict_target    = pd.DataFrame()
        #self.true_target       = pd.DataFrame()
        self.key_list          = ['dt','deadline_dt','type','fenqi_cycle','fenqi','order_month_diff']
        self.amount_list       = ['capital_sum','num','all_num','all_orign_amount']


    def prepare_data(self):

        self.predict = pd.read_csv(self.path_predict)
        self.true    = pd.read_csv(self.path_true)
        self.changkou_predict = pd.read_csv(self.path_changkou_predict)
        self.changkou_true = pd.read_csv(self.path_changkou_true)

        self.predict['deadline_dt_key'] = self.predict['deadline_dt'].apply(lambda x : x[:7])
        self.true['deadline_dt_key'] = self.true['deadline_dt'].apply(lambda x : x[:7])
        # find out key target time, eg 2017-11-22
        self.target_dt = list(self.predict.columns)[191]
        true_dt = list(self.true.columns)[191]
        before_list = list(self.true.columns)[:10]
        after_list = list(self.true.columns)[191:]
        mid_list = [(datetime.strptime(true_dt,'%Y-%m-%d')+timedelta(days=i)).strftime('%Y-%m-%d') for i in range(-181,0)]
        self.true.columns =  before_list + mid_list + after_list
        #print list(self.true.columns)
        #self.predict_target = self.predict[self.key_list + self.amount_list + self.dt_list]
        #self.true_target    = self.true[self.key_list + self.amount_list + self.dt_list]
        #self.merged_df = pd.merge(self.predict_target, self.true_target, how='inner', on=self.key_list)

    def find_diff(self):
        #print self.target_dt
        res = []
        # exposure
        #res.append([self.changkou_true['M0+'].values[0],self.changkou_predict['M0+'].values[0]])
        # d1+
        res.append([sum(self.true[self.true['deadline_dt']<=self.target_dt][self.target_dt])
                ,sum(self.predict[self.predict['deadline_dt']<=self.target_dt][self.target_dt])]) 
        # d1
        res.append([sum(self.true[self.true['deadline_dt']==self.target_dt][self.target_dt])
                ,sum(self.predict[self.predict['deadline_dt']==self.target_dt][self.target_dt])])
        # m0
        start_dt = self.target_dt[:7]+'-01'
        res.append([sum(self.true[(self.true['deadline_dt']<self.target_dt) & (self.true['deadline_dt']>=start_dt)][self.target_dt])
                     ,sum(self.predict[(self.predict['deadline_dt']<self.target_dt) & (self.predict['deadline_dt']>=start_dt)][self.target_dt])])
        # m1-m5
        m_dt = [(datetime.strptime(self.target_dt,'%Y-%m-%d')+relativedelta(months=i)).strftime('%Y-%m') for i in range(-1,-7,-1)]
        for m_x in m_dt[:-1]:
            res.append([sum(self.true[self.true['deadline_dt_key']==m_x][self.target_dt])
                ,sum(self.predict[self.predict['deadline_dt_key']==m_x][self.target_dt])])
        # m6+
        res.append([sum(self.true[self.true['deadline_dt_key']<=m_dt[-1]][self.target_dt])
                ,sum(self.predict[self.predict['deadline_dt_key']<=m_dt[-1]][self.target_dt])])


        #diff_df = self.merged_df[(self.merged_df['2017-11-01_diff'] != 0) | (self.merged_df['2017-11-02_diff'] != 0) | 
        #                    (self.merged_df['2017-11-03_diff'] != 0) | (self.merged_df['2017-11-04_diff'] != 0) | 
        #                    (self.merged_df['2017-11-05_diff'] != 0) | (self.merged_df['2017-11-06_diff'] != 0) ]
        #tmp1 = diff_df[self.key_list+[self.dt_predict+'_x',self.dt_predict+'_y',self.dt_predict+'_diff']]
    #tmp1['abs_diff'] = tmp1[self.dt_predict+'_diff'].apply(np.abs)
        res_df = pd.DataFrame(res)
        res_df.index = ['D1+_overdue','D1_overdue','M0_overdue','M1_overdue','M2_overdue','M3_overdue','M4_overdue','M5_overdue','M6+_overdue']
        res_df.columns = ['TRUE','PREDICT']
        res_df['DIFF'] = res_df['PREDICT'] - res_df['TRUE']
        res_df.to_csv(self.output_path)
        return  res_df
        #return diff_df.groupby(['type','fenqi_cycle','fenqi'],as_index=False).sum()[[
        #    'type','fenqi_cycle','fenqi',self.dt_target+'_diff']].sort_values(by=self.dt_target+'_diff',ascending=False)
        #return tmp1.sort_values(by ='abs_diff', ascending=False).head(20)

    def update_coef(self):
        
        tmp_true = self.true[self.true['deadline_dt']==self.target_dt].groupby(
                    ['type','fenqi_cycle','fenqi'],as_index=False).sum()[['type','fenqi_cycle','fenqi']+[self.target_dt]]
        tmp_predict = self.predict[self.predict['deadline_dt']==self.target_dt].groupby(
                    ['type','fenqi_cycle','fenqi'],as_index=False).sum()[['type','fenqi_cycle','fenqi']+[self.target_dt]]
        tmp_df = pd.merge(tmp_true, tmp_predict, on=['type','fenqi_cycle','fenqi'])
        tmp_df['product_diff'] = tmp_df[self.target_dt+'_y'] - tmp_df[self.target_dt+'_x']
        tmp_df['diff_rate'] = tmp_df['product_diff']/tmp_df[self.target_dt+'_x']
        tmp_df['coef'] = [1 - i if i < 1 else 0.5 for i in tmp_df['diff_rate'].values ]
        # read coefficient table
        coef_old = pd.read_csv("./laifenqi_ck_estimate/data/output/parameters_order_type_migration_risk_rate_config_file")
        coef_merge = pd.merge(coef_old, tmp_df, how='left',on=['type','fenqi_cycle','fenqi'])
        coef_merge = coef_merge.fillna(1)
        coef_merge['risk_rate'] = coef_merge['risk_rate'] * coef_merge['coef']
        coef_merge['risk_rate'] = coef_merge['risk_rate'].apply(lambda x: x if x>1.0 else 1.0)
        coef_new = coef_merge[coef_old.columns]
        coef_new.to_csv("./laifenqi_ck_estimate/data/output/parameters_order_type_migration_risk_rate_config_file",sep=',',index=False)


    def run(self):
        self.prepare_data()
        print '------------end of preparing data--------'
        #self.create_diff()
        self.find_diff()
        print '-----------end of finding difference-----'
        #self.update_coef()
        #print '-----------end of updating coefficient-----'


def main():
    path_changkou_predict = "./laifenqi_ck_estimate/data/output/laifenqi_exposure_overdue_result_predict.csv"
    path_changkou_true = "./laifenqi_ck_estimate/data/output/laifenqi_exposure_overdue_result.csv"
    path_predict = "./laifenqi_ck_estimate/data/output/this_predict_overdue_capital_file_predict.csv"
    path_true = "./laifenqi_ck_estimate/data/output/this_predict_overdue_capital_file.csv"
    output_path = "./laifenqi_ck_estimate/data/output/daily_risk_report_diff.csv"

    run = OptimizeParams(path_predict, path_true, path_changkou_predict, path_changkou_true, output_path)
    run.run()    

if __name__ == "__main__":
    main()   
