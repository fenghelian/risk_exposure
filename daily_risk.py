
import pandas as pd
import numpy as np
from collections import Counter
import math
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


class DailyRisk(object):

    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.target_date = ''
        self.target_list = []
        self.date_array = {}
        self.predict_overdue = pd.DataFrame()


    def prepare_data(self):

        self.predict_overdue = pd.read_csv(self.input_path)
        columns_list = list(self.predict_overdue.columns)
        self.target_date = columns_list[191]
        self.target_list = [(datetime.strptime(self.target_date,'%Y-%m-%d')+timedelta(days=i)).strftime('%Y-%m-%d') for i in range(6,-30,-1)]
        #print self.target_list
        before_list = columns_list[:10]
        after_list = columns_list[191:]
        mid_list = [(datetime.strptime(self.target_date,'%Y-%m-%d')+timedelta(days=i)).strftime('%Y-%m-%d') for i in range(-181,0)]
        self.predict_overdue.columns = before_list + mid_list + after_list
        for date in self.target_list:
            self.date_array[date] = [(datetime.strptime(date,'%Y-%m-%d')+timedelta(days=i)).strftime('%Y-%m-%d') for i in range(30)]
        #print self.date_array

    def get_overdue_rate(self, dt, lst):
#        print dt
#        print lst
        tmp = self.predict_overdue[self.predict_overdue['deadline_dt'] == dt][['deadline_dt','capital_sum']+lst].groupby(['deadline_dt'],as_index=False).sum()
        for column in lst:
            tmp[column+'_rate'] = tmp[column]/tmp['capital_sum']
        return tmp[[column+'_rate' for column in lst]].values[0]

    def get_result(self):

        res = {}
        for k in self.date_array.keys():
        #print self.date_array[k]
            res[k] = self.get_overdue_rate(k, self.date_array[k])     
        res_df = pd.DataFrame(res).T
        #print res_df
        res_df.columns = ['d_'+str(i) for i in range(1,31)]
        res_df['deadline_dt'] = res_df.index
        res_df = res_df[['deadline_dt'] + ['d_'+str(i) for i in range(1,31)]]
        return res_df

def main():

    input_path = './laifenqi_ck_estimate/data/output/this_predict_overdue_capital_file.csv'
    output_path = './laifenqi_ck_estimate/data/output/daily_risk_report.csv'
    dailyRisk = DailyRisk(input_path, output_path)
    dailyRisk.prepare_data()
    ret = dailyRisk.get_result()
    ret.to_csv(output_path, index=False)

if __name__ == "__main__":
    main()
