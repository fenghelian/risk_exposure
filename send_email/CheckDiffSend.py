#coding=utf-8

from pandas import Series,DataFrame
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import MySQLdb

import sys
reload(sys)
sys.setdefaultencoding('utf8')
import os
import time
from datetime import datetime,timedelta,date
import ConfigParser

sys.path.append("./tool/")
import Mailer




class CheckDiff():
    def __init__(self, config_file):
        '''  '''
        conf  = ConfigParser.ConfigParser()
        conf.read(config_file)
        self.path                               = os.path.dirname(os.path.abspath(__file__))
        # 今天和昨天----
        self.check_diff                         = ("%s/%s" % (self.path, conf.get("config_data", "check_diff")))
        self.daily_risk                         = ("%s/%s" % (self.path, conf.get("config_data", "daily_risk")))
        self.email_config_file                  = ("%s/%s" % (self.path, conf.get("config_data", "email_config_file")))
        #
        self.email_title                        = "风险日报"
        #

    def __del__(self):
        ''' '''
        print 'over'

    def send_file(self, this_file):
        ''' '''
        with open(this_file, 'r') as f:
            lines = f.readlines()
            strs = '<br />'.join(lines)
        return strs


    def send_frame(self, this_file):
        ''' '''
        with open(this_file, 'r') as f:
            strs = '<h2></h2><table border="1"><tr>'
            line = f.readline()
            for i in line.strip('\n').split(','):
                strs  += '<td>' + i + '</td>'
            line = f.readline()
            strs += '</tr>'
            while line:
                strs += '<tr>'
                for i in line.strip('\n').split(','):
                    strs += '<td>' + str(i) + '</td>'
                strs += '</tr>'
                line = f.readline()
            strs += '</table></br>'
        return strs

    def add_comma(self,a):
        return '{:,}'.format(int(round(a)))


    def transform(self,a):
        return '{:.2%}'.format(a)


    def reload_file_dataframe(self):
        ''' '''
        df_check_diff           = pd.read_csv(self.check_diff, sep=',', encoding='utf-8') 
        df_daily_risk           = pd.read_csv(self.daily_risk, sep=',', encoding='utf-8') 
        df_check_diff.columns = ['DATE','TRUE','PREDICT','DIFF']
        df_daily_risk = df_daily_risk.sort_values(by='deadline_dt', ascending = False)
        for column in df_daily_risk.columns[1:]:
            #df_daily_risk[column] = df_daily_risk[column].apply(lambda x: round(x,4))
            df_daily_risk[column] = df_daily_risk[column].apply(self.transform)

        for column in df_check_diff.columns[1:]:
            df_check_diff[column] = df_check_diff[column].apply(self.add_comma)
        #predicted_date             = self.df_laifenqi['predicted_date']
        #del self.df_laifenqi['predicted_date']
        #del self.df_qudian['predicted_date']
        #self.df_qudian             = self.df_qudian + self.df_qudian_before_2016
        #self.df_jituan             = self.df_laifenqi + self.df_qudian
        #self.df_rate = self.df_diff / self.df_yesterday
        #maximum = np.max(np.abs(self.df_rate.values))
        return df_check_diff, df_daily_risk

    def get_str_from_dataframe(self, df1, df2):
        #print 'df:',df
        #db = MySQLdb.connect(host='192.168.4.198',user='risk',passwd='qudianrisk',db='changkou',charset="utf8")
        #sql = 'select version from origin_amount where business="laifenqi" order by created_at desc limit 1;'
        #df = pd.read_sql(sql, con=db)
        #version = df['version'].values[0]
        strs = '<h2></h2><table border="1"><tr><p>Hi all, 本次来分期订单敞口的风险日报详细如表1、2所示：</p>' 
        strs += '</br>'
        strs += '<p>(注：来分期用户不含芝麻分下探和600+被拒召回)</p>'
        strs += "<tr><h1>表1  预测来分期订单逾期金额与实际值DIFF</h1>"
        for i in df1.columns.tolist():
            strs  += '<td>' + i + '&nbsp;&nbsp;&nbsp;&nbsp;</td>'
        strs += '</tr>'
        for j,line in enumerate(df1.values.tolist()):
            strs += '<tr>'
            for  k,v  in enumerate(line):
                strs += '<td>' + str(v) + '&nbsp;&nbsp;&nbsp;&nbsp;</td>' 
            strs += '</tr></tr>'
        strs += '</table></br>'

        strs += '<h2></h2><table border="1"><tr>'
        strs += "<h1>表2  预测未来七天的逾期率水平(deadline_dt_all为账单到期日,粗体为预估时间)</h1>"
        for i in df2.columns.tolist():
            if i=='deadline_dt':
                strs  += '<td>' + i + '_all' + '&nbsp;&nbsp;&nbsp;&nbsp;</td>'
            else:
                strs  += '<td>' + i  + '&nbsp;&nbsp;&nbsp;&nbsp;</td>'
        strs += '</tr>'
        for j,line in enumerate(df2.values.tolist()):
            strs += '<tr>'
            for  k,v  in enumerate(line):
                if k==0 and v>=date.today().strftime("%Y-%m-%d"):
                    strs += '<td><strong>' + str(v) + '&nbsp;&nbsp;&nbsp;&nbsp;</strong></td>'
                else:
                    strs += '<td>' + str(v) + '&nbsp;&nbsp;&nbsp;&nbsp;</td>'
            strs += '</tr>'
        strs += '</table></br>'

        return strs

    def send_email_fun(self, df1, df2):
        ''' '''
        send_str = self.get_str_from_dataframe(df1, df2)
        email    = Mailer.Mailer(self.email_config_file)
        email.send_mail(self.email_title, send_str)


    def this_main(self):
        ''' '''
        df1, df2 = self.reload_file_dataframe()
        #print 'ret:df:',df
        #发邮件
        self.send_email_fun(df1,df2)



def main():
    
    config_file = sys.argv[1]
    run = CheckDiff(config_file)
    run.this_main()


if __name__=="__main__":
    main()
