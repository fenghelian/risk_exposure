#coding=utf-8
'''
Created on 2017年9月19日

@author: huangpingchun
'''
import datetime 
from dateutil.relativedelta import relativedelta


def get_month_range(year, month, day, n_size):
    count = 0
    pre_months = []
    while (count < n_size):
        
        current_date = datetime.date(year, month, day) - relativedelta(months=1)
        year = current_date.year
        month = current_date.month
        day = current_date.day
        pre_months.append((str(year), month))
        count += 1
    return pre_months