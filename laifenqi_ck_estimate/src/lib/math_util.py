#coding=utf-8
'''
Created on 2017年9月19日

@author: huangpingchun
'''
import numpy as np
from sklearn import preprocessing
import calendar
import datetime
from dateutil.relativedelta import relativedelta

def scale_by_zscore(data):
    '''
    '''
    scale_data = abs(preprocessing.scale(data))
    return scale_data / np.sum(scale_data)

def cal_rate(data):
    '''
    '''
    return data / np.sum(data + 1)

def test_zscore():
    test_data = [143,3445554,44,56,35,433]
    #均值，方差
    dd = np.array(test_data, dtype='float32')
    dd = dd / (np.sum(dd))
    print dd
    scale_data = abs(preprocessing.scale(test_data))
    print scale_data / np.sum(scale_data)
    
if __name__ == '__main__':
    print calendar.monthcalendar(2017,2)
    print calendar.monthcalendar(2017,7)
    print calendar.monthcalendar(2017,8)
    print calendar.monthcalendar(2017,9)
    print calendar.weekday(2017, 8, 2)
    print(datetime.date(2018, 1, 2) - relativedelta(months=1))
    #test_zscore()