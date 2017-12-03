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

from scipy import optimize

from sklearn import linear_model

from public_lib  import PublicLib



class  FitModelLib(PublicLib):
    ''' 参数拟合函数库 '''
    def __init__(self):
        ''' '''
        print 'will count fitting parameters.....'


    def fitting_linear_model(self, x_data, y_data, x_predict) :
        ''' LinearRegression 线性回归，用历史数据进行训练，对预测数据进行预测 '''
        #print 'x_data:',x_data,',y_data:',y_data,',x_predict:',x_predict
        ret_dict={}
        reg = linear_model.LinearRegression()
        reg.fit(x_data, y_data)
        this_list   = reg.predict(x_predict)
        #print 'predict list:',this_list
        for i,v in enumerate(x_predict, 0) :
            ret_dict[v[0]] = this_list[i]
        return ret_dict


    def fit_inverse_fun(self, x, a, b, c):
        ''' 反比例函数 '''
        return  a/(x*b) + c

    def fit_exp_fun(self, x, a, b, c):
        ''' 指数函数 '''
        return a*np.exp(b/x) + c


    def fitting_function_model(self, this_fun, x_data, y_data, x_predict):
        ''' 通用函数拟合模型 '''
        #print 'x_data:',x_data,',y_data:',y_data,',x_predict:',x_predict
        try :
            fita,fitb=optimize.curve_fit(this_fun,x_data,y_data)
        except  RuntimeError :
            return {},[]
        y_predict = this_fun(x_predict, fita[0], fita[1], fita[2])
        return self.merge_list_to_dict(x_predict, y_predict), fita

    def fitting_function_model_predict(self, this_fun, x_predict, fitp):
        ''' 通用函数拟合模型 '''
        y_predict = this_fun(x_predict, fitp[0], fitp[1], fitp[2])
        return self.merge_list_to_dict(x_predict, y_predict)

    def fitting_model_self(self, model_flag, x_data, y_data, x_predict):
        ''' 函数拟合模型  model_flag: "exp":指数函数拟合模型; "inverse":反比例函数拟合模型 '''
        if model_flag == "inverse":
            #反比例函数拟合模型
            return self.fitting_function_model(self.fit_inverse_fun, x_data, y_data, x_predict)
        elif model_flag == "exp":
            #指数函数拟合模型
            return self.fitting_function_model(self.fit_exp_fun, x_data, y_data, x_predict)
        else :
            return {},[]
        pass

    def fitting_model_predict(self, model_flag, x_predict, fitp):
        ''' 函数拟合模型  自带模型参数预测: model_flag: "exp":指数函数拟合模型; "inverse":反比例函数拟合模型 '''
        #return self.fitting_function_model_predict(self.fit_inverse_fun, x_predict, fitp)  if model_flag == "inverse"   else  self.fitting_function_model_predict(self.fit_exp_fun, x_predict, fitp)
        if model_flag == "inverse" :
            return self.fitting_function_model_predict(self.fit_inverse_fun, x_predict, fitp)
        elif model_flag == "exp" :
            return self.fitting_function_model_predict(self.fit_exp_fun, x_predict, fitp)
        else :
            pass


    def fitmodel_test_main(self):
        ''' 测试调优函数 '''
        x_data=np.arange(1,11,1)
        y_data=np.array([0.035311,0.012701,0.007908,0.005405,0.004222,0.003605,0.003196,0.002898,0.002671,0.002505])
        x_predict=np.arange(10,15,1)
        print self.fitting_exp_model(x_data, y_data, x_predict)


def main():
    ''' '''
    run = FitModelLib()
    run.fitmodel_test_main()

if __name__=="__main__":
    main()



