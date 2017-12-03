#coding=utf-8
#from svmutil import *
import random,time
from grid_search import *
"""
m = svm_load_model('./train.model')
y, x = [0.034313606], [{1:0.029448002,2:0.033424576,3:0.030902798,4:0.029285576,5:0.026745313,6:0.029851518,7:0.027908503,8:0.028531438}]
p_label, p_acc, p_val = svm_predict(y, x, m)
print p_label, x
"""
class Test(object):
    def __init__(self):
        pass
    
    def mse_callback(self, params):
        print params
        time.sleep(4)
        return random.randint(1000, 100000)


find_parameters('-c 0.75,0.83,0.01', Test().mse_callback)

print find_parameters('-c 0.75,0.83,0.01', mse_callback)
