#coding=utf-8
'''
Created on 2017年9月27日

@author: huangpingchun
'''
import unittest
from lib.amount_estimator import EstimateAmount

class Test(unittest.TestCase):
    
    def testEstimateAmount(self):
        special_date='2017-11'
        product_history_amount_table = '/Users/huangpingchun/Documents/workspace/qudian_ck_estimate/data/config/qudian_amount_history_data.csv'
        product_estimate_amount_table = '/Users/huangpingchun/Documents/workspace/qudian_ck_estimate/data/config/qudian_amount_data_file.csv'
        #print product_table
        estimator = EstimateAmount(product_history_amount_table, 12)
        estimator.run(product_estimate_amount_table)
        
        result = estimator.get_estimated_money(special_date, '6')
        print("-----预估赋值" + special_date + "月-----")
        total_money = 0
        total_rate = 0
        for entity in result:
            total_money += entity._money
            total_rate += entity._rate
            print entity.to_string()
        print(total_money,total_rate)

if __name__ == "__main__":
    import sys;sys.argv = ['', 'Test.testEstimateAmount']
    unittest.main()
    
