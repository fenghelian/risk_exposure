#coding=utf-8
'''
Created on 2017年9月19日

@author: huangpingchun
'''

import subprocess

class DataDownload(object):
    '''
    classdocs
    '''


    def __init__(self, dt_type='month', sql_script):
        '''
            Constructor
        '''
        #dt_type = 'month'/'day'
        self.cmdline = "sh ../script/download_odps_data.sh " \
                    + dt_type + " " + sql_script
                        
    def run(self):
        subprocess.Popen(self.cmdline)
        #return dd
        