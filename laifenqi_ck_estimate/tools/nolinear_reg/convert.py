#coding=utf-8
import sys
import math
import subprocess
from gridregression import *

def convert_svr_format(data_file, x_dim):
    tf = open(data_file)
    out = open("train.txt", "w")
    data = []
    for line in tf:
        data.append(line.strip())
    tf.close()
    data.reverse()
    for i, y in enumerate(data):
        if len(data[i+1:]) <= x_dim:break
        #out.write(y +"="+ str(math.log(float(y.strip()), 2)) +"|"+ str(math.pow(2, math.log(float(y.strip()), 2))) + '\n'),
        out.write(y.strip() + '\t')
        #print str(float(y.strip()) * 10) + '\t',
        for j, x in enumerate(data[i+1:]):
            if j >  x_dim:break
            if j == 0:
                #diff = abs(float(x) - float(y))
                out.write(str(j + 1) + ":" + x +" ")
            else:
                #print str(j + 1) + ":" + str(abs(float(x.strip()) - float(data[j - 1]))) +" ",
                out.write(str(j + 1) + ":" + x +" ")
        out.write('\n')
    out.close()
 

if __name__ == '__main__':
    convert_svr_format(sys.argv[1], int(sys.argv[2]))
        
    mse, params = find_parameters('train.txt', "")
    #epsilon-SVR 效果较好
    cmd = './svm-train -s 3 -c ' + str(params.get('c')) + ' -g ' + str(params.get('g')) + ' -p ' + str(params.get('p')) \
          + ' train.txt train.model'
    print "开始训练 "
    print cmd
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    line = popen.stdout.readline()
    while line != "":
        print line.strip()
        line = popen.stdout.readline()

    popen.kill()
    cmd = './svm-predict train.txt train.model out'
    print "开始预估"
    print cmd
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,shell=True)
    line = popen.stdout.readline()
    while line != "":
        print line.strip()
        line = popen.stdout.readline()

    popen.kill()
