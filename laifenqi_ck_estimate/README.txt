一：下载数据
修改laifenqi_day_order_open_money_down_odps_new.sql数据分区信息

运行命令:
/home/risk/home/xxx/odps/bin/odpscmd  -f data/laifenqi_day_order_open_money_down_odps_new.sql


二: 整理初始化数据，并生成各产品基础逾期率d1 ~ d181

运行命令:
cd  src/app/
python deal_data_format.py  ../../data/config/config_data  1/0  "2017-10-19"       #  1: 生成新的各账期占比; 0: 不生成新的账期占比
python deal_mean_parameters.py   ../../data/config/config_data   "laifenqi"
python deal_parameters_rules.py ../../data/config/config_data "laifenqi" "2017-10-19"
cd -



三：修改配置文件：未来放款额、各个产品放款额占比……
data/config/qudian_amount_data_file.csv     #财务预算各个月放款额
data/config/fenqi_cycle_amount_new_rate.txt   #各个分期在各自产品中的占比 例: 现金周订单一期，在现金放款额占比
data/config/qudian_xj_sw_amount_data_new_file.csv   # 现金、实物、趣先享在总放款额占比，可以不修改
data/config/type_fenqi_cycle_fenqi_capital_rate_file.csv    #产品资金分布，一般不动，可以不修改

四： 预估各个产品敞口

运行命令:

cd  src/app/
python exposure_estimator.py   ../../data/config/config_data "2017-12-31"  "month"   1.000  1.050  "month"  "laifenqi"  "2017-10-19"
#							   配置文件						 最终日期    day/month  留存  未来风险   month/day（逾期）   数据时间
cd -


