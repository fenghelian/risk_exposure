#!/bin/bash

#awk -F',' '{if($1=="2017-08" || FNR==1){printf("%s,%s,%s,%s,%s,%s",$1,$2,$3,$4,$5,$6);for(i=27;i<=NF;i++)printf(",%s",$i);printf("\n");};}' data/output/this_predict_overdue_capital_file.csv
#
#/home/luobaowei/odpscmd/bin/odpscmd  -f laifenqi_month_order_open_money_down_odps.sql
#
#python2.7 fitting_parameters.py  data/config/config_data  "2017-12"
#
#python2.7 start_server.py  data/config/config_data  "2017-12"  "month"

ODPS="/home/luobaowei/odpscmd/bin/odpscmd"
this_config="data/config/config_data"
this_date_end=$1
this_dt_type=$2
this_update_date=$3
this_parameters_num=$#


if [[ "$this_dt_type" == "day" ]];then 
	this_dt=$(date -d'1 days ago' +'%Y-%m-%d')
	next_dt=$(date +'%Y-%m-%d')
else 
	this_dt=$(date -d'1 months ago' +'%Y-%m')
	next_dt=$(date +'%Y-%m')
fi

check_project_parameter()
{
#if [[ "$this_dt_type" != "month" -a "$this_dt_type" != "day" ]];then
#if [[ "$this_dt_type" != "month" ]] && [[ "$this_dt_type" != "day" ]]; then 
	if [[ $this_parameters_num -lt 3 ]];then
		echo 'USAGE:Start_Server.sh  date_end date_type update_data'
		exit
	fi
	if [[ "$this_dt_type" != "month" ]] && [[ "$this_dt_type" != "day" ]]; then
	  echo "please enter day type"
	  exit
	fi

}


get_data_from_odps ()
{
	echo "will count and down data from odps!!!"
	if [[ "$this_dt_type" == "month" ]];then
		$ODPS  -f data/laifenqi_month_order_open_money_down_odps_new.sql
	fi
	if [[ "$this_dt_type" == "day" ]];then
		$ODPS  -f  data/laifenqi_day_order_open_money_down_odps_new.sql
	fi
	echo "count and down data from odps over!!!"
}


deal_download_odps_data()
{
	echo "will preliminary deal odps data!!!"
	order_fenqi_rate="data/input/order_create_cycle_fenqi_capital_rate.csv"
	config_fenqi_rate="data/config/type_fenqi_cycle_fenqi_capital_rate_file.csv"
	config_fenqi_rate_pivot="data/config/order_cycle_fenqi_capital_rate_config_file.csv"
	awk -F',' 'BEGIN{this_dt="'$this_dt'";next_dt="'$next_dt'"}{if(FNR == 1 || $1 == this_dt){if(FNR==1){printf("%s", $1);}else{printf("%s", next_dt);}; print ","$2","$3","$4","$5","$8;  }}END{print next_dt",5,1,1,1,1.0"}'   $order_fenqi_rate  >  $config_fenqi_rate
	python2.7 deal_data_format.py  $order_fenqi_rate  "pivot_table"
	#处理各账期资金分布
	awk -F',' 'BEGIN{this_dt="2017-07";}{if(FNR==1 || $1 >= this_dt){tp=$2","$3","$4;if(tp in a){}else{a[tp]=1;for(i=2;i<NF;i++){printf("%s,", $i);};printf("%s\n", $NF);}}}'  $order_fenqi_rate | sort -t$','  -k1,1n -k2,2n -k3,3n   >  $config_fenqi_rate_pivot
	awk -F',' 'BEGIN{this_dt="'$this_dt'";next_dt="'$next_dt'";}{if(FNR == 1 || $1 == this_dt){if(FNR==1){printf("%s", $1);}else{printf("%s", next_dt);}; for(i=2;i<=5;i++){printf(",%s", $i);}; printf("\n");  }}'   data/input/fenqi_cycle_amount_old_rate.txt > data/config/fenqi_cycle_amount_new_rate.txt
	echo "preliminary deal odps data over!!!"
}


fitting_parameters()
{
	echo "will fitting parameters!!!"
	#python2.7 fitting_parameters.py  data/config/config_data  "2017-12-31"  "day"
	#python2.7 fitting_parameters.py  $this_config  $this_date_end   $this_dt_type
	echo "will deal mean parameters ......"
	python deal_mean_parameters.py  $this_config   $this_dt_type
	echo "will deal created orders parameters ......"
	python deal_parameters_rules.py  $this_config   $this_dt_type
	echo "fitting parameters over!!!"
}


count_open_money()
{
	echo "will count laifenqi order open money!!!"
	python2.7  start_server.py  $this_config  $this_date_end  $this_dt_type  85  80   1525  1555
	echo "count laifenqi order open money over!!!"
}


main()
{
	#检查 传入参数
	check_project_parameter
	#从odps计算，并下载数据
	if [[ "$this_update_date" == "update" ]];then
		echo 'will download data from odps, please wait!!!'
		get_data_from_odps
	fi
	#处理odps数据
	deal_download_odps_data
	#拟合参数
	fitting_parameters
	#计算敞口
	count_open_money
}

main
