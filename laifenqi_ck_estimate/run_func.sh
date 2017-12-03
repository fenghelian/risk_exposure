

check_input_params()
{
	this_para=$1
	if test $this_para -eq 0 -o $this_para = NULL ; then
		echo -e "参数还未上传！！"
		exit
	fi
}

check_origin_amount()
{
	this_count=$1
	if [[ $this_count -eq 0 ]] ; then
		echo -e "交易额还未上传！！"
		exit
	fi
}

data_preparation()
{
	yesterday=$1
	ts=$2
	# 更新sql
	sed -e 's/\$this_dt/'${yesterday}'/' -e 's/\$bl/'${bl}'/' data/laifenqi_day_order_open_money_down_odps_new_this_dt.sql > data/laifenqi_day_order_open_money_down_odps_new.sql
	sed -e 's/\$this_dt/'${yesterday}'/' -e 's/\$ts/'${ts}'/' data/laifenqi_upload_changkou_overdue_result_this_dt.sql > data/laifenqi_upload_changkou_overdue_result.sql
	# 下载数据
	/home/risk/home/luobaowei/odpscmd/bin/odpscmd  -f data/laifenqi_day_order_open_money_down_odps_new.sql
}

data_preprocess()
{
	today=$1
	version=$2
	/home/risk/bin/python2.7 deal_data_format.py       ../../data/config/config_data   0  $version   $today
	/home/risk/bin/python2.7 deal_mean_parameters.py   ../../data/config/config_data   $bl
	/home/risk/bin/python2.7 deal_parameters_rules.py  ../../data/config/config_data   $bl  $today
}

calculate_exposure()
{
	ts=$1
	rollrate=$2
	risk=$3
	criteria=$4
	today=$5
	/home/risk/bin/python2.7 exposure_estimator.py     ../../data/config/config_data   $terminal  $ts   $rollrate  $risk  $criteria  $bl  $today
}

upload_data()
{
	/home/risk/home/luobaowei/odpscmd/bin/odpscmd  -f ../../data/laifenqi_upload_changkou_overdue_result.sql
}

main()
{
	switch=$1
    bl=$2	
	terminal=$3
	if [[ $switch -eq 1 ]] ; then
    	# 检查参数表
    	end=`mysql -u risk -pqudianrisk  -r -s  -e "select sum(activation) from changkou.input_params where business=\"$bl\";"`	
    	check_input_params $end
		# 检查交易额表
		version=`mysql -u risk -pqudianrisk  -r -s  -e "use changkou;select version from input_params where activation = 1 and business = \"$bl\";"`
		if [[ $version -eq NULL ]] ; then
			count1=`mysql -u risk -pqudianrisk  -r -s  -e "select count(*) from changkou.origin_amount where activation = 1  and business=\"$bl\";"`
			check_origin_amount $count1
		else
			count2=`mysql -u risk -pqudianrisk  -r -s  -e "select count(*) from changkou.origin_amount where version = \"$version\"  and business=\"$bl\";"`
			check_origin_amount $count2
		fi
		# 从数据库中获取参数
		today=`mysql -u risk -pqudianrisk  -r -s  -e "use changkou;select dt from input_params where activation = 1 and business = \"$bl\";"`
		sec=`date -d $today +%s`
		sec_yesterday=$((sec - 86400))
		yesterday=`date -d @$sec_yesterday +%F`
		ts=`mysql -u risk -pqudianrisk  -r -s  -e "use changkou;select particle_size from input_params where activation = 1 and business = \"$bl\";"`
		criteria=`mysql -u risk -pqudianrisk  -r -s  -e "use changkou;select stats from input_params where activation = 1 and business = \"$bl\";"`
		rollrate=`mysql -u risk -pqudianrisk  -r -s  -e "use changkou;select roll_rate from input_params where activation = 1 and business = \"$bl\";"`
		risk=`mysql -u risk -pqudianrisk  -r -s  -e "use changkou;select risk from input_params where activation = 1 and business = \"$bl\";"`
		version=`mysql -u risk -pqudianrisk  -r -s  -e "use changkou;select version from input_params where activation = 1 and business = \"$bl\";"`
		# 预测前先将参数表的activation字段设置为0
		mysql -urisk -pqudianrisk  -e "use changkou;update input_params set activation=0 where business=\"$bl\";"
	else
		today=$(date -d"0 day" +"%Y-%m-%d")
		yesterday=$(date -d"-1 day" +"%Y-%m-%d")
		#today='2017-12-02'
		#yesterday='2017-12-01'
		ts="day"
		criteria="month"
		rollrate=1.0
		risk=1.0
		version="test_1123"
	fi

	# 数据准备
	data_preparation  $yesterday  $ts
	echo "-------数据准备结束------"
	cd src/app
	# 数据处理
	data_preprocess  $today  $version
	echo "-------数据处理结束------"
	# 计算敞口
	calculate_exposure  $ts   $rollrate  $risk  $criteria  $today 
	echo "-------计算敞口结束------"
	
    # 上传数据
	#upload_data
	# copy 文件作为下一次diff比较
	#cp ../../data/output/laifenqi_exposure_overdue_result.csv ../../data/output/laifenqi_exposure_overdue_result_predict.csv
	#cp ../../data/output/this_predict_overdue_capital_file.csv ../../data/output/this_predict_overdue_capital_file_predict.csv
    echo "计算敞口全部完成！"
}

main $1   $2   $3
