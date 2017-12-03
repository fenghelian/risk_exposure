this_dt_type=$1
echo "will count and down data from odps!!!"
if [[ "$this_dt_type" == "month" ]];then
	$ODPS  -f data/laifenqi_month_order_open_money_down_odps_new.sql
fi
if [[ "$this_dt_type" == "day" ]];then
	$ODPS  -f  data/laifenqi_day_order_open_money_down_odps_new.sql
fi
echo "count and down data from odps over!!!"