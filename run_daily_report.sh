cd laifenqi_ck_estimate/
echo "copy today's file as predition file"
cp ./data/output/this_predict_overdue_capital_file.csv  ./data/output/this_predict_overdue_capital_file_predict.csv
cp ./data/output/laifenqi_exposure_overdue_result.csv  ./data/output/laifenqi_exposure_overdue_result_predict.csv
echo "end of copy"
# 运行今天预测
echo "---------begin predict exposure-------"
sh run_func.sh 3  laifenqi  2018-01-20
echo "---------end predict exposure-------"
cd ..
echo "--------begin calculate the difference-------"
/home/risk/bin/python2.7 check_diff.py
echo "--------end calculate the difference-------"
echo "--------begin report future risk-------"
/home/risk/bin/python2.7 daily_risk.py
echo "-------end future risk--------"
cd tool_bak/
echo "-------sending email-------"
/home/risk/bin/python2.7 CheckDiffSend.py  config_file
echo "-------end sending email------"

echo "=======END======"
