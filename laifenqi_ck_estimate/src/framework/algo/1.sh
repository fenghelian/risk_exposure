#!/bin/sh





python this_test.py   ./../../../data/config/config_data   "2017-11-19"  "day"  5 1 1   0.90 1.85 0.01  "risk"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  5 2 1   0.90 1.85 0.01  "risk"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  5 2 3   0.90 1.85 0.01  "risk"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  5 2 6   0.90 1.85 0.01  "risk"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  5 2 9   0.90 1.85 0.01  "risk"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  5 2 12  0.90 1.85 0.01  "risk"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  6 2 2   0.90 1.85 0.01  "risk"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  6 2 3   0.90 1.85 0.01  "risk"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  6 2 6   0.90 1.85 0.01  "risk"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  6 2 9   0.90 1.85 0.01  "risk"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  6 2 12  0.90 1.85 0.01  "risk"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  8 2 6   0.90 1.85 0.01  "risk"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  8 2 12  0.90 1.85 0.01  "risk"  "2017-11-14"


python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  5 2 12  0.65 1.55 0.01  "migration"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  5 1 1   0.65 1.55 0.01  "migration"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  5 2 1   0.65 1.55 0.01  "migration"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  5 2 3   0.65 1.55 0.01  "migration"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  5 2 6   0.65 1.55 0.01  "migration"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  5 2 9   0.65 1.55 0.01  "migration"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  6 2 2   0.65 1.55 0.01  "migration"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  6 2 3   0.65 1.55 0.01  "migration"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  8 2 6   0.65 1.55 0.01  "migration"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  6 2 6   0.65 1.55 0.01  "migration"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  6 2 9   0.65 1.55 0.01  "migration"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  8 2 12  0.65 1.55 0.01  "migration"  "2017-11-14"
python this_test.py   ./../../../data/config/config_data   "2017-11-21"  "day"  6 2 12  0.65 1.55 0.01  "migration"  "2017-11-14"


#grep ^"result:"  risk_log.txt  | head -1 | awk -F'[: ,]' '{for(i=1;i<=NF;i++)print i":"$i;}'
#grep ^"result:"  risk_log.txt  | awk -F'[: ,]' '{tp=$3","$4","$5","$6;if(tp in a){print tp","$8","$9","a[tp];}else{a[tp]=$8","$9;};next;}'
#echo "bs,type,fenqi_cycle,fenqi,migration_rate,migration_lose,risk_rate,risk_lose"  > parameters_order_type_migration_risk_rate_config_file
#grep ^"result:"  risk_log.txt  | awk -F'[: ,]' '{tp=$3","$4","$5","$6;if(tp in a){print tp","$8","$9","a[tp];}else{a[tp]=$8","$9;};next;}' >> parameters_order_type_migration_risk_rate_config_file 



