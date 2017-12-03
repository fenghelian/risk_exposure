alter table bidata_market.dm_jituan_changkou_predict_result_dt drop if exists partition (bs="laifenqi",ts="$ts",pt="$this_dt");
alter table bidata_market.dm_jituan_changkou_predict_overdue_result_dt drop if exists partition (bs="laifenqi",ts="$ts",pt="$this_dt");
alter table bidata_market.dm_jituan_changkou_predict_result_dt add partition (bs="laifenqi",ts="$ts",pt="$this_dt");
alter table bidata_market.dm_jituan_changkou_predict_overdue_result_dt add partition (bs="laifenqi",ts="$ts",pt="$this_dt");
tunnel upload /home/risk/home/changkou_project/laifenqi_ck_estimate_no_test/data/output/laifenqi_exposure_overdue_result.csv   bidata_market.dm_jituan_changkou_predict_result_dt/bs="laifenqi",ts="$ts",pt="$this_dt"  -fd ',' -h 'true';
tunnel upload /home/risk/home/changkou_project/laifenqi_ck_estimate_no_test/data/output/parameters_cycle_fenqi_mean_overdue_rate.csv   bidata_market.dm_jituan_changkou_predict_overdue_result_dt/bs="laifenqi",ts="$ts",pt="$this_dt"  -fd ',' -h 'true';
