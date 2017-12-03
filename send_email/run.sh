today=$(date -d"-1 day" +"%Y-%m-%d")
yesterday=$(date -d"-2 day" +"%Y-%m-%d")
sed -e 's/\$yesterday/'${yesterday}'/' -e 's/\$today/'${today}'/'  download_this_dt.sql > download.sql
./../qudian_ck_estimate/odpscmd/bin/odpscmd  -f download.sql
python CheckDiffSend.py  config_file
