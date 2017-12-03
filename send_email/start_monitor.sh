#/bin/sh

this_dt=$(date -d"-1 days" +"%Y-%m-%d")
that_dt=$(date -d"-2 days" +"%Y-%m-%d")


down_data()
{
	sed "s/$that_dt/$this_dt/"  download.sql  > tmp_tmp    && mv  tmp_tmp   download.sql
	/home/risk/home/luobaowei/odpscmd/bin/odpscmd  -f download.sql
}



send_email()
{
	python CheckDiffSend.py  config_file
}

main()
{
	down_data
	send_email
}

main

