#!/bin/bash
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Djline.terminal=jline.UnsupportedTerminal"
# mapred.min.split.size=136870912
function run_newuser_group() {
    hive_sql="
    set mapred.min.split.size=68435456;
    set mapred.max.split.size=68435456;
    set mapred.min.split.size.per.node=68435456;
    set mapred.min.split.size.per.rack=68435456;    
    insert overwrite table bi_dwb.t_dwb_acct_$1_newuser_group_d partition
    (par_dt)
    select userid,
            account,
            groupname,
            today_firstlogin_time,
            today_firstlogin_ip,
            to_date(today_firstlogin_time) par_dt
        from (select userid,
                    account,
                    groupname,
                    today_firstlogin_time,
                    today_firstlogin_ip,
                    row_number() over(partition by userid,groupname order by today_firstlogin_time) rn
                from bi_dwb.t_dwb_act_$1_user_active_d t
            where par_dt >= '$2-01-01' and par_dt<'$3-01-01'
                and today_login_counts > 0) t
    where t.rn = 1
        and not exists (select 1
                from bi_dwb.t_dwb_acct_$1_newuser_group_d s
            where s.par_dt < '$2-01-01'
                and t.userid = s.userid and t.groupname = s.groupname);
    "
    hive -v -i /var/lib/impala/test/dongtai.conf -e "${hive_sql}"
    if [ $? -eq 0 ];then
        echo "[`date +"%Y-%m-%d %H:%M:%S"` - INFO] load dwb run_newuser_group SUCCESS !"
    else
        echo "[`date +"%Y-%m-%d %H:%M:%S"` - ERROR] load dwb run_newuser_group FAILED !"
    fi
}

while getopts "g:s:e:" opt
do
    case $opt in
        g)
            echo "GameID: $OPTARG"
            gameid=$OPTARG
        ;;
        s)
            echo "Start year: $OPTARG"
            start_year=$OPTARG
        ;;
        e)
            echo "End year: $OPTARG"
            end_year=$OPTARG
        ;;
        ?)
            echo "Unknow input parameters"
            echo "-g gameid -s startYear -e endYear"
            exit 1
        ;;
    esac
done

gameid_long=`echo $gameid | awk '{printf("%06d\n",$0)}'`
stop_year=$[end_year+1]
while [[ $start_year < $stop_year ]]; do
    echo "[`date +"%Y-%m-%d %H:%M:%S"` - INFO]****** data year : ${start_year} ******"
    end_year=$[start_year+1]
    echo "run_newuser_group ${gameid_long} ${start_year} ${end_year}"
    run_newuser_group ${gameid_long} ${start_year} ${end_year}
    sleep 3
    start_year=$[start_year+1]
done