#!/bin/bash
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Djline.terminal=jline.UnsupportedTerminal"
# mapred.min.split.size=136870912
function run_userinfo() {
    hive_sql="
    set mapred.min.split.size=36870912;
    set mapred.max.split.size=36870912;
    set mapred.min.split.size.per.node=36870912;
    set mapred.min.split.size.per.rack=36870912;   
    set hive.exec.reducers.bytes.per.reducer=36870912; 
    insert overwrite table bi_dwa.t_dwa_acct_$1_userinfo 
    select a.userid,
        account,
        firstlogin_time,
        firstlogin_groupname,
        firstlogin_ip,
        lastlogin_time,
        lastlogin_groupname,
        lastlogin_ip,
        firstpay_time,
        lastpay_time,
        substr(current_timestamp, 1, 19) updatetime
    from (select userid,
                account,
                firstlogin_time,
                firstlogin_groupname,
                firstlogin_ip,
                lastlogin_time,
                lastlogin_groupname,
                lastlogin_ip
            from (select userid,
                        account,
                        first_value(today_firstlogin_time) over(partition by userid order by today_firstlogin_time rows BETWEEN unbounded preceding AND unbounded following) firstlogin_time,
                        first_value(groupname) over(partition by userid order by today_firstlogin_time rows BETWEEN unbounded preceding AND unbounded following) firstlogin_groupname,
                        first_value(today_firstlogin_ip) over(partition by userid order by today_firstlogin_time rows BETWEEN unbounded preceding AND unbounded following) firstlogin_ip,
                        last_value(today_lastlogin_time) over(partition by userid order by today_lastlogin_time rows BETWEEN unbounded preceding AND unbounded following) lastlogin_time,
                        last_value(groupname) over(partition by userid order by today_lastlogin_time rows BETWEEN unbounded preceding AND unbounded following) lastlogin_groupname,
                        last_value(today_lastlogin_ip) over(partition by userid order by today_lastlogin_time rows BETWEEN unbounded preceding AND unbounded following) lastlogin_ip
                    from bi_dwb.t_dwb_act_$1_user_active_d t) t
            group by userid,
                    account,
                    firstlogin_time,
                    firstlogin_groupname,
                    firstlogin_ip,
                    lastlogin_time,
                    lastlogin_groupname,
                    lastlogin_ip) a
    left join (select userid,
                        min(today_firstpay_time) firstpay_time,
                        max(today_lastpay_time) lastpay_time
                from bi_dwb.t_dwb_econ_$1_user_pay_d
                group by userid) c
        on a.userid = c.userid
    where a.userid is not null
    and a.firstlogin_time is not null;
    "
    hive -v -i /var/lib/impala/test/dongtai.conf -e "${hive_sql}"
    if [ $? -eq 0 ];then
        echo "[`date +"%Y-%m-%d %H:%M:%S"` - INFO] load dwb run_userinfo SUCCESS !"
    else
        echo "[`date +"%Y-%m-%d %H:%M:%S"` - ERROR] load dwb run_userinfo FAILED !"
    fi
}

while getopts "g:s:e:" opt
do
    case $opt in
        g)
            echo "GameID: $OPTARG"
            gameid=$OPTARG
        ;;
        ?)
            echo "Unknow input parameters"
            echo "-g gameid -s startYear -e endYear"
            exit 1
        ;;
    esac
done

gameid_long=`echo $gameid | awk '{printf("%06d\n",$0)}'`
echo "run_userinfo ${gameid_long} ${start_year} ${end_year}"
run_userinfo ${gameid_long} ${start_year} ${end_year}
