#!/bin/bash

function run_act() {
    hive_sql="
    set mapred.min.split.size=136870912;
    set mapred.max.split.size=136870912;
    set mapred.min.split.size.per.node=136870912;
    set mapred.min.split.size.per.rack=136870912;
    set hive.exec.parallel=true;
    insert overwrite table bi_dwb.t_dwb_act_$1_user_active_d partition(par_dt)
    select nvl(a.userid,b.userid) userid,
        nvl(a.account,b.account) account,
        nvl(a.groupname,b.groupname) groupname,
        nvl(a.mintime,b.mintime) min_time,
        nvl(a.maxtime,b.maxtime) max_time,
        nvl(a.login_count,0) login_count,
        nvl(b.onlinetime,0) onlinetime,
        nvl(a.first_ip,b.first_ip) first_ip,
        nvl(a.last_ip,b.last_ip) last_ip,
        nvl(a.par_dt,b.par_dt)
    from (
    select userid,
        account,
        groupname,
        min(logtime) mintime,
        max(logtime) maxtime,
        count(1) login_count,
        first_ip,
        last_ip,
        par_dt
    from (select userid,
                last_value(regexp_replace(account, '@sso', ''), true) 
                over(partition by par_dt,userid order by logtime rows between unbounded preceding and unbounded following) account,
                groupname,
                logtime,
                first_value(ip) over(partition by par_dt,userid, groupname order by logtime 
                rows between unbounded preceding and unbounded following) first_ip,
                last_value(ip, true) over(partition by par_dt,userid, groupname order by logtime 
                rows between unbounded preceding and unbounded following) last_ip,
                par_dt
            from bi_dwd.t_dwd_act_$1_login
            where par_dt >= '$2-01-01' and par_dt<'$3-01-01') t
    group by userid, 
        account, 
        groupname, 
        first_ip, 
        last_ip,
        par_dt ) a 
    full join 
    (
    select userid,
        account,
        groupname,
        min(logtime) mintime,
        max(logtime) maxtime,
        count(1) login_count,
        sum(onlinetime) onlinetime,
        first_ip,
        last_ip,
        par_dt
    from (select userid,
                last_value(regexp_replace(account, '@sso', ''), true) 
                over(partition by par_dt,userid order by logtime rows between unbounded preceding and unbounded following) account,
                groupname,
                logtime,
                onlinetime,
                first_value(ip) over(partition by par_dt,userid, groupname order by logtime 
                rows between unbounded preceding and unbounded following) first_ip,
                last_value(ip, true) over(partition by par_dt,userid, groupname order by logtime 
                rows between unbounded preceding and unbounded following) last_ip,
                par_dt
            from bi_dwd.t_dwd_act_$1_logout
            where par_dt >= '$2-01-01' and par_dt<'$3-01-01') t
    group by userid, 
        account, 
        groupname, 
        first_ip, 
        last_ip,
        par_dt 
    ) b 
    on a.userid=b.userid and a.groupname=b.groupname and a.par_dt=b.par_dt 
    "
    hive -v -i /var/lib/impala/test/dongtai.conf -e "${hive_sql}"
    if [ $? -eq 0 ];then
        echo "[`date +"%Y-%m-%d %H:%M:%S"` - INFO] load dwb user_active_d SUCCESS !"
    else
        echo "[`date +"%Y-%m-%d %H:%M:%S"` - ERROR] load dwb user_active_d FAILED !"
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
    echo "run_act ${gameid_long} ${start_year} ${end_year}"
    run_act ${gameid_long} ${start_year} ${end_year}
    sleep 3
    start_year=$[start_year+1]
done
