#!/bin/bash

function run_userpay() {
    hive_sql="
    set mapred.min.split.size=136870912;
    set mapred.max.split.size=136870912;
    set mapred.min.split.size.per.node=136870912;
    set mapred.min.split.size.per.rack=136870912;    
    insert overwrite table bi_dwb.t_dwb_econ_$1_user_pay_d partition (par_dt)
    select userid,
        groupname,
        today_first_pay_time,
        today_last_pay_time,
        today_first_pay_cash,
        today_last_pay_cash,
        today_max_pay_cash,
        today_min_pay_cash,
        today_pay_total_cash,
        today_pay_total_counts,
        par_dt
    from (select userid,
                groupname,
                par_dt,
                first_value(logtime) over(partition by par_dt,userid,groupname order by logtime rows between unbounded preceding and unbounded following) today_first_pay_time,
                last_value(logtime) over(partition by par_dt,userid,groupname order by logtime rows between unbounded preceding and unbounded following) today_last_pay_time,
                first_value(cash) over(partition by par_dt,userid,groupname order by logtime rows between unbounded preceding and unbounded following) today_first_pay_cash,
                last_value(cash) over(partition by par_dt,userid,groupname order by logtime rows between unbounded preceding and unbounded following) today_last_pay_cash,
                max(cash) over(partition by par_dt,userid,groupname rows between unbounded preceding and unbounded following) today_max_pay_cash,
                min(cash) over(partition by par_dt,userid,groupname rows between unbounded preceding and unbounded following) today_min_pay_cash,
                sum(cash) over(partition by par_dt,userid,groupname rows between unbounded preceding and unbounded following) today_pay_total_cash,
                count(1) over(partition by par_dt,userid,groupname rows between unbounded preceding and unbounded following) today_pay_total_counts,
                row_number() over(partition by par_dt,userid,groupname order by logtime rows between unbounded preceding and unbounded following) rn
            from bi_dwd.t_dwd_econ_$1_addcash
            where par_dt >= '$2-01-01' and par_dt<'$3-01-01'
            ) t
    where rn = 1;
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
    echo "run_userpay ${gameid_long} ${start_year} ${end_year}"
    run_userpay ${gameid_long} ${start_year} ${end_year}
    sleep 3
    start_year=$[start_year+1]
done