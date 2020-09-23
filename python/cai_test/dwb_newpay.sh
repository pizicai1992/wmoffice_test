#!/bin/bash
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Djline.terminal=jline.UnsupportedTerminal"

function run_newpay() {
    hive_sql="
    set mapred.min.split.size=36870912;
    set mapred.max.split.size=36870912;
    set mapred.min.split.size.per.node=36870912;
    set mapred.min.split.size.per.rack=36870912;    
    set hive.exec.reducers.bytes.per.reducer=36870912;
    insert overwrite table bi_dwb.t_dwb_acct_$1_newpayuser_d partition
    (par_dt)
    select userid, 
            today_firstpay_time, 
            groupname,
            today_firstpay_cash,
            to_date(today_firstpay_time) par_dt
        from (select userid,
                    groupname,
                    today_firstpay_time,
                    today_firstpay_cash,
                    par_dt,
                    row_number() over(partition by userid order by today_firstpay_time) rn
                from bi_dwb.t_dwb_econ_$1_user_pay_d t
            where par_dt >= '$2-01-01' and par_dt<'$3-01-01') t
    where t.rn = 1
        and not exists (select 1
                from bi_dwb.t_dwb_acct_$1_newpayuser_d s
            where s.par_dt < '$2-01-01'
                and s.userid = t.userid);
    "
    hive -v -i /var/lib/impala/test/dongtai.conf -e "${hive_sql}"
    if [ $? -eq 0 ];then
        echo "[`date +"%Y-%m-%d %H:%M:%S"` - INFO] load dwb run_newpay SUCCESS !"
    else
        echo "[`date +"%Y-%m-%d %H:%M:%S"` - ERROR] load dwb run_newpay FAILED !"
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
    echo "run_newpay ${gameid_long} ${start_year} ${end_year}"
    run_newpay ${gameid_long} ${start_year} ${end_year}
    sleep 3
    start_year=$[start_year+1]
done