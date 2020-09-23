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
    insert overwrite table bi_dwa.t_dwa_acct_$1_userinfo_abn
    select userid, account, par_dt, ip
    from (select t.*,
                row_number() over(partition by userid order by par_dt) rn
            from (select par_dt,
                        userid,
                        ip,
                        account,
                        count(userid) over(partition by par_dt, ip) user_cnt
                    from (select par_dt, userid, ip, account
                            from (select par_dt,
                                        userid,
                                        ip,
                                        last_value(account) over(partition by userid order by logtime rows BETWEEN unbounded preceding AND unbounded following) account
                                    from bi_dwd.t_dwd_act_$1_login
                                    where ip is not null
                                    and length(ip) > 6) t
                            group by par_dt, userid, ip, account) t) t
            where user_cnt >= 30) t
    where rn = 1;
    "
    hive -v -i /var/lib/impala/test/dongtai.conf -e "${hive_sql}"
    if [ $? -eq 0 ]; then
        echo "[$(date +"%Y-%m-%d %H:%M:%S") - INFO] load dwb run_userinfo SUCCESS !"
    else
        echo "[$(date +"%Y-%m-%d %H:%M:%S") - ERROR] load dwb run_userinfo FAILED !"
    fi
}

while getopts "g:s:e:" opt; do
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

gameid_long=$(echo $gameid | awk '{printf("%06d\n",$0)}')
echo "run_userinfo ${gameid_long} ${start_year} ${end_year}"
run_userinfo ${gameid_long} ${start_year} ${end_year}
