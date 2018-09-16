#!/bin/sh

port=$1
cmd="/usr/local/mysql-5.7.13/bin/mysql -S /var/run/mysqld/mysqld-$port.sock"

get_binlog_error()
{
    # 1032: delete-row fails
    $cmd -Ns -e 'SELECT LAST_SEEN_TRANSACTION FROM performance_schema.replication_applier_status_by_worker WHERE LAST_ERROR_NUMBER = 1032 limit 1;'
}

while ``; do
    for i in $(seq 20); do
        gtid="$(get_binlog_error)"
        if [ -n "$gtid" ]; then
            echo found error gtid: $gtid
            break
        else
            sleep 0.1
        fi
    done

    if [ -z "$gtid" ];  then
        # replication_applier_status_by_worker sometimes does not report error occured while applying binlog
        # re-setup replication fixes it.

        echo "re-setup replication to find more error"

        ./mysqlops.py --cmd setup_replication --ports $port
        sleep 3
        gtid="$(get_binlog_error)"
        if [ -z "$gtid" ]; then
            echo "no error found"
            exit 0
        else
            continue
        fi
    fi
{

cat <<END
STOP SLAVE;
SET GTID_NEXT='$gtid';
BEGIN;
COMMIT;
SET GTID_NEXT='AUTOMATIC';
START SLAVE;
END
} | $cmd

done
