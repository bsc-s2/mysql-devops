#!/bin/sh

usage()
{
    echo Print average number of transactions in a binlog-group-commit
}


port="$1"

datadir="/data1/mysql-$port"
n_binlog=10000
mysql_dir=/usr/local/mysql-5.7.13

cd $datadir
last_binlog_fn=$(cat mysql-bin.index | tail -n1)

# Same `last_committed` indicates a same binlog-group-commit
# The number of different `sequence_number` with a same `last_committed` is NO.
# transactions in a binlog-group-commit.
#
#171130 12:26:22 server id 33501  end_log_pos 539 CRC32 0x1f67c314      GTID    last_committed=0        sequence_number=1


n_group_commit=$($mysql_dir/bin/mysqlbinlog $last_binlog_fn \
    | grep sequence_number \
    | head -n $n_binlog \
    | awk '{print $11}' \
    | uniq -c \
    | wc)

let avg=n_binlog/n_group_commit
print $avg
