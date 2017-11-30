#!/bin/sh

port=$1
mysql_cmd="./bin/mysql -s --socket=/var/run/mysqld/mysqld-${port}.sock -e "

cd /usr/local/mysql-5.7.13


# awk script to display multiple master horizontally
ak='{fields[$1]=fields[$1] ":" $2} END {for (k in fields) { print k ":" fields[k]; } }'

cmds=''
# show only field that is not empty, and gtid starts from next line
cmds="$cmds $mysql_cmd"' "show slave status\G" | grep "Slave_IO_State\|Master_Host\|Slave_IO_Running\|Slave_SQL_Running\|Last_Error\|Seconds_Behind_Master\|Last_IO_Error\|Last_SQL_Error\|Master_Server_Id\|Master_UUID\|Slave_SQL_Running_State\|Channel_Name" | awk -F: '"'"$ak"' | sort | column -t -s:;"
cmds="$cmds $mysql_cmd"' "select @@read_only, @@server_uuid, @@server_id\G"; '
cmds="$cmds $mysql_cmd"' "show master status\G"; '
cmds="$cmds $mysql_cmd"' "show processlist" | head -n10; '

if [ ".$2" = ".-" ]; then
    eval "$cmds"
else
    watch -d -n1 "$cmds"
fi
