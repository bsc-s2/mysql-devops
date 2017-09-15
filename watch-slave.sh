#!/bin/sh

port=$1
mysql_cmd="./bin/mysql -s --socket=/var/run/mysqld/mysqld-${port}.sock -e "

cd /usr/local/mysql-5.7.13

cmds=''
# show only field that is not empty, and gtid starts from next line
cmds="$cmds $mysql_cmd"' "show slave status\G" | grep "Slave_IO_State\|Master_Host\|Slave_IO_Running\|Slave_SQL_Running\|Last_Errno\|Seconds_Behind_Master\|Last_IO_Errno\|Last_SQL_Errno\|Master_Server_Id\|Master_UUID\|Slave_SQL_Running_State\|Channel_Name"; '
cmds="$cmds $mysql_cmd"' "select @@read_only, @@server_uuid, @@server_id\G"; '
cmds="$cmds $mysql_cmd"' "show master status\G"; '
cmds="$cmds $mysql_cmd"' "show processlist"; '

if [ ".$2" = ".-" ]; then
    eval "$cmds"
else
    watch -d -n1 "$cmds"
fi
