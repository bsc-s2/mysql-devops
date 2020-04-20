#!/bin/bash

# Version:0.1
# File Name: check-topology.sh
# Created Time: 一  4/20 16:54:37 2020
# Author: xianbao.wang
# email: breakwang0929@gmail.com

cd /usr/local/mysql-5.7.13

for fn in $(ls /var/run/mysqld/*.sock)
do
  port=$(echo $fn | awk -F'-' '{print $2}' | awk -F'.' '{print $1}')
  echo "port: ${port}"
  mysql_cmd="./bin/mysql -s --socket=/var/run/mysqld/mysqld-${port}.sock -e "
  $mysql_cmd "show slave status\G" | grep  -w -E 'Master_Host|Slave_IO_Running|Slave_SQL_Running'
done
