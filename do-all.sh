#!/bin/sh


cd /usr/local/mysql-5.7.13

for fn in $(ls /var/run/mysqld); do
    port=${fn%.pid}
    port=${port#*-}

    mysql_cmd="./bin/mysql -s --socket=/var/run/mysqld/mysqld-${port}.sock -e "

    $mysql_cmd "$@"

done

