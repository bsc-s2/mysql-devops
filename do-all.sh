#!/bin/sh


cd /usr/local/mysql-5.7.13

for fn in $(ls /var/run/mysqld/*.sock); do
    port=${fn%.sock}
    port=${port#*-}

    mysql_cmd="./bin/mysql -s --socket=/var/run/mysqld/mysqld-${port}.sock -e "

    echo "\033[31;40;1m #port: $port \033[0m"
    $mysql_cmd "$@"

done

