#!/bin/sh

while ``; do

    found=0

    for fn in $(ls /var/run/mysqld/*.sock); do
        port=${fn%.pid}
        port=${port#*-}
        clear
        ./watch-slave.sh $port -
        sleep 0.3

        found=1
    done

    echo "--"
    if [ "$found" = "0" ]; then
        sleep 1
    fi
done
