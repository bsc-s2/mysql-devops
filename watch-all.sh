#!/bin/sh

interactive=0
if [ "$1" = "-i" ]; then
    interactive=1
    shift
fi

while ``; do

    found=0

    for fn in $(ls /var/run/mysqld/*.sock); do
        port=${fn%.sock}
        port=${port#*-}
        if [ "$interactive" = "1" ]; then
            while ``; do
                clear
                ./watch-slave.sh $port -
                echo "<< press space to continue, press r to refresh >>.........."
                read -n 1 c
                if [ "$c" = "r" ]; then
                    :
                else
                    break
                fi
            done
        else
            clear
            ./watch-slave.sh $port -
            sleep 0.3
        fi

        found=1
    done

    echo "--"
    if [ "$found" = "0" ]; then
        if [ "$interactive" = "1" ]; then
            echo "<<press space to continue>>.........."
            read -n 1 c
        else
            sleep 1
        fi
    fi
done
