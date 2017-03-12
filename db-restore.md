# Add Mysql Slave to a Group(port)

-   add ssh key to coding.net

    ```
    ssh-keygen
    cat ~/.ssh/id_ras.pub
    ```

-   clone mysql-devops

    ```
    git clone ssh://git@git.coding.net/baishancloud/mysql-devops
    ```

-   restore mysql instance, for example restore all data dirs from
    `192.168.8.19`:

    ```
    ./restore.py "from-19"
    # ./restore.py "from-20"
    # ./restore.py "from-29"
    # see conf.py
    ```

-   update inventory config `mysql_replication.yaml` in s2-init to add new master.

    for example, change 
    ```
    mysql_replication:
      "3401":
        192.168.8.29: { id: "1" }
    ```

    to:

    ```
    mysql_replication:
      "3401":
        192.168.8.29: { id: "1" }
        192.168.8.57: { id: "2", src: "1" }
    ```

    >   For safety, the above step actually does not setup dual-master
    >   replication, but only a single way replication that lets mysql id=2 to
    >   read binlog from mysql id=1,

    and run ansible book to apply config:

    ```
    ./init.sh -i inventories/xx  -c s2 -b install-mysql.yaml
    ```

    >   ansible role `mysql` does not affect existent mysql instance.
    >   although it update `my.cnf`, but it does not restart mysql):

    It does following things:

    -   update `my.cnf` on new instance.
    -   setup replication on new instance.
    -   administrator the should then confirm that data on this slave is close
        enough to its source: the instance with id=1.

-   switch the config in etcd to let core to use instance id=2 for group `3401`:

    ```
    ./init.sh -i inventories/xx -c s2 -b mysql-change-port-master.yaml -x "mysql_port_to_change=3401 mysql_master_id=2"
    ```


