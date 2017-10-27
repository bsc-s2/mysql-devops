<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
#   Table of Content

- [Script](#script)
  - [do-all.sh](#do-allsh)
  - [watch-all.sh](#watch-allsh)
  - [watch-slave.sh](#watch-slavesh)
- [Snippet](#snippet)
  - [Check `mrr` optimization option on every mysql instance:](#check-mrr-optimization-option-on-every-mysql-instance)
  - [Check replication status on every mysql instance:](#check-replication-status-on-every-mysql-instance)
  - [Check replication progress on every mysql instance:](#check-replication-progress-on-every-mysql-instance)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Script

## do-all.sh

List all mysql local socket file in `/var/run/mysqld/*.sock` and do sql queries
on each socket.

**Synopsis**

```
./do-all.sh 'show master logs\G'
```

## watch-all.sh

Watch replication status on every mysql instance.
It display replication status one by one.

**options**:

`-i`: interactive mode, press `<space>` to switch to next port, do not
automatically refresh.

Its output is a short summary of `show slave status`, and some other info.

```
               Slave_IO_State: Waiting for master to send event
                  Master_Host: 192.168.8.20
             Slave_IO_Running: Yes
            Slave_SQL_Running: Yes
                   Last_Errno: 0
                   Last_Error:
        Seconds_Behind_Master: 0
                Last_IO_Errno: 0
                Last_IO_Error:
               Last_SQL_Errno: 0
               Last_SQL_Error:
             Master_Server_Id: 13902
                  Master_UUID: 716a54d5-a050-11e6-aa87-a0369fabbdf0
      Slave_SQL_Running_State: Slave has read all relay log; waiting for more updates
      Last_IO_Error_Timestamp:
     Last_SQL_Error_Timestamp:
                 Channel_Name: master-1
               Slave_IO_State: Waiting for master to send event
                  Master_Host: 10.104.0.26
             Slave_IO_Running: Yes
            Slave_SQL_Running: Yes
                   Last_Errno: 0
                   Last_Error:
        Seconds_Behind_Master: 0
                Last_IO_Errno: 0
                Last_IO_Error:
               Last_SQL_Errno: 0
               Last_SQL_Error:
             Master_Server_Id: 33902
                  Master_UUID: b18e8590-ab0a-11e6-912a-a0369fb4eb08
      Slave_SQL_Running_State: Slave has read all relay log; waiting for more updates
      Last_IO_Error_Timestamp:
     Last_SQL_Error_Timestamp:
                 Channel_Name: master-3
*************************** 1. row ***************************
  @@read_only: 0
@@server_uuid: 29dcb03b-a019-11e6-b613-a0369fabbda4
  @@server_id: 23902
*************************** 1. row ***************************
             File: mysql-bin.000016
         Position: 523036718
     Binlog_Do_DB:
 Binlog_Ignore_DB:
Executed_Gtid_Set: 14f4d1fb-9f77-11e6-890d-a0369fabbdb8:1-28647,
29dcb03b-a019-11e6-b613-a0369fabbda4:1-24032,
716a54d5-a050-11e6-aa87-a0369fabbdf0:1-4790456,
b18e8590-ab0a-11e6-912a-a0369fb4eb08:1-16460318,
e3091dde-6b12-11e6-ba9c-a0369fabbdf0:1-1559103
Id      User    Host    db      Command Time    State   Info
468511  system user             NULL    Connect 2327315 Waiting for master to send event        NULL
468512  system user             NULL    Connect 2       Slave has read all relay log; waiting for more updates  NULL
468513  system user             NULL    Connect 2327315 Waiting for master to send event        NULL
468514  system user             NULL    Connect 2       Slave has read all relay log; waiting for more updates  NULL
468515  replicator      192.168.8.20:45972      NULL    Binlog Dump GTID        2327315 Master has sent all binlog to slave; waiting for more updates   NULL
673280  replicator      10.104.0.26:50641       NULL    Binlog Dump GTID        2150387 Master has sent all binlog to slave; waiting for more updates   NULL
3145080 monitor 192.168.8.12:38675      NULL    Sleep   2               NULL
3145087 root    localhost       NULL    Query   0       starting        show processlist
```

## watch-slave.sh

Similar to `watch-all.sh` but it display replication status for one port.

```
./watch-slave.sh <port>
```

# Snippet

## Check `mrr` optimization option on every mysql instance:

```
./do-all.sh 'select @@optimizer_switch' | grep -o "mrr=[^,]*"
```

## Check replication status on every mysql instance:

```
./do-all.sh 'show slave status\G' | grep 'Seconds\|Error\|State\|Master_Server_Id'
```

## Check replication progress on every mysql instance:

```
./do-all.sh 'select @@port; show master status\G' | grep -v "Pos\|File\|Binlog_Do_DB\|Binlog_Ig\|^\*\*" | awk '/:/{print "    "$NF} !/:/{print $NF}'

# 5202
#     133ce91b-46bf-11e7-a847-a0369fb4eb08:1-2,
#     1888d423-46bf-11e7-821e-a0369fabbdf0:1-11
# 5302
#     1745d7fc-46bf-11e7-86b9-a0369fb4eb08:1-2,
#     1b13a2f0-46bf-11e7-a894-a0369fabbdf0:1-11
```

## Display replication diff from other replication source.

```sh
python2 mysqlops.py --human --conf-base /s2/mysql/backup_conf --cmd replication_diff

#   10.104.0.18:3402-[3]:  IDontHave:          4: {'e90a6f39-a80c-11e6-9cb8-a0369fb4eb00': [[568116547, 568116550]]}
#  192.168.8.19:3403-[1]:  IDontHave:          1: {'f4ebb415-a801-11e6-9079-a0369fb4eb00': [[2453681515, 2453681515]]}
#   10.104.0.18:3403-[3]:  IDontHave:         70: {'f4ebb415-a801-11e6-9079-a0369fb4eb00': [[2453681515, 2453681584]]}
#   10.104.0.18:3404-[3]:  IDontHave:          5: {'1f975092-a813-11e6-a042-a0369fb4eb00': [[567346128, 567346132]]}
#   10.104.0.18:3405-[3]:  IDontHave:          5: {'a7d3758d-a884-11e6-b316-a0369fb4eb00': [[1517445278, 1517445282]]}
#  192.168.8.19:3501-[1]:  IDontHave:          1: {'1c685bd4-b80b-11e7-b686-a0369fb4eb00': [[32102044, 32102044]]}
#   10.104.0.18:3501-[3]:  IDontHave:         44: {'1c685bd4-b80b-11e7-b686-a0369fb4eb00': [[32102044, 32102087]]}
#   10.104.0.18:3503-[3]:  IDontHave:          3: {'44722efe-a889-11e6-98df-a0369fb4eb00': [[1342034374, 1342034376]]}
#   10.104.0.18:3505-[3]:  IDontHave:         10: {'3e2a6047-a891-11e6-9fa0-a0369fb4eb00': [[341717052, 341717061]]}
#   10.104.0.18:3508-[3]:  IDontHave:          4: {'9dd98013-a885-11e6-9c9d-a0369fb4eb00': [[3666711890, 3666711893]]}

```

Other options

```
# concurrently 8 jobs, makes it faster
python2 mysqlops.py --jobs 8 --human --conf-base /s2/mysql/backup_conf --cmd replication_diff

# output json instead of human readable text
python2 mysqlops.py --conf-base /s2/mysql/backup_conf --cmd replication_diff

# check only one port
python2 mysqlops.py --conf-base /s2/mysql/backup_conf --cmd replication_diff --port 3401
```

<!--

mysql multi master

show master status;
show slave status;

SELECT * FROM performance_schema.replication_connection_status;

select * from mysql.gtid_executed;

close and reopen binary logs:
flush BINARY logs;

reset slave;


change master to master_host="10.173.24.184",
 master_port=3309,
 master_user="replicator",
 master_password="123qwe",
 MASTER_AUTO_POSITION=1
 for channel 'master-2';

change master to master_host="10.51.118.223",
 master_port=3309,
 master_user="replicator",
 master_password="123qwe",
 MASTER_AUTO_POSITION=1
 for channel 'master-1';

--read-from-remote-master=BINLOG-DUMP-GTIDS

echo 'show binary logs' | /usr/local/mysql-5.7.13/bin/mysql --skip-column-names --socket=/tmp/mysql-3309.sock

/usr/local/mysql-5.7.13/bin/mysqlbinlog -v --host=10.173.24.184 --port=3309 --user=replicator --password=123qwe --read-from-remote-master="BINLOG-DUMP-GTIDS" --exclude-gtids=‘1fcc256b-84b0-11e6-acaf-00163e03bb03:1-326’  mysql-bin.000001 mysql-bin.000002 mysql-bin.000003

/usr/local/mysql-5.7.13/bin/mysqlbinlog --raw --read-from-remote-server --stop-never --host 10.173.24.18  --port 3309 -u replicator -p123qwe mysql-bin.000001 mysql-bin.000002 mysql-bin.000003

!cd ~/mysql-devops/; git fetch ; git merge origin/master

mysql should always starts as readonly, to avoid data write before all binlog synced from remote master/slave.
Or there is chance mysql write conflict primary id to local.

/usr/local/mysql-5.7.13/bin/mysqlbinlog -v --host=10.173.24.184 --port=3309 --user=replicator --password=123qwe --read-from-remote-master="BINLOG-DUMP-GTIDS" --exclude-gtids='1616352c-8226-11e6-bbab-00163e03bb03:1,  229421a1-8224-11e6-ac58-00163e03bb13:1-2,  4b0918bc-8235-11e6-87fd-00163e03bb03:1,  5fca6d6a-823f-11e6-a40b-00163e03bb03:1-2, 22e3067c-8224-11e6-b3ef-00163e03bb03:1-299'  mysql-bin.000001 mysql-bin.000002 mysql-bin.000003 | /usr/local/mysql-5.7.13/bin/mysql --socket=/tmp/mysql-3309.sock

-->
