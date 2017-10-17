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
./do-all.sh 'show slave status\G' | grep 'Port\|Executed\|^[^ ]'
```
