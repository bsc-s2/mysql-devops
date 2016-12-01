# mysqlbackup.py

`mysqlbackup.py` is a wrapper of `xtrabackup`.
It provides with a full backup/restore mechanism for both data and binlog.

##  Workflow

### Backup

To backup, it does following things:

-   Backup data from an active mysql instance.

-   Backup `my.cnf`(It does not backup `auto.cnf`, in order to make it
    possible for a restored instance to accept binlog events generated by
    itself from remote).

-   Backup binlog from an active mysql instance.
    Binlog may contains more events than it is in the data backup.

-   Pack backup dir into a `tar.gz` package.

-   Calculate checksum of the `tar.gz`.

-   Upload it to a aws-s3 compatible storage service, if s3 account is
    provided in conf.

-   Removes backup dir but leaves `tar.gz` where it is.

### Restore

To restore, it does following things:

-   Download backup `tar.gz` from aws-s3 compatible storage service,
    if s3 account is provided.

-   Check file checksum(sha1 is prefered) against the checksum recorded in
    meta of object in aws s3.

-   Unpack `tar.gz`.

-   Copies data back to where specified by the `my.cnf` from the backup.

-   Starts mysql instance in **read-only** mode and applies binlog
    events from backup to this instance.

-   Restarts mysql with a **temporary** `server-id`, and starts binlog
    replication to let it receive binlog events those created by itself but
    not in backup, from other instances.

-   Shuts it down and leave it there as a **ready-to-use** mysql data
    directory.

## How it works

`innobackupex` is a great tool for backing up data from an active mysql
instance.
But `innobackupex` does not backup/restore binlog.
Thus we need to backup/restore binlog manually and keep binlog and data
consistent.

Since the backup may be run on an active mysql instance, after data was
backed up, there could be more events in binlog than there are in backup data.

Thus every time restoring, we need to apply binlog events those are in binlog
backup but not in data backup to the restored mysql instance.

It also backup `my.cnf` but it does not backup `auto.cnf`(the file in which
the uuid of the mysql instance is defined).

<!-- TODO third party -->

Then a third party script is responsible to setup new replication after backup
restored.
Since replication topology might change after backup is made.


With new replication setup, start mysql instance with temporary **server-id**
and catchup binlog from all reachable source, until this instance is less than
1 second behind source instance.

Then shut down mysql and leave it as a ready-to-use instance.

<!-- TODO make it a function to setup replication in mysqlbackup.py -->
<!-- TODO continue from here -->

After restoring data and binlog, we also need to setup binlog replication to
sync binlog events from other mysql instances those are in a same port group,
to recovery events those are written after this backup created.

This step is done by:

-   resets relay log files and index, since relay logs are not backed up.

    ```
    flush local relay logs;
    ```

-   resets slave status:

    ```
    reset slave
    ```

-   restarts replication.

    ```
    start slave for channel "**"
    ```

    Mysql instance must not be stop and restart between the last two steps.
    Or replication information gets lost.

    And `start slave` must be done on every channel explicitly.
    Just `start slave` does not start replication, although it should,
    according to mysql ref manual.

But, because mysql just ignores binlog those are created by itself, it is
impossible to restore binlog events from other instance to recover events
those are created after backup and before crash.

Thus every time restoring a mysql instance, it creates a new uuid(by starting
mysql without `auto.cnf`, mysql will create a new one),
and start mysql with a temporary `server-id`.

>   A temporary `server-id` must not be used to generate any binlog events,
>   and not equal to any existent `server-id`.

This way the new instance will accept all binlog event(created by itself or
other instances) those it does not have.

**We can not use `--replicate-same-server-id` to force mysql to apply binlog
either.**

Because `--replicate-same-server-id` can not be used together with
`--log-slave-updates`, which is required in our scenario(multiple master with
multiple way replication), to let every mysql instance keeps full history
binlog from several instances.

After syncing enough binlog, stop this temporary mysql instance.

For the next time it starts, with the original `server-id` it will work as
normal.

A diagram illustrates how backup and restore works as follow.
There are two instance `instace 1` and `instance 2`.

`instance 1` is the one that is backed up.
`instance 2` is a remote replication with the same data in it.

```
instance 1                            instance 2
uuid = aaa:                           uuid = ccc:

backup data and binlog,               active instance with
without latest gtid:5                 latest gtid:5
bkp:    data:     aaa:1-3             ccc:    data:     aaa:1-5
        binlog:   aaa:1-4                     binlog:   aaa:1-5

restored with new uuid = bbb:
bbb:    data:     aaa:1-3

apply rows in binlog but not in
backup-data:
bbb:    data:     aaa:1-4

copy back binlog, now it is
consistent but behind ccc:
bbb:    data:     aaa:1-4
        binlog:   aaa:1-4

apply binlog from ccc:
bbb:    data:     aaa:1-5          <- ccc:    data:     aaa:1-5, bbb:1-1
        binlog:   aaa:1-5                     binlog:   aaa:1-5, bbb:1-1

new write:                            sync from bbb:
bbb:    data:     aaa:1-5, bbb:1-1 -> ccc:    data:     aaa:1-5, bbb:1-1
        binlog:   aaa:1-5, bbb:1-1            binlog:   aaa:1-5, bbb:1-1

```

## Dependency

-   xtrabackup, `2.4` or newer.

    ```
    yum install libev-devel libev perl-DBD-MySQL
    ```

    [download xtrabackup](https://www.percona.com/downloads/XtraBackup/Percona-XtraBackup-2.4.4/binary/redhat/7/x86_64/percona-xtrabackup-24-2.4.4-1.el7.x86_64.rpm)

    [xtrabackup website](https://www.percona.com/downloads/XtraBackup/LATEST/)

-   python libs:

    ```
    pip install MySQLdb
    pip install boto3
    ```

## Usage

`mysqlbackup.py` provides with a sample usage snippet in its main block at the
bottom:

```python
conf = {
    # required:
    'mysql_base': '/usr/local/mysql-5.7.13',

    # required, the host name or ip, used only for naming the backup tgz file.
    'host': '127.0.0.1',

    # required, base dir of running mysql instance data-dir
    'data_base':   '/data1',

    # required, base dir of backup dir.
    'backup_base': '/data1/backup',

    # required, port of the mysql instance to be backed up.
    'port': 3309,

    # required, the instance id in our mysql replication group.
    'instance_id': '1',

    # optional, for data completeness test only
    'sql_test_insert': ('insert into `xp2`.`heartbeat`'
                        ' (`key`, `value`, `ts`)'
                        ' values'
                        ' ("test-backup", "{v}", "{v}")'),
    'sql_test_get_2': 'select `value` from `xp2`.`heartbeat` order by `_id` desc limit 2',

    # optional, for backup to s3 storage.
    's3_host':          's2.i.qingcdn.com',
    's3_bucket':        'mysql-backup',
    's3_access_key':    's2kjq4d8ml56brfnxagz',
    's3_secret_key':    'UmF2/KNAsZbPB4RnCEUE47vFsui5zG3RxhXdWxtV',
}

mb = MysqlBackup(conf)

# creates tgz {backup_base}/mysql-{port}.tgz
# and store backup tgz to s3 if s3_bucket is provided.
mb.backup()

# restore mysql {backup_base}/mysql-{port}.tgz
# or restore from s3 if s3_bucket is provided.
mb.restore()
```
