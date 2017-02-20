#!/usr/bin/env python
# coding: utf-8

import logging
import sys
import time

import MySQLdb

import mysqlbackup

logger = logging.getLogger(__name__)


if __name__ == "__main__":

    mysqlbackup.init_logger()

    conf = {
        'mysql_base':  '/usr/local/mysql-5.7.13',

        'host':        '127.0.0.1',
        'data_base':   '/data1',
        'backup_base': '/data1/backup',
        'port':        3309,
        'instance_id': '1',

        # optional, for test only
        'sql_test_insert': ('insert into `xp2`.`heartbeat`'
                            ' (`key`, `value`, `ts`)'
                            ' values'
                            ' ("test-backup", "{v}", "{v}")'),

        'sql_test_get_2': 'select `value` from `xp2`.`heartbeat` order by `_id` desc limit 2',

        # optional, for backup to remote storage
        's3_host': '127.0.0.1',
        's3_bucket': 'mysql-backup',
        's3_access_key': 's2kjq4d8ml56brfnxagz',
        's3_secret_key': 'UmF2/KNAsZbPB4RnCEUE47vFsui5zG3RxhXdWxtV',
    }

    mb = mysqlbackup.MysqlBackup(conf)

    if sys.argv[1] == 'b':
        try:
            mb.backup()

        except mysqlbackup.MysqlBackupError as e:
            print e[0]
            print e[1]
            for l in e[2].split('\\n'):
                print l
            for l in e[3].split('\\n'):
                print l

    elif sys.argv[1] == 'r':

        try:
            mb.restore()
        except mysqlbackup.MysqlBackupError as e:
            print e[0]
            print e[1]
            for l in e[2].split('\\n'):
                print l
            for l in e[3].split('\\n'):
                print l

    elif sys.argv[1] == 't':

        # test connectivity
        mb.mysql_get_last_2_test_value()

        mb.backup_data()

        # Writes another row after backing up data, and before backing up binlog.
        # Emulates write that happens when backing up is running

        value_after_data = str(time.time())
        mb.mysql_insert_test_value(value_after_data)
        logger.info('inserted: {v} after data backup'.format(
            v=value_after_data))

        mb.backup_binlog()
        mb.calc_checksum()
        mb.upload_backup()

        # write after all backup, test restoring from remote binlog

        value_after_binlog = str(time.time())
        mb.mysql_insert_test_value(value_after_binlog)
        logger.info('inserted: {v} after binlog backup'.format(
            v=value_after_binlog))

        logger.info('wait for binlog to sync')
        time.sleep(1)

        logger.info('kill mysql')
        mysqlbackup._shell_run(mb.render('kill $(cat {mysql_pid_path})'))

        while True:
            try:
                mb.list_binlog_fns()
                time.sleep(0.1)
                continue
            except MySQLdb.OperationalError as e:
                break

        logger.info('remove mysql data dir')
        mysqlbackup._shell_run(
            mb.render('rm -rf {mysql_data_dir} {backup_data_dir} {backup_binlog_dir}'))

        mb.restore()

        # check data
        proc = mb.start_tmp_mysql()

        rst = mb.mysql_get_last_2_test_value()

        assert rst[0].values()[
            0] == value_after_binlog, 'write after binlog backup should be seen: ' + str(value_after_binlog)
        assert rst[1].values()[
            0] == value_after_data, 'write after data backup should be seen: ' + str(value_after_data)

        mb.stop_tmp_mysql(proc)

    else:
        raise ValueError('invalid command: ' + sys.argv[1])
