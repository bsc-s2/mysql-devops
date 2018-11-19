#!/usr/bin/env python2
# coding: utf-8

# -*- coding:utf-8 -*-
'''
required:
    xtrabackup version > 2.4 (for MySQL 5.7)
'''

import argparse
import copy
import errno
import hashlib
import json
import logging
import logging.handlers
import os
import signal
import subprocess
import sys
import time

import yaml
from pykit import humannum
from pykit import logutil
from pykit import mysqlconnpool
from pykit import mysqlutil
from pykit import timeutil

import botoclient
import MySQLdb
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class MysqlBackupError(Exception):
    pass


class MysqlRestoreError(Exception):
    pass


class MysqlIsAlive(Exception):
    pass


class MysqlIsDown(Exception):
    pass


class MysqlBackup(object):

    def __init__(self, bkp_conf):

        self.bkp_conf = self.extend_backup_conf(bkp_conf)

        self.mysql_addr = {'unix_socket': self.bkp_conf['mysql_socket'],
                           'user': 'root'}

        if 'root_password' in self.bkp_conf:
            self.mysql_addr.update({
                'passwd': self.bkp_conf['root_password'],
            })

        self.mysql_conn_pool = mysqlconnpool.make(self.mysql_addr)

        self.bc = botoclient.BotoClient(self.bkp_conf['s3_host'],
                                        self.bkp_conf['s3_access_key'],
                                        self.bkp_conf['s3_secret_key'])

    def create_user(self,
                    username, password,
                    host='%',
                    privileges=None,
                    binlog=True):

        # To setup replication, a replicator user should be created on each
        # instance, before binlog can be sync-ed.
        # But after setting up binlog, the row in mysql.user of the `replicator`
        # will be sync-ed and results in conflicting user row.
        #
        # Thus we might need to disable binlog when creating replication user.
        # See: https://dev.mysql.com/doc/refman/5.7/en/group-replication-user-credentials.html

        self.info('create or change user:'
                  ' {username}@{host},'
                  ' password: {password}'
                  ' privileges: {privileges}'
                  ' binlog: {binlog}'.format(
                      username=username,
                      password=password,
                      host=host,
                      privileges=privileges,
                      binlog=binlog))

        username = MySQLdb.escape_string(username)
        password = MySQLdb.escape_string(password)
        host = MySQLdb.escape_string(host)

        # privileges is a list of tuple (<table>, <privilege>)

        alive = self.is_instance_alive()
        proc = None

        if not alive:
            proc = self.start_tmp_mysql()

        try:
            pool = mysqlconnpool.make(self.mysql_addr)

            if not binlog:
                self.mysql_pool_query(pool, 'SET SQL_LOG_BIN=0')

            rst = self.mysql_pool_query(
                pool,
                'SELECT `Host`, `User` FROM `mysql`.`user`'
                ' WHERE `Host`="{host}" AND `User`="{username}"'.format(
                    host=host,
                    username=username))

            if len(rst) == 0:
                self.mysql_pool_query(
                    pool,
                    'CREATE USER "{username}"@"{host}" IDENTIFIED BY "{password}"'.format(
                        host=host,
                        username=username,
                        password=password))
            else:
                # user exists, just set password
                self.mysql_pool_query(
                    pool,
                    'SET PASSWORD FOR "{username}"@"{host}" = "{password}"'.format(
                        host=host,
                        username=username,
                        password=password))

            for tbl, prv in privileges:

                # `prv` is a key of privilege set defined in
                # mysqlutil.privileges, or a tuple of mysql privileges.
                # Such as: "monitor", "readwrite", or just ("REPLICATION SLAVE")
                #
                # Convert it to tuple.
                if isinstance(prv, basestring):
                    prv = mysqlutil.privileges[prv]

                prv = ','.join(prv)

                self.mysql_pool_query(
                    pool,
                    'GRANT {prv} ON {tbl} TO "{username}"@"{host}"'.format(
                        host=host,
                        username=username,
                        prv=prv,
                        tbl=tbl))

            self.mysql_pool_query(pool, 'FLUSH PRIVILEGES')

            if not binlog:
                self.mysql_pool_query(pool, 'SET SQL_LOG_BIN=1')

        finally:
            if not alive:
                self.stop_tmp_mysql(proc)

    def query(self, sql):

        pool = mysqlconnpool.make(self.mysql_addr)
        return self.mysql_pool_query(pool, sql)

    def group_replication_bootstrap(self):

        pool = mysqlconnpool.make(self.mysql_addr)

        sqls = (
            "SET GLOBAL group_replication_bootstrap_group=ON",
            "START GROUP_REPLICATION",
            "SET GLOBAL group_replication_bootstrap_group=OFF",
        )

        for sql in sqls:
            self.mysql_pool_query(pool, sql)

    def group_replication_setup_channel(self):

        # replication:
        #     user:       "{{ mysql_master_user }}"
        #     password:   "{{ mysql_master_password }}"
        #     source: [...]

        rpl = self.bkp_conf['replication']

        alive = self.is_instance_alive()
        proc = None

        if not alive:
            proc = self.start_tmp_mysql()

        try:
            pool = mysqlconnpool.make(self.mysql_addr)

            sql = (
                'CHANGE MASTER TO'
                '      MASTER_USER="{user}"'
                '    , MASTER_PASSWORD="{password}"'
                ' FOR CHANNEL "group_replication_recovery"'
            ).format(**rpl)

            self.mysql_pool_query(pool, sql)

        finally:
            if not alive:
                self.stop_tmp_mysql(proc)

    def setup_replication(self):

        # replication:
        #     user:       "{{ mysql_master_user }}"
        #     password:   "{{ mysql_master_password }}"
        #     source:
        #       - host:
        #         port:
        #         id:
        #       - host:
        #         port:
        #         id:

        alive = self.is_instance_alive()
        proc = None

        if not alive:
            proc = self.start_tmp_mysql()

        try:
            pool = mysqlconnpool.make(self.mysql_addr)

            if self.bkp_conf['replication'].get('group_replication') == 1:
                self._group_replication_reset_relay(pool)
                if alive:
                    self.mysql_pool_query(pool, 'START group_replication')
            else:
                self._slave_reset(pool)
                if alive:
                    self.mysql_pool_query(pool, 'START SLAVE')

        finally:
            if not alive:
                self.stop_tmp_mysql(proc)

    def _group_replication_reset_relay(self, pool):

        sqls = (
            'SET GLOBAL master_info_repository = "TABLE"',
            'SET GLOBAL relay_log_info_repository = "TABLE"',

            'RESET SLAVE ALL FOR CHANNEL "group_replication_applier"',
            'RESET SLAVE ALL FOR CHANNEL "group_replication_recovery"',
            'RESET SLAVE ALL FOR CHANNEL ""',
            'RESET SLAVE ALL',
        )

        for sql in sqls:
            self.mysql_pool_query(pool, sql)

    def _slave_reset(self, pool):

        rpl = self.bkp_conf['replication']

        sqls_reset = [
            'STOP SLAVE',

            'SET GLOBAL master_info_repository = "TABLE"',
            'SET GLOBAL relay_log_info_repository = "TABLE"',

            'RESET SLAVE ALL FOR CHANNEL ""',
            'RESET SLAVE ALL',
        ]
        for sql in sqls_reset:
            self.mysql_pool_query(pool, sql)
            time.sleep(1)

        for src in rpl['source']:
            kwarg = copy.deepcopy(rpl)
            kwarg.update(src)
            sql = (
                'CHANGE MASTER TO'
                '      MASTER_HOST="{host}"'
                '    , MASTER_PORT={port}'
                '    , MASTER_USER="{user}"'
                '    , MASTER_PASSWORD="{password}"'
                '    , MASTER_AUTO_POSITION=1'
                ' FOR CHANNEL "master-{id}"'
            ).format(**kwarg)

            self.mysql_pool_query(pool, sql)

    def diff_replication(self):

        self.assert_instance_is_alive()

        rpl = self.bkp_conf['replication']

        sql = 'show slave status'

        mine = self.mysql_query(sql)[0]

        rst = {}

        for src in rpl['source']:
            pool = mysqlconnpool.make({
                'host': src['host'],
                'port': int(src['port']),
                'user': rpl['user'],
                'passwd': rpl['password'],
            })

            k = '{host}:{port}-[{id}]'.format(**src)

            try:
                r = pool.query(sql)[0]
            except MySQLdb.OperationalError as e:
                rst[k] = 'Unreachable: ' + repr(e)
            else:
                diff = self.diff_slave_status_gtidset(mine, r)
                rst[k] = diff

        return rst

    def diff_slave_status_gtidset(self, a, b):

        x = mysqlutil.gtidset.load(a['Executed_Gtid_Set'])
        y = mysqlutil.gtidset.load(b['Executed_Gtid_Set'])

        diff = mysqlutil.gtidset.compare(x, y)

        return diff

    def table_sizes(self, db=None, sortby=None):

        if db is None:
            db = self.get_dbs()[0]

        if sortby is None:
            sortby = 'total'

        mp = {
            'free': lambda x: x['Data_free'],
            'used': lambda x: x['Data_length'],
            'total': lambda x: x['Data_free'] + x['Data_length'],
        }
        sort_key = mp[sortby]

        addr = {}
        addr.update(self.mysql_addr)
        addr.update({
            'db': db,
        })

        pool = mysqlconnpool.make(addr)
        rsts = pool.query('show table status')

        for rst in rsts:
            self._rst_to_number(rst)

        rsts = list(rsts)
        rsts.sort(key=sort_key, reverse=True)

        rsts = [('{Data_length:>6} free-able: {Data_free:>6} {Name}'.format(**humannum.humannum(x)), x)
                for x in rsts]

        return rsts

    def optimize_tables(self, db=None):

        if db is None:
            db = self.get_dbs()[0]

        addr = {}
        addr.update(self.mysql_addr)
        addr.update({
            'db': db,
        })

        pool = mysqlconnpool.make(addr)
        rsts = pool.query('show table status')

        for rst in rsts:

            self._rst_to_number(rst)
            msg = self._table_status_str(rst)

            need_optimize = False

            if rst['Data_free'] > 10 * (1024 ** 3):
                self.info(msg + ' Need optimize, Data_free > 10G')
                need_optimize = True

            if float(rst['Data_free']) / float(rst['Data_length']) > 0.5:
                self.info(msg + ' Need optimize, Data_free/Data_length > 0.5')
                need_optimize = True

            if need_optimize:
                _rsts = pool.query(
                    'optimize local table `{Name}`'.format(**rst))

                self.info('optimize done')
                for _r in _rsts:
                    self.info(str(_r))

            else:
                self.info(msg + ' Do not need optimize')

        rsts = pool.query('show table status')
        self.info('After optimize:')
        for rst in rsts:
            self._rst_to_number(rst)
            msg = self._table_status_str(rst)
            self.info(msg)

    def _rst_to_number(self, rst):
        for k, v in rst.items():
            if isinstance(v, basestring) and v.isdigit():
                rst[k] = int(v)

    def _table_status_str(self, rst):
        hum = humannum.humannum(rst)
        return ('`{Name}` Data_free:{Data_free} / Data_length:{Data_length}'
                ' = {rate:.3f}').format(
            rate=float(rst['Data_free']) / rst['Data_length'],
            **hum)

    def get_dbs(self, exclude=None):

        addr = {}
        addr.update(self.mysql_addr)

        if exclude is None:
            exclude = (
                'information_schema',
                'mysql',
                'performance_schema',
                'sys',
            )

        pool = mysqlconnpool.make(addr)
        rsts = pool.query('show databases')
        dbs = [x['Database'] for x in rsts
               if x['Database'] not in exclude]

        return dbs

    def backup(self):

        self.info("backup ...")

        self.remove_backup(
            'remove old backup {backup_tgz_des3} {backup_data_dir} {backup_binlog_dir}')

        try:
            self.clean_binlog()
            self.backup_data()
            self.backup_binlog()
            self.calc_checksum()
            self.upload_backup()
            self.info_r('backup to s3://{s3_bucket}/{s3_key_by_date} OK')
        finally:
            self.remove_backup(
                'remove backup {backup_tgz_des3} {backup_data_dir} {backup_binlog_dir}')

    def clean_binlog(self, before_days=1):

        ts = timeutil.ts()
        ts = ts - 86400 * before_days
        dt = timeutil.format_ts(ts, 'daily')

        pool = mysqlconnpool.make(self.mysql_addr)

        self.mysql_pool_query(pool,
                              'PURGE BINARY LOGS BEFORE "{dt} 00:00:00"'.format(
                                  dt=dt))

    def backup_data(self):

        self.shell_run('create backup dir {backup_data_dir}',
                       ("innobackupex"
                        " --defaults-file={mysql_my_cnf}"
                        " --user=root"
                        + self._get_arg_password() +
                        " --slave-info"
                        " --no-timestamp"
                        " {backup_data_dir}")
                       )

        self.shell_run('apply log',
                       ("innobackupex"
                        " --apply-log"
                        " {backup_data_dir}")
                       )

        self.shell_run('backup {mysql_my_cnf}',
                       ("cp"
                        " {mysql_my_cnf}"
                        " {backup_data_dir}/")
                       )

    def backup_binlog(self):

        # Backing up binlog after backing up data makes binlog contains more data
        # if mysql is still serving.

        self.info_r('backup binlog {backup_binlog_dir} ...')

        os.makedirs(self.render('{backup_binlog_dir}'), mode=0755)

        binlog_fns = self.list_binlog_fns()
        self.info_r('binlog files: ' + repr(' '.join(binlog_fns)))

        self.shell_run('backup master binlog',
                       ('bin/mysqlbinlog'
                        ' --socket={mysql_socket}'
                        ' --user=root'
                        + self._get_arg_password() +
                        ' --raw'
                        ' --read-from-remote-server'
                        ' --result-file={backup_binlog_dir}/'
                        ' {binlog_fns}'
                        ),
                       binlog_fns=' '.join(binlog_fns),
                       cwd=self.bkp_conf['mysql_base']
                       )

        self.info('backup binlog index')
        with open(self.render("{backup_binlog_dir}/mysql-bin.index"), 'w') as f:
            f.write('\n'.join(['./' + x for x in binlog_fns]) + '\n')

        self.chown('{backup_data_dir} {backup_binlog_dir}')

        self.shell_run('create tar-gz with des3 encrytion',
                       ("tar"
                        "  -c"
                        "  -C {backup_base}"
                        "  {backup_data_tail}"
                        "  {backup_binlog_tail}"
                        " | gzip - --fast"
                        " | openssl des3"
                        "     -pass file:{conf_base}/{des3_password_fn}"
                        " > {backup_tgz_des3}"
                        )
                       )

        self.info_r('backup binlog {backup_binlog_dir} OK')

    def calc_checksum(self):

        self.info_r('calculate checksum of {backup_tgz_des3} ...')

        p = self.render('{backup_tgz_des3}')
        checksums = get_file_checksum(p, ('sha1', ))

        self.bkp_conf['backup_tgz_des3_meta'].update(checksums)

        self.info_r('calculate checksum of {backup_tgz_des3} ...')

    def upload_backup(self):

        if 's3_bucket' not in self.bkp_conf:
            self.info('no s3 bucket specified, ignore backup to s3')
            return

        self.info_r('backup to s3://{s3_bucket}/{s3_key_by_date} ...')

        # boto adds Content-MD5 automatically
        extra_args = {'Metadata': self.bkp_conf['backup_tgz_des3_meta']}

        try:
            self.bc.boto_put(self.render('{backup_tgz_des3}'),
                             self.render('{s3_bucket}'),
                             self.render('{s3_key_by_date}'),
                             extra_args
                             )

        except S3UploadFailedError as e:

            self.info(repr(e) + 'while upload {backup_tgz_des3} to s2 cloud')

            try:
                resp = self.bc.boto_head(self.render('{s3_bucket}'),
                                         self.render('{s3_key_by_date}')
                                         )
            except ClientError as ee:
                self.error(
                    repr(ee) + 'backup file: {backup_tgz_des3} not found in s2 cloud')
                raise

            if resp['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.info('backup file: {backup_tgz_des3} already in s2 cloud')
            else:
                self.error(
                    repr(e) + 'get backup file: {backup_tgz_des3} error')
                raise S3UploadFailedError(
                    repr(e) + 'while upload backup file failed')

        try:
            self.bc.boto_copy(self.render('{s3_bucket}'),
                              self.render('{s3_key_by_date}'),
                              self.render('{se_key_by_port}')
                              )
        except ClientError as e:
            self.info(
                repr(e) + 'while copy {s3_key_by_date} to {se_key_by_port} failed')

    def remove_backup(self, cmd_info):

        self.shell_run(cmd_info,
                       'rm -rf {backup_tgz_des3} {backup_data_dir} {backup_binlog_dir}')

    def restore(self):

        self.info('restore ...')

        self.assert_instance_is_down()

        if not self.has_data_dir():
            self.restore_from_backup()
        else:
            self.info_r('{mysql_data_dir} exists, skip download/restore step')

        self.catchup()
        self.info("restore OK")

    def catchup(self):

        if self.bkp_conf['replication'].get('group_replication') == 1:
            self.info(
                'configured as group replication, do not need to manually catchup')
            return

        self.info_r('catchup start...')
        self.make_runtime_my_cnf()

        if self.is_instance_alive():
            self.info_r('{port} is active, skip applying remote binlog')
        else:
            self.apply_remote_binlog()

        self.info_r('catchup end')

    def restore_from_backup(self):

        self.assert_instance_is_down()

        if 's3_bucket' in self.bkp_conf:
            self.download_backup()

        self.data_check_for_restore()
        self.restore_data()

        self.apply_local_binlog()
        self.shell_run('copy-back binlog file form {backup_binlog_dir}',
                       ("cp"
                        "    {backup_binlog_dir}/mysql-bin.*"
                        "    {mysql_data_dir}/"
                        ))

        self.chown('{mysql_data_dir}')

        if self.bkp_conf['clean_after_restore']:
            self.shell_run('remove backup dir and files',
                           'rm -rf {backup_data_dir} {backup_binlog_dir} {backup_tgz_des3}')

    def assert_instance_is_down(self):

        try:
            self.list_binlog_fns()
            raise MysqlIsAlive(self.render('mysql is still alive {port}'))
        except MySQLdb.OperationalError as e:
            if e[0] == 2002:
                pass
            else:
                raise

    def assert_instance_is_alive(self):

        if self.is_instance_alive():
            pass
        else:
            raise MysqlIsDown(self.render('mysql is down {port}'))

    def is_instance_alive(self):
        try:
            self.list_binlog_fns()
            return True
        except MySQLdb.OperationalError as e:
            if e[0] == 2002:
                return False
            else:
                raise

    def data_check_for_restore(self):

        meta = self.bkp_conf['backup_tgz_des3_meta']
        if 'sha1' in meta:
            actual_sha1 = get_file_checksum(
                self.render('{backup_tgz_des3}'), ('sha1', ))['sha1']
            assert meta['sha1'] == actual_sha1
            self.info('sha1 check OK')

        elif 'md5' in meta:
            actual_md5 = get_file_checksum(
                self.render('{backup_tgz_des3}'), ('md5', ))['md5']
            assert meta['md5'] == actual_md5
            self.info('md5 check OK')

    def restore_data(self):

        self.shell_run('unpack {backup_tgz_des3}',
                       ("cat {backup_tgz_des3}"
                        " | openssl des3"
                        "     -d"
                        "     -pass file:{conf_base}/{des3_password_fn}"
                        " | tar"
                        "     -xzf -"
                        "     -C {backup_base}")
                       )

        self.shell_run('copy-back from {backup_data_dir}',
                       ("innobackupex"
                        "  --defaults-file={backup_my_cnf}"
                        "  --copy-back {backup_data_dir}")
                       )

    def download_backup(self):

        self.info_r(
            'download backup from s3://{s3_bucket}/{s3_key_by_date} ...')

        try:
            os.makedirs(self.render('{backup_base}'), mode=0755)
        except OSError as e:
            if e[0] == errno.EEXIST:
                pass
            else:
                raise

        resp = self.bc.boto_get(
            self.render('{backup_tgz_des3}'),
            self.render('{s3_bucket}'),
            self.render('{s3_key_by_date}')
        )

        self.info_r('downloaded backup to {backup_tgz_des3}')

        meta = resp.get('Metadata')
        self.info('meta: ' + json.dumps(resp.get('Metadata'), indent=2))

        self.bkp_conf['backup_tgz_des3_meta'] = meta

        self.info_r(
            'download backup from s3://{s3_bucket}/{s3_key_by_date} OK')

    def apply_local_binlog(self):

        self.info_r('apply only-in-binlog events ...')

        self.chown('{mysql_data_dir}')

        # A lot binlog will be skipped because there could already be in data.
        # Thus there would be a long time no event is sent to mysql, in which
        # case, mysql closes client connect that causes a failure.
        #
        # Thus we need a very long timeout.
        #
        # But I am still not very sure which timeout causes this failure.
        # In several tests I had random failure with each paarameter set.

        self.info("start mysqld to apply only-in-binlog events ...")
        proc = self.start_tmp_mysql([
            '--net_read_timeout=36000',
            '--wait_timeout=36000',
            '--interactive_timeout=36000',
        ])

        binlog_fns = os.listdir(self.render('{backup_binlog_dir}'))
        binlog_fns = [x for x in binlog_fns
                      if x != 'mysql-bin.index']
        binlog_fns.sort()
        binlog_fns = [self.render('{backup_binlog_dir}/') + x
                      for x in binlog_fns]

        self.info(
            'apply only-in-binlog events back to instance, from: ' + repr(binlog_fns))

        last_binlog_file, last_binlog_pos, gtid_set = self.get_backup_binlog_info()

        for bfn in binlog_fns:

            # apply binlog file one by one or sometimes it experience
            # 'Connection Lost' issue.

            self.info('about to apply local binlog: {bfn}'.format(bfn=bfn))

            self.shell_run('apply binlog not in backup from {backup_binlog_dir}',
                           ('bin/mysqlbinlog'
                            '    --disable-log-bin'
                            '    --exclude-gtids="{gtid_set_str}"'
                            '    {bfn}'
                            ' | bin/mysql --socket={mysql_socket} '
                            + self._get_arg_password()
                            ),
                           bfn=bfn,
                           gtid_set_str=mysqlutil.gtidset.dump(
                               gtid_set, line_break=''),
                           cwd=self.bkp_conf['mysql_base']
                           )

            self.info('finsihed applying local binlog: {bfn}'.format(bfn=bfn))

        self.stop_tmp_mysql(proc)
        self.info_r('apply only-in-binlog events OK')

    def apply_remote_binlog(self):

        # Start a temporary instance to retrieve binlog generated by the
        # this instance after this backup was made.

        # We need to use a temporary server-id or mysql refuse to apply
        # binlog.
        #
        # We can not use "--replicate-same-server-id" to force mysql to apply
        # binlog either.
        # Since "--replicate-same-server-id" can not be used together with
        # "--log-slave-updates", which is required in our scenario, to let
        # every mysql instance keep full history binlog.

        self.info('apply remote binlog ...')

        proc = self.start_tmp_mysql(['--server-id={tmp_server_id}'])

        self.setup_replication()

        # wait for binlog-sync to start
        time.sleep(5)
        self.wait_remote_binlog()

        self.stop_tmp_mysql(proc)
        self.info('apply remote binlog OK')

    def wait_remote_binlog(self):

        # wait for binlog-sync to start
        time.sleep(60)

        while True:

            rsts = self.mysql_query('show slave status')
            if len(rsts) == 0:
                raise MysqlRestoreError('no slave status found!')

            all_done = True
            sleeps = []
            for rst in rsts:

                state = self.check_slave_status(rst)
                if state != 'done':
                    all_done = False

                sleep_time = int(rst['Seconds_Behind_Master']) / 5
                sleeps.append(sleep_time)

            if all_done:
                break

            sleep_time = max(sleeps)

            if sleep_time > 60 * 5:
                sleep_time = 60 * 5

            if sleep_time < 30:
                sleep_time = 30

            time.sleep(sleep_time)

    def check_slave_status(self, rst):

        ctx = '{Master_Host}:{Master_Port}'.format(**rst)

        if rst['Last_IO_Error'] != '' or rst['Last_SQL_Error'] != '':
            raise MysqlRestoreError(
                rst['Last_IO_Error'] + ' ' + rst['Last_SQL_Error'])

        if rst['Slave_IO_Running'] != 'Yes' or rst['Slave_SQL_Running'] != 'Yes':
            raise MysqlRestoreError('IO/SQL not running')

        rcv = mysqlutil.gtidset.load(rst["Retrieved_Gtid_Set"])
        exc = mysqlutil.gtidset.load(rst["Executed_Gtid_Set"])

        diff = mysqlutil.gtidset.compare(exc, rcv)

        self.debug('applying remote binlog {ctx}:'
                   ' io_state: {Slave_IO_State}'
                   ' recv: {rcv}'
                   ' exec: {exc}'.format(ctx=ctx, rcv=rcv, exc=exc, **rst))

        self.debug('applying remote binlog {ctx}:'
                   ' recv but not exec: "{diff}"'.format(ctx=ctx, diff=diff['onlyright'],))

        if rst['Slave_IO_State'] != 'Waiting for master to send event':
            return 'wait'

        # - lagging seconds is small
        # - received but not unexecuted events are little(mysql running on 1000 tps is normal).
        # - only one actively changing uuid(mysql instance), the only one
        #   serving user requests.
        if (int(rst['Seconds_Behind_Master']) < 1
            and diff['onlyright']['length'] < 1000
                and len(diff['onlyright']['gtidset']) <= 1):

            self.debug('applying remote binlog {ctx}: done'.format(ctx=ctx))

            # there could be events on remote created in less than 1
            # second
            time.sleep(1)
            return 'done'

        return 'wait'

    def start_tmp_mysql(self, opts=()):

        self.info("start mysqld ...")

        # always disable group replication auto-start during restoring
        opts = list(opts) + ['--loose-group-replication-start-on-boot=off']

        cmd = self.render(
            './bin/mysqld --defaults-file={mysql_my_cnf} ' + (' '.join(opts)))
        cwd = self.bkp_conf['mysql_base']
        proc = subprocess.Popen(cmd,
                                cwd=cwd,
                                close_fds=True,
                                shell=True,
                                stderr=subprocess.PIPE,
                                stdout=subprocess.PIPE)

        now = time.time()
        while time.time() - now < 60:

            try:
                self.mysql_query('show master logs')
                break

            except MySQLdb.OperationalError as e:
                if e[0] == 2002:
                    self.debug_r(repr(e) + ' while starting mysql')
                    time.sleep(0.1)
                else:
                    raise
        else:
            out, err = proc.communicate()
            proc.wait()
            raise MysqlRestoreError(proc.returncode, cmd, out, err)

        self.info_r("start mysqld OK")

        return proc

    def stop_tmp_mysql(self, proc):

        self.info_r('stop mysql ...')

        proc.send_signal(signal.SIGTERM)
        proc.wait()

        while True:
            if not os.path.exists(self.render("{mysql_pid_path}")):
                break
            else:
                self.debug('waiting for mysql to stop')
                time.sleep(0.5)

        time.sleep(0.5)
        self.info_r('stop mysql OK')

    def mysql_query(self, sql):
        pool = mysqlconnpool.make(self.mysql_addr)
        return self.mysql_pool_query(pool, sql)

    def mysql_pool_query(self, pool, sql):

        self.debug('mysql query: {sql}'.format(sql=sql))
        rst = pool.query(sql)

        self.debug('mysql query resutl of {sql}: {rst}'.format(
            sql=repr(sql),
            rst=json.dumps(rst, indent=2)))

        return rst

    def mysql_insert_test_value(self, v):
        self.mysql_query(self.bkp_conf['sql_test_insert'].format(v=v))

    def mysql_get_last_2_test_value(self):
        return self.mysql_query(self.bkp_conf['sql_test_get_2'])

    def extend_backup_conf(self, base_backup_conf):

        bkp_conf = copy.deepcopy(base_backup_conf)
        bkp_conf.update({
            'cwd':                  os.getcwd(),
            'des3_password_fn':     'des3_password',
            'backup_tgz_des3_meta': {},
        })

        bkp_conf.setdefault('date_str',  backup_date_str())

        ptn = [
            ('mysql_user',           "mysql-{port}"),
            ('mysql_user_group',     "mysql"),

            ('mysql_socket',         "/var/run/mysqld/mysqld-{port}.sock"),
            ('mysql_pid_path',       "/var/run/mysqld/mysqld-{port}.pid"),

            ('mysql_data_dir',       "{data_base}/mysql-{port}"),
            ('mysql_my_cnf',         "{data_base}/mysql-{port}/my.cnf"),

            # use a different server-id to accept binlog created on this
            # instance.
            ('tmp_server_id',        "{instance_id}00{port}"),

            ('backup_tgz_des3_tail',               "mysql-{port}.tgz.des3"),
            ('backup_data_tail',                   "mysql-{port}-backup"),
            ('backup_data_dir',      "{backup_base}/mysql-{port}-backup"),
            ('backup_my_cnf',
             "{backup_base}/mysql-{port}-backup/my.cnf"),
            ('backup_tgz_des3',     "{backup_base}/{backup_tgz_des3_tail}"),

            ('backup_binlog_tail',                 "mysql-{port}-binlog"),
            ('backup_binlog_dir',    "{backup_base}/mysql-{port}-binlog"),

            ('s3_key_by_date',
             "{date_str}/{port}/{backup_tgz_des3_tail}"),
            ('s3_key_by_port',
             "{port}/{date_str}/{backup_tgz_des3_tail}"),
            ('mes',
             "{host}:{port} [{instance_id}] {mysql_data_dir}"),
        ]

        for k, v in ptn:
            bkp_conf.setdefault(k, v.format(**bkp_conf))

        for k, v in sorted(bkp_conf.items()):
            logger.info('backup conf: {k:<20} : {v}'.format(k=k, v=v))

        return bkp_conf

    def has_data_dir(self):
        return os.path.exists(self.render("{mysql_data_dir}"))

    def make_runtime_my_cnf(self):
        self.shell_run('copy {conf_base}/my.cnf to {mysql_data_dir}/',
                       ("cp"
                        " {conf_base}/my.cnf"
                        " {mysql_data_dir}/")
                       )
        self.chown('{mysql_data_dir}/my.cnf')

    def list_binlog_fns(self):

        # +------------------+-----------+
        # | Log_name         | File_size |
        # +------------------+-----------+
        # | mysql-bin.000001 |       154 |
        # +------------------+-----------+

        rst = self.mysql_query('show master logs')

        return [x['Log_name'] for x in rst]

    def get_backup_binlog_info(self):

        path = self.render('{backup_data_dir}/xtrabackup_binlog_info')

        with open(path, 'r') as f:
            cont = f.read()

        # # cont is:
        # mysql-bin.000005	194	1fcc256b-84b0-11e6-acaf-00163e03bb03:1-326,\n
        # xxxxxxxx-84b0-11e6-acaf-00163e03bb03:1

        self.debug('read from xtrabackup_binlog_info: ' + repr(cont))

        cont = ''.join(cont.split('\n'))
        fields = cont.split('\t')

        last_binlog_file = fields[0]
        last_binlog_pos = int(fields[1])

        gtid_set = mysqlutil.gtidset.load(fields[2])
        self.debug('gtid_set extracted: ' + repr(gtid_set))

        return last_binlog_file, last_binlog_pos, gtid_set

    def chown(self, path_or_key):

        # self.chown("/a/b/c")
        # or
        # self.chown("{mysql_data_dir} {backup_data_dir}")

        self.shell_run('chown to {mysql_user}:{mysql_user_group} dir: ' + path_or_key,
                       ("chown"
                        " -R {mysql_user}:{mysql_user_group}"
                        " " + path_or_key)
                       )

    def shell_run(self, mes, cmd, cwd=None, **kwarg):

        self.info_r(mes + ' ...', **kwarg)
        self.info_r('cwd={cwd}')

        cmd = self.render(cmd, **kwarg)

        returncode, out, err = _shell_run(cmd, cwd=cwd)

        for line in out + err:
            self.debug_r(line)

        out = '\n'.join(out)
        err = '\n'.join(err)

        if returncode != 0:
            raise MysqlBackupError(returncode, cmd, out, err,
                                   'failed to backup')

        self.info_r(mes + ' OK', **kwarg)

    def render(self, s, **kwarg):
        a = {}
        a.update(self.bkp_conf)
        a.update(kwarg)
        return s.format(**a)

    def error_r(self, s, **kwarg):
        logger.error(self.bkp_conf['mes'] + ': ' + self.render(s, **kwarg))

    def warn_r(self, s, **kwarg):
        logger.warn(self.bkp_conf['mes'] + ': ' + self.render(s, **kwarg))

    def info_r(self, s, **kwarg):
        logger.info(self.bkp_conf['mes'] + ': ' + self.render(s, **kwarg))

    def debug_r(self, s, **kwarg):
        logger.debug(self.bkp_conf['mes'] + ': ' + self.render(s, **kwarg))

    def error(self, s):
        logger.error(self.bkp_conf['mes'] + ': ' + s)

    def warn(self, s):
        logger.warn(self.bkp_conf['mes'] + ': ' + s)

    def info(self, s):
        logger.info(self.bkp_conf['mes'] + ': ' + s)

    def debug(self, s):
        logger.debug(self.bkp_conf['mes'] + ': ' + s)

    def _get_arg_password(self):
        if 'root_password' in self.bkp_conf:
            arg_password = ' --password=' + self.bkp_conf['root_password']
        else:
            arg_password = ''

        return arg_password


def _shell_run(cmd, cwd=None):

    proc = subprocess.Popen(cmd,
                            cwd=cwd,
                            shell=True,
                            stderr=subprocess.PIPE,
                            stdout=subprocess.PIPE)

    out, err = proc.communicate()
    proc.wait()

    out = out.strip().split('\\n')
    err = err.strip().split('\\n')

    if out == ['']:
        out = []

    if err == ['']:
        err = []

    return proc.returncode, out, err


def backup_date_str():
    return cdate("%Y_%m_%d")


def cdate(fmt):
    return time.strftime(fmt, time.localtime())


def get_file_checksum(path, checksum_keys=('md5')):

    sums = {}
    for k in checksum_keys:
        if k == 'md5':
            sums['md5'] = hashlib.md5()
        elif k == 'sha1':
            sums['sha1'] = hashlib.sha1()
        else:
            raise ValueError('invalid checksum key ' + k)

    with open(path, 'r') as f:

        while True:
            buf = f.read(1024 * 1024 * 32)
            if buf == '':
                break
            for k in sums:
                sums[k].update(buf)

    rst = {}
    for k in sums:
        rst[k] = sums[k].hexdigest().lower()

    return rst


def boto_client(host, access_key, secret_key):

    session = boto3.session.Session()
    client = session.client(
        's3',
        use_ssl=False,
        endpoint_url="http://%s:%s" % (host, 80),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(s3={'addressing_style': 'path'}),
    )

    return client


def boto_put(cli, fpath, bucket_name, key_name, extra_args):

    MB = 1024**2
    GB = 1024**3

    config = TransferConfig(multipart_threshold=1 * GB,
                            multipart_chunksize=512 * MB, )

    cli.upload_file(
        Filename=fpath,
        Bucket=bucket_name,
        Key=key_name,
        Config=config,
        ExtraArgs=extra_args,
    )


def boto_head(cli, bucket_name, key_name):

    resp = cli.head_object(
        Bucket=bucket_name,
        Key=key_name,
    )

    return resp


def boto_get(cli, fpath, bucket_name, key_name):

    obj = cli.get_object(Bucket=bucket_name, Key=key_name)

    # obj = {
    #         'Body': <botocore.response.StreamingBody object at 0x1bc7d10>,
    #         'ContentType': 'application/octet-stream',
    #         'ResponseMetadata': {
    #                 'HTTPStatusCode': 200, 'RetryAttempts': 0, 'HostId': '',
    #                 'RequestId': '00079534-1610-1919-2642-00163e03bb03',
    #                 'HTTPHeaders': {
    #                         'access-control-allow-headers': 'Origin, Content-Type, Accept, Content-Length',
    #                         'access-control-allow-methods': 'GET, PUT, POST, DELETE, OPTIONS, HEAD',
    #                         'access-control-allow-origin': '*',
    #                         'access-control-max-age': '31536000',
    #                         'cache-control': 'max-age=31536000',
    #                         'connection': 'keep-alive',
    #                         'content-length': '3508888',
    #                         'content-type': 'application/octet-stream',
    #                         'date': 'Wed, 19 Oct 2016 11:26:42 GMT',
    #                         'etag': '"12619d55847bb120b903b7b7998be1fb"',
    #                         'last-modified': 'Wed, 19 Oct 2016 11:14:11 GMT',
    #                         'server': 'openresty/1.9.7.4',
    #                         'x-amz-meta-md5': '12619d55847bb120b903b7b7998be1fb',
    #                         'x-amz-meta-s2-crc32': 'c96376ab',
    #                         'x-amz-meta-s2-size': '3508888'
    #                         'x-amz-meta-sha1': 'aba30b9b9da5ea743d52c85db7ff82f7c7dc41eb',
    #                         'x-amz-request-id': '00079534-1610-1919-2642-00163e03bb03',
    #                         'x-amz-s2-requester': 'drdrxp',
    #                 }
    #         },
    #         'LastModified': datetime.datetime(2016, 10, 19, 11, 14, 11, tzinfo=tzutc()),
    #         'ContentLength': 3508888,
    #         'ETag': '"12619d55847bb120b903b7b7998be1fb"',
    #         'CacheControl': 'max-age=31536000',
    #         'Metadata': {
    #                 's2-size': '3508888',
    #                 's2-crc32': 'c96376ab',
    #                 'sha1': 'aba30b9b9da5ea743d52c85db7ff82f7c7dc41eb',
    #                 'md5': '12619d55847bb120b903b7b7998be1fb'
    #         }
    # }

    with open(fpath, 'wb') as f:
        while True:
            buf = obj['Body'].read(1024 * 1024 * 8)
            if buf == '':
                break

            f.write(buf)

    logger.debug('get_object rst: ' + repr(obj))

    return obj


def load_cli_args():

    parser = argparse.ArgumentParser(description='mysql backup-restore tool')

    parser.add_argument('--conf-path', action='store',
                        help='path to config file in yaml')

    # command line arguments override config file.

    parser.add_argument('--mysql-base',    action='store',
                        help='base dir of mysql executable')
    parser.add_argument('--host',          action='store',
                        help='name of this host, just as identity of backup file name')
    parser.add_argument('--data-base',     action='store',
                        help='base dir of mysql data, like /data1')
    parser.add_argument('--backup-base',   action='store',
                        help='base dir of backup tmp files, like /data1/backup')
    parser.add_argument('--port',          action='store',
                        help='serving port of the mysql instance to backup/restore')
    parser.add_argument('--instance-id',   action='store',
                        help='id in number, as part of backup file name, it should be unique in a replication group')
    parser.add_argument('--date-str',      action='store',
                        help='date in form 2017_01_01. It is used in backup file name, or to specify which backup to use for restore. when absent, use date of today')
    parser.add_argument('--s3-host',       action='store',
                        help='s3 compatible service hostname to store backup')
    parser.add_argument('--s3-bucket',     action='store',
                        help='s3 bucket name')
    parser.add_argument('--s3-access-key', action='store',
                        help='s3 access key')
    parser.add_argument('--s3-secret-key', action='store',
                        help='s3 secret key')
    parser.add_argument('--verbose', '-v', action='count',
                        help='output more debug info')

    parser.add_argument('--clean-after-restore', action='store_true',
                        help='clean backup files after restore')

    parser.add_argument('cmd', type=str, nargs=1, choices=[
                        'backup', 'restore', 'catchup_binlog', 'setup_replication'], help='command to run')
    parser.add_argument('--when', action='store', choices=[
                        'no-data-dir', 'stopped'], help='condition that must be satisfied before a command runs')

    args = parser.parse_args()
    logger.info('args={a}'.format(a=args))

    return args


def load_conf_from_file(conf_path):

    logger.info('loading backup conf from {p}'.format(p=conf_path))

    if conf_path is not None:

        with open(conf_path, 'r') as f:
            content = f.read()

        conf = yaml.load(content)
        logger.info('load conf from {f}: {c}'.format(f=conf_path, c=conf))

        if 'conf_base' not in conf:
            conf['conf_base'] = os.path.dirname(os.path.realpath(conf_path))
            logger.info('add config conf_base={conf_base}'.format(**conf))
    else:
        conf = {}

    return conf


def load_conf(args):

    conf = load_conf_from_file(args.conf_path)

    conf_keys = ('mysql_base',
                 'host',
                 'data_base',
                 'backup_base',
                 'port',
                 'instance_id',
                 'date_str',
                 's3_host',
                 's3_bucket',
                 's3_access_key',
                 's3_secret_key',

                 'clean_after_restore',
                 )

    for k in conf_keys:
        v = getattr(args, k)
        if v is not None:
            conf[k] = v
            logger.info(
                'add config from command line: {k}={v}'.format(k=k, v=v))

    logger.info('conf={c}'.format(c=conf))

    return conf


def main():

    # config root logger
    logging.basicConfig(stream=sys.stdout, level=logging.WARNING)

    # config logger for this module
    logutil.make_logger(base_dir='/tmp',
                        log_name=__name__,
                        log_fn=logutil.get_root_log_fn(),
                        level=logging.DEBUG)

    args = load_cli_args()
    conf = load_conf(args)

    if args.verbose is not None:

        lvls = {
            1: logging.DEBUG,
        }
        level = lvls.get(args.verbose, logging.DEBUG)

        logging.basicConfig(stream=sys.stdout, level=level)

    mb = MysqlBackup(conf)

    # silently quit if condition 'when' is not satisfied

    when = args.when

    if when is not None:

        if when == 'no-data-dir':
            if os.path.exists(mb.render("{mysql_data_dir}")):
                return

        elif when == 'stopped':
            if mb.is_instance_alive():
                return

        else:
            raise ValueError('invalid argument "when": {w}'.format(w=when))

    # run command

    cmd = args.cmd[0]

    try:
        if cmd == 'backup':
            mb.backup()

        elif cmd == 'restore':
            mb.restore_from_backup()

        elif cmd == 'catchup_binlog':
            mb.assert_instance_is_down()
            mb.apply_remote_binlog()

        elif cmd == 'setup_replication':
            mb.setup_replication()

        else:
            raise ValueError('invalid command: ' + str(cmd))

    except (MysqlBackupError, MysqlRestoreError) as e:
        print e.__class__.__name__
        for i in range(4):
            print e[i]
        sys.exit(1)


if __name__ == "__main__":
    main()
