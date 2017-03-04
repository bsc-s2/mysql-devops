#!/usr/bin/env python2
# coding: utf-8

# -*- coding:utf-8 -*-
'''
required:
    xtrabackup version > 2.4 (for MySQL 5.7)
'''

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
import argparse
import yaml

import boto3
import MySQLdb
from boto3.s3.transfer import TransferConfig
from botocore.client import Config

from pykit import mysqlconnpool

logger = logging.getLogger(__name__)


class MysqlBackupError(Exception):
    pass


class MysqlRestoreError(Exception):
    pass


class MysqlIsAlive(Exception):
    pass


class MysqlBackup(object):

    def __init__(self, bkp_conf):

        self.bkp_conf = self.extend_backup_conf(bkp_conf)

        self.mysql_addr = {'unix_socket': self.bkp_conf['mysql_socket'],
                           'user': 'root'}

        self.mysql_conn_pool = mysqlconnpool.make(self.mysql_addr)

    def setup_replication(self, source_list):

        alive = self.is_instance_alive()
        proc = None

        if not alive:
            proc = self.start_tmp_mysql()

        sqls_reset = [
            'STOP SLAVE',

            'SET GLOBAL master_info_repository = "TABLE"',
            'SET GLOBAL relay_log_info_repository = "TABLE"',

            'RESET SLAVE ALL FOR CHANNEL ""',
            'RESET SLAVE ALL',
        ]

        try:
            for sql in sqls_reset:
                self.mysql_query(sql)

            for src in source_list:
                sql = (
                    'CHANGE MASTER TO'
                    '      MASTER_HOST="{host}"'
                    '    , MASTER_PORT={port}'
                    '    , MASTER_USER="{user}"'
                    '    , MASTER_PASSWORD="{password}"'
                    '    , MASTER_AUTO_POSITION=1'
                    ' FOR CHANNEL "{channel}"'
                ).format(**src)

                self.mysql_query(sql)

            if alive:
                self.mysql_query('START SLAVE')

        finally:
            if not alive:
                self.stop_tmp_mysql(proc)

    def backup(self):

        self.info("backup ...")

        self.backup_data()
        self.backup_binlog()
        self.calc_checksum()
        self.upload_backup()
        self.shell_run('remove backup dir',
                       'rm -rf {backup_data_dir} {backup_binlog_dir}')

        self.info("backup OK")

    def backup_data(self):

        self.shell_run('create backup dir {backup_data_dir}',
                       ("innobackupex"
                        " --defaults-file={mysql_my_cnf}"
                        " --user=root"
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

        self.info_r('backup to s3://{s3_bucket}/{s3_key} ...')

        bc = boto_client(self.bkp_conf['s3_host'],
                         self.bkp_conf['s3_access_key'],
                         self.bkp_conf['s3_secret_key'])

        # boto adds Content-MD5 automatically
        extra_args = {'Metadata': self.bkp_conf['backup_tgz_des3_meta']}

        boto_put(bc,
                 self.render('{backup_tgz_des3}'),
                 self.render('{s3_bucket}'),
                 self.render('{s3_key}'),
                 extra_args
                 )

        self.info_r('backup to s3://{s3_bucket}/{s3_key} OK')

    def restore(self):

        self.info('restore ...')

        self.restore_from_backup()
        self.apply_remote_binlog()

        self.info("restore OK")

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

    def assert_instance_is_down(self):

        try:
            self.list_binlog_fns()
            raise MysqlIsAlive(self.render('mysql is still alive {port}'))
        except MySQLdb.OperationalError as e:
            if e[0] == 2002:
                pass
            else:
                raise

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

        self.info_r('download backup from s3://{s3_bucket}/{s3_key} ...')

        try:
            os.makedirs(self.render('{backup_base}'), mode=0755)
        except OSError as e:
            if e[0] == errno.EEXIST:
                pass
            else:
                raise

        bc = boto_client(self.bkp_conf['s3_host'],
                         self.bkp_conf['s3_access_key'],
                         self.bkp_conf['s3_secret_key'])
        resp = boto_get(bc,
                        self.render('{backup_tgz_des3}'),
                        self.render('{s3_bucket}'),
                        self.render('{s3_key}')
                        )

        self.info_r('downloaded backup to {backup_tgz_des3}')

        meta = resp.get('Metadata')
        self.info('meta: ' + json.dumps(resp.get('Metadata'), indent=2))

        self.bkp_conf['backup_tgz_des3_meta'] = meta

        self.info_r('download backup from s3://{s3_bucket}/{s3_key} OK')

    def apply_local_binlog(self):

        self.info_r('apply only-in-binlog events ...')

        self.chown('{mysql_data_dir}')

        self.info("start mysqld to apply only-in-binlog events ...")
        proc = self.start_tmp_mysql()

        binlog_fns = os.listdir(self.render('{backup_binlog_dir}'))
        binlog_fns = [x for x in binlog_fns
                      if x != 'mysql-bin.index']
        binlog_fns.sort()
        binlog_fns = [self.render('{backup_binlog_dir}/') + x
                      for x in binlog_fns]

        self.info(
            'apply only-in-binlog events back to instance, from: ' + repr(binlog_fns))

        last_binlog_file, last_binlog_pos, gtid_set = self.get_backup_binlog_info()

        self.shell_run('apply binlog not in backup from {backup_binlog_dir}',
                       ('bin/mysqlbinlog'
                        '    --disable-log-bin'
                        '    --exclude-gtids="{gtid_set_str}"'
                        '    {binlog_fns}'
                        ' | bin/mysql --socket={mysql_socket} '
                        ),
                       binlog_fns=' '.join(binlog_fns),
                       gtid_set_str=make_gtid_set_str(gtid_set),
                       cwd=self.bkp_conf['mysql_base']
                       )

        self.stop_tmp_mysql(proc)
        self.info_r('apply only-in-binlog events OK')

    def apply_remote_binlog(self):

        # Start a temporary instance to retreive binlog generated by the
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

        self.mysql_query('start slave')

        # wait for binlog-sync to start
        time.sleep(1)
        self.wait_remote_binlog()

        self.stop_tmp_mysql(proc)
        self.info('apply remote binlog OK')

    def wait_remote_binlog(self):

        # wait for binlog-sync to start
        time.sleep(60)

        while True:

            # TODO wait all master to be synced
            rst = self.mysql_query('show slave status')
            rst = rst[0]

            if rst['Last_IO_Error'] != '' or rst['Last_SQL_Error'] != '':
                raise MysqlRestoreError(
                    rst['Last_IO_Error'] + ' ' + rst['Last_SQL_Error'])

            if rst['Slave_IO_Running'] != 'Yes' or rst['Slave_SQL_Running'] != 'Yes':
                raise MysqlRestoreError('IO/SQL not running')

            if rst['Slave_IO_State'] == 'Waiting for master to send event':

                if int(rst['Seconds_Behind_Master']) < 1:
                    # there could be events on remote created in less than 1
                    # second
                    time.sleep(1)
                    break

            self.info('applying remote binlog:'
                      ' io_state: "{Slave_IO_State}"'
                      ' recv: "{Retrieved_Gtid_Set}"'
                      ' exec: "{Executed_Gtid_Set}"'.format(**rst))

            time.sleep(1)

    def start_tmp_mysql(self, opts=()):

        self.info("start mysqld ...")

        cmd = self.render(
            './bin/mysqld --defaults-file={mysql_my_cnf} ' + (' '.join(opts)))
        cwd = self.bkp_conf['mysql_base']
        proc = subprocess.Popen(cmd,
                                cwd=cwd,
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

        bkp_conf.setdefault('date_str',  cdate("%Y_%m_%d"))

        ptn = [
            ('mysql_user',           "mysql-{port}"),
            ('mysql_user_group',     "mysql"),

            ('mysql_socket',         "/tmp/mysql-{port}.sock"),
            ('mysql_pid_path',       "/var/run/mysqld/mysqld-{port}.pid"),

            ('mysql_data_dir',       "{data_base}/mysql-{port}"),
            ('mysql_my_cnf',         "{data_base}/mysql-{port}/my.cnf"),

            # use a different server-id to accept binlog created on this
            # instance.
            ('tmp_server_id',        "{instance_id}00{port}"),

            ('backup_data_tail',                   "mysql-{port}-backup"),
            ('backup_data_dir',      "{backup_base}/mysql-{port}-backup"),
            ('backup_my_cnf',        "{backup_base}/mysql-{port}-backup/my.cnf"),
            ('backup_tgz_des3',      "{backup_base}/mysql-{port}.tgz.des3"),

            ('backup_binlog_tail',                 "mysql-{port}-binlog"),
            ('backup_binlog_dir',    "{backup_base}/mysql-{port}-binlog"),

            ('s3_key',               "mysql-backup-daily/{port}/{date_str}/mysql-{port}.tgz"),
            ('mes',                  "{host}:{port} [{instance_id}] {mysql_data_dir}"),
        ]

        for k, v in ptn:
            bkp_conf.setdefault(k, v.format(**bkp_conf))

        for k, v in sorted(bkp_conf.items()):
            logger.info('backup conf: {k:<20} : {v}'.format(k=k, v=v))

        return bkp_conf

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
        _set = fields[2].split(',')

        self.debug('gtid set: ' + repr(_set))

        gtid_set = {}
        for elt in _set:

            uuid, rng = elt.split(':')
            rng = rng.split('-')

            # single transaction range is just xxx:1, not xxx:1-2
            if len(rng) == 1:
                rng = [rng[0], rng[0]]

            gtid_set[uuid] = [int(rng[0]), int(rng[1])]

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


def make_gtid_set_str(gtid_set):
    ranges = []
    for uuid, rng in gtid_set.items():
        ranges.append('{uuid}:{start}-{end}'.format(uuid=uuid,
                                                    start=rng[0], end=rng[1]))
    return ','.join(ranges)


def mysql_query(conn, sql):

    cur = conn.cursor(MySQLdb.cursors.DictCursor)
    cur.execute(sql)
    rst = cur.fetchall()
    cur.close()

    return rst


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
                            multipart_chunksize=32 * MB, )

    cli.upload_file(
        Filename=fpath,
        Bucket=bucket_name,
        Key=key_name,
        Config=config,
        ExtraArgs=extra_args,
    )


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


def init_logger(fn=None, lvl=logging.DEBUG):

    logging.basicConfig(stream=sys.stdout, level=lvl)

    logger.setLevel(lvl)

    file_path = os.path.join('/tmp/{n}.out'.format(n=fn or 'mysqlbackup'))
    handler = logging.handlers.WatchedFileHandler(file_path)

    _formatter = logging.Formatter(
        "[%(asctime)s,%(process)d-%(thread)d,%(filename)s,%(lineno)d,%(levelname)s] %(message)s")

    handler.setFormatter(_formatter)

    logger.handlers = []
    logger.addHandler(handler)


def load_args_conf():

    parser = argparse.ArgumentParser(description='mysql backup-restore tool')

    parser.add_argument('--conf-path', action='store', help='path to config file in yaml')

    # command line arguments override config file.

    parser.add_argument('--mysql-base',    action='store', help='base dir of mysql executable')
    parser.add_argument('--host',          action='store', help='name of this host, just as identity of backup file name')
    parser.add_argument('--data-base',     action='store', help='base dir of mysql data, like /data1')
    parser.add_argument('--backup-base',   action='store', help='base dir of backup tmp files, like /data1/backup')
    parser.add_argument('--port',          action='store', help='serving port of the mysql instance to backup/restore')
    parser.add_argument('--instance-id',   action='store', help='id in number, as part of backup file name, it should be unique in a replication group')
    parser.add_argument('--date-str',      action='store', help='date in form 2017_01_01. It is used in backup file name, or to specify which backup to use for restore. when absent, use date of today')
    parser.add_argument('--s3-host',       action='store', help='s3 compatible service hostname to store backup')
    parser.add_argument('--s3-bucket',     action='store', help='s3 bucket name')
    parser.add_argument('--s3-access-key', action='store', help='s3 access key')
    parser.add_argument('--s3-secret-key', action='store', help='s3 secret key')

    parser.add_argument('cmd', type=str, nargs=1, choices=['backup', 'restore', 'catchup_binlog'], help='command to run')
    parser.add_argument('--when', action='store', choices=['no-data-dir', 'stopped'], help='condition that must be satisfied before a command runs')

    args = parser.parse_args()

    if args.conf_path is not None:

        with open(args.conf_path, 'r') as f:
            content = f.read()

        conf = yaml.load(content)
        logger.info('load conf from {f}: {c}'.format(f=args.conf_path, c=conf))

        if 'conf_base' not in conf:
            conf['conf_base'] = os.path.dirname(os.path.realpath(args.conf_path))
            logger.info('add config conf_base={conf_base}'.format(**conf))
    else:
        conf = {}


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
    )

    for k in conf_keys:
        v = getattr(args, k)
        if v is not None:
            conf[k] = v
            logger.info('add config from command line: {k}={v}'.format(k=k, v=v))

    logger.info('conf={c}'.format(c=conf))

    return args, conf


def main():

    init_logger()
    args, conf = load_args_conf()

    logger.info('cmd={cmd}'.foramt(cmd=cmd))
    logger.info('conf={conf}'.format(conf=conf))

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

    cmd = args.cmd

    try:
        if cmd == 'backup':
            mb.backup()

        elif cmd == 'restore':
            mb.restore_from_backup()

        elif cmd == 'catchup_binlog':
            mb.assert_instance_is_down()
            mb.apply_remote_binlog()

        else:
            raise ValueError('invalid command: ' + str(cmd))

    except (MysqlBackupError, MysqlRestoreError) as e:
        print e.__class__.__name__
        for i in range(4):
            print e[i]
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
