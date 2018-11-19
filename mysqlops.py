#!/usr/bin/env python2
# coding: utf-8

import argparse
import json
import logging
import os
import sys

from pykit import humannum
from pykit import jobq
from pykit import logutil
from pykit import timeutil
from pykit import utfjson

import botoclient
import mysqlbackup

logger = logging.getLogger(__name__)

if __name__ == "__main__":

    rootlogger = logutil.make_logger(base_dir='/var/log',
                                     log_fn=logutil.get_root_log_fn(),
                                     level=logging.DEBUG)
    logutil.add_std_handler(rootlogger, stream=sys.stdout)
    rootlogger.handlers[1].setLevel(logging.WARN)

    parser = argparse.ArgumentParser(
        description='run commands for one or more ports concurrenty')

    parser.add_argument('--conf-base', type=str,
                        required=False,  help='base path to config file')
    parser.add_argument('--conf-fn',   type=str, required=False,
                        help='conf file name for each port')
    parser.add_argument('--jobs',      type=int, required=False,
                        default=1, help='nr of threads to run')
    parser.add_argument('--cmd',       type=str, required=True,  choices=[
        'backup',
        'catchup',
        'group_replication_setup_channel',
        'group_replication_bootstrap',
        'make_mycnf',
        'optimize',
        'replication_diff',
        'restore',
        'restore_from_backup',
        'query',
        'setup_replication',
        'table_size',
        'user',
        'check_backup_file',
    ], help='command to run')
    parser.add_argument('--ports',     type=int, required=False,
                        nargs='*', help='ports to run "cmd" on')
    parser.add_argument('--db',        type=str, required=False,
                        help='specifies db name to run command on')
    parser.add_argument('--human',     action='store_true',
                        required=False,  help='print result for human')
    parser.add_argument('--full',      action='store_true',
                        required=False,  help='do not reduce any info when display')
    parser.add_argument('--size',      type=str, required=False,
                        help='specify size filter expression e.g.: ">10M"')
    parser.add_argument('--sortby',    type=str, required=False,
                        choices=['free', 'total', 'used'], help='sort by')

    # for command query
    parser.add_argument('--sql',       type=str,
                        required=False,  help='sql in string')

    # options for command 'user'
    parser.add_argument('--username',  type=str,
                        required=False,  help='user name to create')
    parser.add_argument('--password',  type=str,
                        required=False,  help='login password')
    parser.add_argument('--host',      type=str,
                        required=False, default='%', help='user host')
    parser.add_argument('--privilege', type=str, required=False, default='*.*:readwrite',
                        help='privilege in form of "my_db.my_table:SELECT,UPDATE"')
    parser.add_argument('--binlog',    type=int, required=False,
                        choices=[0, 1], help='generate binlog for user created')

    parser.add_argument('--date-str',            action='store',
                        help='date in form 2017_01_01. It is used in backup file name, or to specify which backup to use for restore. when absent, use date of today')
    parser.add_argument('--clean-after-restore', action='store_true',
                        help='clean backup files after restore')

    args = parser.parse_args()
    logger.info('command:' + str(args))

    if args.conf_base is None:
        args.conf_base = '/s2/mysql/backup_conf'

    if args.conf_fn is None:
        args.conf_fn = 'backup_conf'

    ports = args.ports

    if ports is None:
        ports = os.listdir(args.conf_base)

        ports = [int(x)
                 for x in ports
                 if x.isdigit()]
        ports.sort()

    cmd = args.cmd
    date_str = mysqlbackup.backup_date_str()
    rsts = {}

    def setdef(dic, key, v):
        if v is not None:
            dic[key] = v

    def load_port_conf(port):

        conf_path = '{conf_base}/{port}/{conf_fn}.yaml'.format(
            conf_base=args.conf_base,
            conf_fn=args.conf_fn,
            port=port)

        conf = mysqlbackup.load_conf_from_file(conf_path)

        setdef(conf, 'clean_after_restore', args.clean_after_restore)

        conf.setdefault('date_str', date_str)
        setdef(conf, 'date_str', args.date_str)

        return conf

    def get_latest_backup_date(port):

        conf = load_port_conf(port)

        bc = botoclient.BotoClient(
            conf['s3_host'], conf['s3_access_key'], conf['s3_secret_key'])

        backup_files = bc.boto_list(conf['s3_bucket'])

        date_ts = []

        for bf in backup_files:
            # the backup file has two formats key in s2,
            # {date_str}/{port}/{backup_tgz_des3_tail} and {port}/{date_str}/{backup_tgz_des3_tail}
            # We use the first format to get date_str
            _date_str, _port, _backup_tgz = bf['Key'].split('/')

            if _port == str(port):

                date_ts.append(timeutil.parse_to_ts(_date_str, "%Y_%m_%d"))

        if len(date_ts) != 0:
            latest_date_str = timeutil.format_ts(max(date_ts), "%Y_%m_%d")
        else:
            latest_date_str = None

        return latest_date_str

    def check_backup_file(ports):

        for port in ports:

            conf = load_port_conf(port)

            latest_date_str = get_latest_backup_date(port)

            if latest_date_str is not None:

                specified_date_ts = timeutil.parse_to_ts(
                    conf['date_str'], "%Y_%m_%d")
                latest_date_ts = timeutil.parse_to_ts(
                    latest_date_str, "%Y_%m_%d")

                if specified_date_ts > latest_date_ts:
                    raise ValueError('port: {p} backup file:{d} is not found in s2'.format(
                        p=repr(port), d=repr(conf['date_str'])))
                else:
                    setdef(conf, 'date_str', latest_date_str)
                    logger.info('port: {p} backup file:{d} is ok'.format(
                        p=repr(port), d=repr(conf['date_str'])))
            else:
                raise ValueError(
                    'port: {p} is found any backup file is s2'.format(p=repr(port)))

    def worker(port):

        try:
            rst = _worker(port)
            rsts[port] = True
            return rst
        except Exception as e:
            logger.exception(repr(e))
            return jobq.EmptyRst

    def _worker(port):

        conf = load_port_conf(port)

        mb = mysqlbackup.MysqlBackup(conf)

        if cmd == 'backup':
            mb.backup()
        elif cmd == 'setup_replication':
            mb.setup_replication()
        elif cmd == 'group_replication_bootstrap':
            mb.group_replication_bootstrap()
        elif cmd == 'group_replication_setup_channel':
            mb.group_replication_setup_channel()

        elif cmd == 'restore':
            if mb.has_data_dir():
                logger.info('data-dir presents, skip restore_from_backup')
            else:
                mb.restore_from_backup()
            mb.catchup()

        elif cmd == 'restore_from_backup':
            if mb.has_data_dir():
                logger.info('data-dir presents, skip restore_from_backup')
            else:
                mb.restore_from_backup()
        elif cmd == 'catchup':
            mb.catchup()

        elif cmd == 'make_mycnf':
            mb.make_runtime_my_cnf()

        elif cmd == 'optimize':
            mb.optimize_tables(args.db)

        elif cmd == 'query':
            rst = mb.query(args.sql)

            def _out():
                for line in rst:
                    print utfjson.dump(line)

            return _out

        elif cmd == 'replication_diff':
            rst = mb.diff_replication()
            if not args.full:
                for k, diff in rst.items():
                    if isinstance(diff, basestring):
                        continue

                    for side in ('onlyleft', 'onlyright'):
                        if diff[side]['length'] == 0:
                            del diff[side]

            if args.human:
                mapping = {
                    'onlyleft': 'OnlyIHave',
                    'onlyright': 'IDontHave'
                }
                hm = []
                for k, diff in rst.items():
                    if isinstance(diff, basestring):
                        hm.append('{k:>24}: {desc}'.format(k=k, desc=diff))
                        continue
                    for side in ('onlyleft', 'onlyright'):
                        if side not in diff:
                            continue
                        d = diff[side]
                        line = '{k:>24}: {side:>10}: {length:>10}: {rs}'.format(
                            k=k,
                            side=mapping[side],
                            length=d['length'],
                            rs=str(d['gtidset']))
                        hm.append(line)

                def _out():
                    for line in hm:
                        print line
                rst = _out
            return rst
        elif cmd == 'table_size':
            rsts = mb.table_sizes(args.db, args.sortby)

            def _out():
                print port, args.db
                for _repr, tbl_stat in rsts:
                    if args.size is not None:
                        op = args.size[0]
                        num = humannum.parseint(args.size[1:])

                        if op == '>' and tbl_stat['Data_length'] < num:
                            continue
                        if op == '<' and tbl_stat['Data_length'] > num:
                            continue
                    print _repr
            return _out

        elif cmd == 'user':
            mb.create_user(args.username,
                           args.password,
                           host=args.host,
                           privileges=[args.privilege.split(':', 1)],
                           binlog=(args.binlog == 1))
        else:
            raise ValueError('unsupported command: ' + repr(cmd))

        return jobq.EmptyRst

    def output(rst):
        if callable(rst):
            rst()
        else:
            print json.dumps(rst, indent=2)

    need_check_backup_cmds = ['check_backup_file',
                              'restore_from_backup', 'restore']

    if cmd in need_check_backup_cmds:
        check_backup_file(ports)
    else:
        jm = jobq.JobManager([(worker, args.jobs),
                              (output, 1)])

        for port in ports:
            jm.put(port)

        jm.join()

        if len(rsts) == len(ports):
            sys.exit(0)
        else:
            sys.exit(1)
