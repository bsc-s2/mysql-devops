#!/usr/bin/env python2
# coding: utf-8

import sys
import argparse
import logging

from pykit import logutil
from pykit import jobq

import mysqlbackup

logger = logging.getLogger(__name__)

if __name__ == "__main__":

    # config root logger
    logging.basicConfig(stream=sys.stdout, level=logging.WARNING)
    # config logger for this module
    logutil.make_logger(base_dir='/tmp', level=logging.DEBUG)

    parser = argparse.ArgumentParser(description='run commands for one or more ports concurrenty')

    parser.add_argument('--conf-base', type=str, required=True,  help='base path to config file')
    parser.add_argument('--jobs',      type=int, required=False, default=1, help='nr of threads to run')
    parser.add_argument('--cmd',       type=str, required=True,  choices=['backup', 'restore', 'catchup_binlog', 'setup_replication'], help='command to run')
    parser.add_argument('--ports',     type=int, required=True,  nargs='+', help='ports to run "cmd" on')

    parser.add_argument('--date-str',            action='store', help='date in form 2017_01_01. It is used in backup file name, or to specify which backup to use for restore. when absent, use date of today')
    parser.add_argument('--clean-after-restore', action='store_true', help='clean backup files after restore')

    args = parser.parse_args()
    logger.info('command:' + str(args))

    cmd = args.cmd
    date_str = mysqlbackup.backup_date_str()
    rsts = {}

    def setdef(dic, key, v):
        if v is not None:
            dic[key] = v

    def worker(port):

        try:
            _worker(port)
            rsts[port] = True
        except Exception as e:
            logger.exception(repr(e))

    def _worker(port):

        conf_path = '{conf_base}/{port}/backup_conf.yaml'.format(
                conf_base=args.conf_base, port=port)

        conf = mysqlbackup.load_conf_from_file(conf_path)

        setdef(conf, 'date_str', args.date_str)
        setdef(conf, 'clean_after_restore', args.clean_after_restore)

        conf.setdefault('date_str', date_str)

        mb = mysqlbackup.MysqlBackup(conf)

        if cmd == 'backup':
            mb.backup()
        elif cmd == 'setup_replication':
            mb.setup_replication()
        elif cmd == 'restore_from_backup':
            mb.restore_from_backup()
        elif cmd == 'catchup':
            mb.catchup()
        else:
            raise ValueError('unsupported command: ' + repr(cmd))

    jm = jobq.JobManager([(worker, args.jobs)])

    for port in args.ports:
        jm.put(port)

    jm.join()

    if len(rsts) == len(args.ports):
        sys.exit(0)
    else:
        sys.exit(1)


