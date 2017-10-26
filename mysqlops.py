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
    logutil.make_logger(base_dir='/tmp', log_name=__name__, level=logging.DEBUG)

    parser = argparse.ArgumentParser(description='run commands for one or more ports concurrenty')

    parser.add_argument('--conf-base', type=str, required=True,  help='base path to config file')
    parser.add_argument('--jobs',      type=int, required=False, default=1, help='nr of threads to run')
    parser.add_argument('--cmd',       type=str, required=True,  choices=['backup', 'restore', 'catchup_binlog', 'setup_replication'], help='command to run')
    parser.add_argument('--ports',     type=int, required=True,  nargs='+', help='ports to run "cmd" on')

    args = parser.parse_args()
    logger.info('command:' + str(args))

    cmd = args.cmd
    date_str = mysqlbackup.backup_date_str()
    rsts = {}

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
        conf.setdefault('date_str', date_str)

        mb = mysqlbackup.MysqlBackup(conf)

        if cmd == 'backup':
            mb.backup()
        elif cmd == 'setup_replication':
            mb.setup_replication()
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


