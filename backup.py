#!/usr/bin/env python2
# coding: utf-8

import argparse
import logging

import mysqlbackup

logger = logging.getLogger(__name__)

if __name__ == "__main__":

    mysqlbackup.init_logger()

    parser = argparse.ArgumentParser(description='mysql backup cron')

    parser.add_argument('--conf-base', action='store', help='base path to config file')
    parser.add_argument('ports', type=int, nargs='+', help='ports to backup')

    args = parser.parse_args()

    # example: /s2/mysql/backup_conf
    conf_base = args.conf_base
    ports = args.ports
    date_str = mysqlbackup.backup_date_str()

    for port in ports:

        conf_path = '{conf_base}/{port}/backup_conf.yaml'.format(
                conf_base=conf_base, port=port)

        conf = mysqlbackup.load_conf_from_file(conf_path)
        conf.setdefault('date_str', date_str)

        mb = mysqlbackup.MysqlBackup(conf)

        try:
            mb.backup()
        except mysqlbackup.MysqlBackupError as e:
            logger.exception(repr(e))
