#!/usr/bin/env python2
# coding: utf-8

import logging
import mysqlbackup

import argparse

logger = logging.getLogger(__name__)

if __name__ == "__main__":

    mysqlbackup.init_logger()

    parser = argparse.ArgumentParser(description='mysql backup cron')

    # TODO use conf-base
    parser.add_argument('--conf-base', action='store', help='base path to config file')
    parser.add_argument('ports', type=int, nargs='+', help='ports to backup')

    args = parser.parse_args()

    ports = args.ports

    for port in ports:

        conf_path = '/s2/mysql/backup_conf/{port}/backup_conf.yaml'.format(port=port)
        conf = mysqlbackup.load_conf_from_file(conf_path)

        conf['clean_after_restore'] = True

        mb = mysqlbackup.MysqlBackup(conf)
        mb.backup()
