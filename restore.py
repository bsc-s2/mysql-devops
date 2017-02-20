#!/usr/bin/env python
# coding: utf-8

import logging
import sys

import conf
import mysqlbackup

if __name__ == "__main__":

    arg = sys.argv[1]
    # arg is one of from-19, from-20, from-29

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    restore_conf = conf.restore_conf

    ports = conf.ports_by_host['from-19']

    for port in ports:
        conf = restore_conf.copy()
        conf.update({
            'date_str': '2016_10_20',
            'port': port,

            # we use instance_is=2, although in the backup, 'server-id' in my.cnf is still 1xxxx.
            # We will update my.cnf with ansible books after data is
            # restored.
            'instance_id': '2',
        })

        mb = mysqlbackup.MysqlBackup(conf)
        mb.restore()
