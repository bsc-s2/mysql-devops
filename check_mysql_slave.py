#!/usr/bin/env python2
# coding: utf-8

import sys

import MySQLdb

from pykit import mysqlconnpool


def get_slave_status(port):

    pool = mysqlconnpool.make({
        'unix_socket': '/tmp/mysql-{p}.sock'.format(p=port)
    })

    try:

        slave_status = pool.query('show slave status')

    except (MySQLdb.OperationalError, MySQLdb.InternalError):

        return None

    return slave_status


def check_instance_role(slave_status):

    if len(slave_status) == 0:
        return 'Master'
    else:
        return "Slaveof-{n}-master".format(n=len(slave_status))


def check_slave_is_healthy(slave_status):

    health_value = 0

    if len(slave_status) == 0:
        return 0

    for st in slave_status:
        if st['Slave_SQL_Running'] == 'Yes':
            if st['Slave_IO_Running'] == 'Yes':
                health_value += 0
            else:
                health_value += 1
        else:
            if st['Slave_IO_Running'] == 'Yes':
                health_value += 10
            else:
                health_value += 100

    return health_value


def check_behind_master(slave_status):

    behind_sec = 0

    if len(slave_status) == 0:
        return 0

    for st in slave_status:
        behind_sec += int(st['Seconds_Behind_Master'])

    return behind_sec


if __name__ == "__main__":

    port, metric = sys.argv[1:3]

    slave_status = get_slave_status(port)

    if slave_status is None:
        print -1
        sys.exit(-1)

    if metric == 'role':

        print check_instance_role(slave_status)

    elif metric == 'slave_health':

        print check_slave_is_healthy(slave_status)

    elif metric == 'slave_behind':

        print check_behind_master(slave_status)

    else:

        print -2
