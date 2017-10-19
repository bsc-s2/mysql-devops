#!/usr/bin/env python2
# coding: utf-8

import errno
import logging
import os
import subprocess

from pykit import daemonize

logger = logging.getLogger(__name__)


class ProcError(Exception):

    def __init__(self, returncode, out, err, cmd, arguments, options):

        super(ProcError, self).__init__(returncode,
                                        out,
                                        err,
                                        cmd,
                                        arguments,
                                        options)

        self.returncode = returncode
        self.out = out
        self.err = err
        self.command = cmd
        self.arguments = arguments
        self.options = options


def command(cmd, *arguments, **options):

    close_fds = options.get('close_fds', True)
    cwd = options.get('cwd', None)
    env = options.get('env', None)
    stdin = options.get('stdin', None)

    subproc = subprocess.Popen([cmd] + list(arguments),
                               close_fds=close_fds,
                               cwd=cwd,
                               env=env,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, )

    out, err = subproc.communicate(input=stdin)

    subproc.wait()

    return (subproc.returncode, out, err)


def command_ex(cmd, *arguments, **options):
    returncode, out, err = command(cmd, *arguments, **options)
    if returncode != 0:
        raise ProcError(returncode, out, err, cmd, arguments, options)


def shell_script(script_str, **options):
    options['stdin'] = script_str
    return command('sh', **options)


def start_daemon(cmd, target, env, *args):

    try:
        pid = os.fork()
    except OSError as e:
        logger.error(repr(e) + ' while fork')
        raise

    if pid == 0:
        d = daemonize.Daemon(close_fds=True)
        d.daemonize()
        args = list(args)
        ctype = os.environ.get('LC_CTYPE')
        if ctype is not None and 'LC_CTYPE' not in env:
            env['LC_CTYPE'] = ctype
        args.append(env)
        os.execlpe(cmd, cmd, target, *args)
    else:
        while True:
            try:
                os.waitpid(pid, 0)
                break
            except OSError as e:
                # In case we encountered an OSError due to EINTR (which is
                # caused by a SIGINT or SIGTERM signal during
                # os.waitpid()), we simply ignore it and enter the next
                # iteration of the loop, waiting for the child to end.  In
                # any other case, this is some other unexpected OS error,
                # which we don't want to catch, so we re-raise those ones.
                if e.errno != errno.EINTR:
                    raise


def _close_fds():
    try:
        max_fd = os.sysconf("SC_OPEN_MAX")
    except ValueError:
        max_fd = 65536

    for i in range(max_fd):
        try:
            os.close(i)
        except OSError:
            pass


def start_process(cmd, target, env, *args):

    try:
        pid = os.fork()
    except OSError as e:
        logger.error(repr(e) + ' while fork')
        raise

    if pid == 0:
        _close_fds()
        args = list(args)
        ctype = os.environ.get('LC_CTYPE')
        if ctype is not None and 'LC_CTYPE' not in env:
            env['LC_CTYPE'] = ctype
        args.append(env)
        os.execlpe(cmd, cmd, target, *args)
    else:
        while True:
            try:
                os.waitpid(pid, 0)
                break
            except OSError as e:
                # In case we encountered an OSError due to EINTR (which is
                # caused by a SIGINT or SIGTERM signal during
                # os.waitpid()), we simply ignore it and enter the next
                # iteration of the loop, waiting for the child to end.  In
                # any other case, this is some other unexpected OS error,
                # which we don't want to catch, so we re-raise those ones.
                if e.errno != errno.EINTR:
                    raise