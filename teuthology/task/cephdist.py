from cStringIO import StringIO

import argparse
import contextlib
import errno
import logging
import os
import shutil
import subprocess
import sys
import tempfile

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.parallel import parallel
from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def ceph_log(ctx, config):
    log.info('Creating log directories...')
    run.wait(
        ctx.cluster.run(
            args=[
                'install', '-d', '-m0755', '--',
                '/tmp/cephtest/archive/log',
                '/tmp/cephtest/archive/log/valgrind',
                '/tmp/cephtest/archive/profiling-logger',
                ],
            wait=False,
            )
        )

    try:
        yield
    finally:

        if ctx.archive is not None:
            log.info('Compressing logs...')
            run.wait(
                ctx.cluster.run(
                    args=[
                        'find',
                        '/tmp/cephtest/archive/log',
                        '-name',
                        '*.log',
                        '-print0',
                        run.Raw('|'),
                        'xargs',
                        '-0',
                        '--no-run-if-empty',
                        '--',
                        'gzip',
                        '--',
                        ],
                    wait=False,
                    ),
                )

            # log file transfer is done by the generic archive data
            # handling

@contextlib.contextmanager
def ship_utilities(ctx, config):
    assert config is None
    FILES = ['daemon-helper', 'enable-coredump', 'chdir-coredump',
             'valgrind.supp']

    for filename in FILES:
        log.info('Shipping %r...', filename)
        src = os.path.join(os.path.dirname(__file__), filename)
        dst = os.path.join('/tmp/cephtest', filename)
        with file(src, 'rb') as f:
            for rem in ctx.cluster.remotes.iterkeys():
                teuthology.write_file(
                    remote=rem,
                    path=dst,
                    data=f,
                    )
                f.seek(0)
                rem.run(
                    args=[
                        'chmod',
                        'a=rx',
                        '--',
                        dst,
                        ],
                    )
    FILES_BINARY = [ 'ceph', 'ceph-clsinfo', 'ceph-debugpack', 'ceph-fuse', 'ceph-mon', 'ceph-rbdnamer', 'ceph-syn', 'ceph-authtool', 'ceph-conf', 'ceph-dencoder', 'ceph-mds', 'ceph-osd', 'ceph-run', 'cephfs', 'rados', 'radosgw-admin', 'radosgw', 'rbd' ]
    for filename in FILES_BINARY:
        for rem in ctx.cluster.remotes.iterkeys():
            rem.run(args=["mkdir", "-p", "/tmp/cephtest/binary/usr/local/bin/"])
            rem.run(args=["ln", "-s", "/usr/bin/%s"%filename, "/tmp/cephtest/binary/usr/local/bin/%s"%filename])

    dst = "/tmp/cephtest/binary/usr/local/bin/ceph-coverage"
    for rem in ctx.cluster.remotes.iterkeys():
        f = StringIO()
        f.write("""
#!/bin/bash
shift
exec "$@"
""")
        f.seek(0)
        teuthology.write_file(
            remote=rem,
            path=dst,
            data=f,
            )
        f.close()
        rem.run(
            args=[
                'chmod',
                'a=rx',
                '--',
                dst,
                ],
            )
    for rem in ctx.cluster.remotes.iterkeys():
        rem.run(args=[ "ln", "-s", "/etc/ceph/ceph.conf", "/tmp/cephtest/ceph.conf" ])

    try:
        yield
    finally:
        log.info('Removing shipped files: %s...', ' '.join(FILES))
        filenames = [
            os.path.join('/tmp/cephtest', filename)
            for filename in FILES
            ] + [
            os.path.join('/tmp/cephtest/binary/usr/local/bin/', filename)
            for filename in FILES_BINARY
            ] + [ '/tmp/cephtest/binary/usr/local/bin/ceph-coverage','/tmp/cephtest/ceph.conf' ]
            
        run.wait(
            ctx.cluster.run(
                args=[
                    'rm',
                    '-rf',
                    '--',
                    ] + list(filenames),
                wait=False,
                ),
            )

def healthy(ctx, config):
    log.info('Waiting until ceph is healthy...')
    firstmon = teuthology.get_first_mon(ctx, config)
    (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()
    teuthology.wait_until_osds_up(
        cluster=ctx.cluster,
        remote=mon0_remote
        )
    teuthology.wait_until_healthy(
        remote=mon0_remote,
        )


@contextlib.contextmanager
def task(ctx, config):
    """
    Set up and tear down a Ceph cluster.

    For example::

        tasks:
        - dummyceph:
        - interactive:

    By default, the cluster log is checked for errors and warnings,
    and the run marked failed if any appear. You can ignore log
    entries by giving a list of egrep compatible regexes, i.e.:

        tasks:
        - ceph:
            log-whitelist: ['foo.*bar', 'bad message']

    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "task ceph only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph', {}))

    # Flavor tells us what gitbuilder to fetch the prebuilt software
    # from. It's a combination of possible keywords, in a specific
    # order, joined by dashes. It is used as a URL path name. If a
    # match is not found, the teuthology run fails. This is ugly,
    # and should be cleaned up at some point.

    dist = 'precise'
    format = 'tarball'
    arch = 'x86_64'
    flavor = 'basic'

    # First element: controlled by user (or not there, by default):
    # used to choose the right distribution, e.g. "oneiric".
    flavor = config.get('flavor', 'basic')

    if config.get('path'):
        # local dir precludes any other flavors
        flavor = 'local'
    else:
        if config.get('valgrind'):
            log.info('Using notcmalloc flavor and running some daemons under valgrind')
            flavor = 'notcmalloc'
        else:
            if config.get('coverage'):
                log.info('Recording coverage for this run.')
                flavor = 'gcov'

    ctx.summary['flavor'] = flavor
    
    if config.get('coverage'):
        coverage_dir = '/tmp/cephtest/archive/coverage'
        log.info('Creating coverage directory...')
        run.wait(
            ctx.cluster.run(
                args=[
                    'install', '-d', '-m0755', '--',
                    coverage_dir,
                    ],
                wait=False,
                )
            )

    with contextutil.nested(
        lambda: ship_utilities(ctx=ctx, config=None),
        ):
        healthy(ctx=ctx, config=None)
        yield
