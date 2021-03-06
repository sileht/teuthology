import argparse
import copy
import errno
import itertools
import logging
import os
import subprocess
import sys
import time
import yaml

from teuthology import misc as teuthology
from teuthology import safepath

log = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="""
Run a suite of ceph integration tests.

A suite is a set of collections.

A collection is a directory containing facets.

A facet is a directory containing config snippets.

Running a collection means running teuthology for every configuration
combination generated by taking one config snippet from each facet.

Any config files passed on the command line will be used for every
combination, and will override anything in the suite.
""")
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=None,
        help='be more verbose',
        )
    parser.add_argument(
        '--name',
        help='name for this suite',
        required=True,
        )
    parser.add_argument(
        '--collections',
        metavar='DIR',
        nargs='+',
        required=True,
        help='the collections to run',
        )
    parser.add_argument(
        '--owner',
        help='job owner',
        )
    parser.add_argument(
        '--email',
        help='address to email test failures to',
        )
    parser.add_argument(
        '--timeout',
        help='how many seconds to wait for jobs to finish before emailing results',
        )
    parser.add_argument(
        '-n', '--num',
        default=1,
        type=int,
        help='number of times to run/queue each job'
        )
    parser.add_argument(
        'config',
        metavar='CONFFILE',
        nargs='*',
        default=[],
        help='config file to read',
        )

    args = parser.parse_args()

    loglevel = logging.INFO
    if args.verbose:
        loglevel = logging.DEBUG

    logging.basicConfig(
        level=loglevel,
        )

    base_arg = [
        os.path.join(os.path.dirname(sys.argv[0]), 'teuthology-schedule'),
        '--name', args.name,
        '--num', str(args.num),
        ]
    if args.verbose:
        base_arg.append('-v')
    if args.owner:
        base_arg.extend(['--owner', args.owner])

    for collection in args.collections:
        if not os.path.isdir(collection):
            print >>sys.stderr, 'Collection %s is not a directory' % collection
            sys.exit(1)

    collections = [
        (collection,
         os.path.basename(safepath.munge(collection)))
        for collection in args.collections
        ]

    for collection, collection_name in sorted(collections):
        log.info('Collection %s in %s' % (collection_name, collection))
        facets = [
            f for f in sorted(os.listdir(collection))
            if not f.startswith('.')
            and os.path.isdir(os.path.join(collection, f))
            ]
        facet_configs = (
            [(f, name, os.path.join(collection, f, name))
             for name in sorted(os.listdir(os.path.join(collection, f)))
             if not name.startswith('.')
             and name.endswith('.yaml')
             ]
            for f in facets
            )
        for configs in itertools.product(*facet_configs):
            description = 'collection:%s ' % (collection_name);
            description += ' '.join('{facet}:{name}'.format(
                    facet=facet, name=name)
                                 for facet, name, path in configs)
            log.info(
                'Running teuthology-schedule with facets %s', description
                )
            arg = copy.deepcopy(base_arg)
            arg.extend([
                    '--description', description,
                    '--',
                    ])
            arg.extend(args.config)
            arg.extend(path for facet, name, path in configs)
            subprocess.check_call(
                args=arg,
                )

    arg = copy.deepcopy(base_arg)
    arg.append('--last-in-suite')
    if args.email:
        arg.extend(['--email', args.email])
    if args.timeout:
        arg.extend(['--timeout', args.timeout])
    subprocess.check_call(
        args=arg,
        )

def ls():
    parser = argparse.ArgumentParser(description='List teuthology job results')
    parser.add_argument(
        '--archive-dir',
        metavar='DIR',
        help='path under which to archive results',
        required=True,
        )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=False,
        help='show reasons tests failed',
        )
    args = parser.parse_args()

    for j in sorted(os.listdir(args.archive_dir)):
        job_dir = os.path.join(args.archive_dir, j)
        if j.startswith('.') or not os.path.isdir(job_dir):
            continue

        summary = {}
        try:
            with file(os.path.join(job_dir, 'summary.yaml')) as f:
                g = yaml.safe_load_all(f)
                for new in g:
                    summary.update(new)
        except IOError, e:
            if e.errno == errno.ENOENT:
                print '%s      ' % j,

                # pid
                try:
                    pidfile = os.path.join(job_dir, 'pid')
                    found = False
                    if os.path.isfile(pidfile):
                        pid = open(pidfile, 'r').read()
                        if os.path.isdir("/proc/%s" % pid):
                            cmdline = open('/proc/%s/cmdline' % pid, 'r').read()
                            if cmdline.find(args.archive_dir) >= 0:
                                print '(pid %s)' % pid,
                                found = True
                    if not found:
                        print '(no process or summary.yaml)',
                    # tail
                    tail = os.popen(
                        'tail -1 %s/%s/teuthology.log' % (args.archive_dir, j)
                        ).read().rstrip()
                    print tail,
                except IOError, e:
                    continue
                print ''
                continue
            else:
                raise

        print "{job} {success} {owner} {desc} {duration}s".format(
            job=j,
            owner=summary.get('owner', '-'),
            desc=summary.get('description', '-'),
            success='pass' if summary.get('success', False) else 'FAIL',
            duration=int(summary.get('duration', 0)),
            )
        if args.verbose and 'failure_reason' in summary:
            print '    {reason}'.format(reason=summary['failure_reason'])

def generate_coverage(args):
    log.info('starting coverage generation')
    subprocess.Popen(
        args=[
            os.path.join(os.path.dirname(sys.argv[0]), 'teuthology-coverage'),
            '-v',
            '-o',
            os.path.join(args.teuthology_config['coverage_output_dir'], args.name),
            '--html-output',
            os.path.join(args.teuthology_config['coverage_html_dir'], args.name),
            '--cov-tools-dir',
            args.teuthology_config['coverage_tools_dir'],
            args.archive_dir,
            ],
        )

def email_results(subject, from_, to, body):
    log.info('Sending results to {to}: {body}'.format(to=to, body=body))
    import smtplib
    from email.mime.text import MIMEText
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = from_
    msg['To'] = to
    log.debug('sending email %s', msg.as_string())
    smtp = smtplib.SMTP('localhost')
    smtp.sendmail(msg['From'], [msg['To']], msg.as_string())
    smtp.quit()

def results():
    parser = argparse.ArgumentParser(description='Email teuthology suite results')
    parser.add_argument(
        '--email',
        help='address to email test failures to',
        )
    parser.add_argument(
        '--timeout',
        help='how many seconds to wait for all tests to finish (default no wait)',
        type=int,
        default=0,
        )
    parser.add_argument(
        '--archive-dir',
        metavar='DIR',
        help='path under which results for the suite are stored',
        required=True,
        )
    parser.add_argument(
        '--name',
        help='name of the suite',
        required=True,
        )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=False,
        help='be more verbose',
        )
    args = parser.parse_args()

    loglevel = logging.INFO
    if args.verbose:
        loglevel = logging.DEBUG

    logging.basicConfig(
        level=loglevel,
        )

    teuthology.read_config(args)

    handler = logging.FileHandler(
        filename=os.path.join(args.archive_dir, 'results.log'),
        )
    formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d %(levelname)s:%(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        )
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)

    try:
        _results(args)
    except:
        log.exception('error generating results')
        raise

def _results(args):
    running_tests = [
        f for f in sorted(os.listdir(args.archive_dir))
        if not f.startswith('.')
        and os.path.isdir(os.path.join(args.archive_dir, f))
        and not os.path.exists(os.path.join(args.archive_dir, f, 'summary.yaml'))
        ]
    starttime = time.time()
    log.info('Waiting up to %d seconds for tests to finish...', args.timeout)
    while running_tests and args.timeout > 0:
        if os.path.exists(os.path.join(
                args.archive_dir,
                running_tests[-1], 'summary.yaml')):
            running_tests.pop()
        else:
            if time.time() - starttime > args.timeout:
                log.warn('test(s) did not finish before timeout of %d seconds',
                         args.timeout)
                break
            time.sleep(10)
    log.info('Tests finished! gathering results...')

    descriptions = []
    failures = []
    num_failures = 0
    unfinished = []
    passed = []
    all_jobs = sorted(os.listdir(args.archive_dir))
    for j in all_jobs:
        job_dir = os.path.join(args.archive_dir, j)
        if j.startswith('.') or not os.path.isdir(job_dir):
            continue
        summary_fn = os.path.join(job_dir, 'summary.yaml')
        if not os.path.exists(summary_fn):
            unfinished.append(j)
            continue
        summary = {}
        with file(summary_fn) as f:
            g = yaml.safe_load_all(f)
            for new in g:
                summary.update(new)
        desc = '{test}: ({duration}s) {desc}'.format(
            duration=int(summary.get('duration', 0)),
            desc=summary['description'],
            test=j,
            )
        descriptions.append(desc)
        if summary['success']:
            passed.append(desc)
        else:
            failures.append(desc)
            num_failures += 1
            if 'failure_reason' in summary:
                failures.append('    {reason}'.format(
                        reason=summary['failure_reason'],
                        ))

    if failures or unfinished:
        subject = ('{num_failed} failed, {num_hung} possibly hung, '
                   'and {num_passed} passed tests in {suite}'.format(
                num_failed=num_failures,
                num_hung=len(unfinished),
                num_passed=len(passed),
                suite=args.name,
                ))
        body = """
The following tests failed:

{failures}

These tests may be hung (did not finish in {timeout} seconds after the last test in the suite):
{unfinished}

These tests passed:
{passed}""".format(
            failures='\n'.join(failures),
            unfinished='\n'.join(unfinished),
            passed='\n'.join(passed),
            timeout=args.timeout,
            )
    else:
        subject = 'All tests passed in {suite}!'.format(suite=args.name)
        body = '\n'.join(descriptions)

    try:
        if args.email:
            email_results(
                subject=subject,
                from_=args.teuthology_config['results_sending_email'],
                to=args.email,
                body=body,
                )
    finally:
        generate_coverage(args)
