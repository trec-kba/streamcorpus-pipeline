#!/usr/bin/python
'''
See help(Pipeline) for details on configuring a Pipeline.

This software is released under an MIT/X11 open source license.

Copyright 2012 Diffeo, Inc.
'''
import os
import sys
import logging
from _getch import getch
from _task_queues import ZookeeperTaskQueue

if __name__ == '__main__':
    import yaml
    import argparse
    parser = argparse.ArgumentParser(
        description='',
        usage='python -m kba.pipeline.load config.yaml')
    parser.add_argument(
        'config', metavar='config.yaml', 
        help='configuration parameters for a pipeline run')
    parser.add_argument(
        '--delete-all', action='store_true', default=False,
        dest='delete_all',
        help='Delete all data in the namespace.')
    parser.add_argument(
        '--load', 
        help='Load a file of one task string per line, defaults to stdin.')
    parser.add_argument(
        '--allow-wrong-s3', action='store_true', default=False,
        dest='allow_wrong_s3',
        help='Allow strings that start with s3:/')
    parser.add_argument(
        '--counts', action='store_true', default=False,
        help='Display counts for the three different states.')
    parser.add_argument(
        '--redo', action='store_true', default=False,
        help='Must be used with --load, and sets all ' + \
            'loaded tasks to "available" and ERASES previous state.')
    parser.add_argument(
        '--set-completed', action='store_true', default=False,
        dest='set_completed',
        help='Must be used with --load, and sets all ' + \
            'loaded tasks to "completed" and ERASES previous state.')
    parser.add_argument(
        '--list-completed', action='store_true', default=False,
        dest='list_completed',
        help='List all completed tasks.')
    parser.add_argument(
        '--cleanup', action='store_true', default=False,
        help='Cleans up "available" and "pending" to match "state" of tasks.')
    parser.add_argument(
        '--terminate', action='store_true', default=False,
        help='End all current task workers.')
    parser.add_argument(
        '--finish', action='store_true', default=False,
        help='Finish the queue and then stop all current task workers.')
    parser.add_argument(
        '--reset-pending', action='store_true', default=False,
        help='Move all tasks from "pending" back to "available".')
    parser.add_argument(
        '--purge', 
        help='Totally remove these tasks from the entire queue -- gone.')
    parser.add_argument(
        '--quiet', action='store_true', default=False,
        help='Do not display purging details.')
    parser.add_argument(
        '--i-meant-that', action='store_true', default=False,
        dest='i_meant_that',
        help='Enter "y" for prompt when purging.')
    args = parser.parse_args()

    assert os.path.exists(args.config), '%s does not exist' % args.config
    config = yaml.load(open(args.config))

    ## setup loggers
    log_level = getattr(logging, config['kba.pipeline']['log_level'])

    logger = logging.getLogger('kba')
    logger.setLevel( log_level )

    ch = logging.StreamHandler()
    ch.setLevel( log_level )
    #ch.setFormatter(formatter)
    logger.addHandler(ch)

    tq = ZookeeperTaskQueue(config['kba.pipeline']['zookeeper'])
    namespace = config['kba.pipeline']['zookeeper']['namespace']

    if args.delete_all:
        sys.stdout.write('Are you sure you want to delete everything in %r?  (y/N): ' \
                            % namespace)
        ch = getch()
        if ch.lower() == 'y':
            sys.stdout.write('\nDeleting ...')
            tq.delete_all()
            print('')
        else:
            print(' ... Aborting.')

    elif args.load:
        if args.load == '-':
            args.load = sys.stdin
        else:
            args.load = open(args.load)
        sys.stdout.write('Loading...')
        num = tq.push(*args.load, completed=args.set_completed, redo=args.redo, allow_wrong_s3=args.allow_wrong_s3)
        print(' %d pushed new tasks.' % num)

    if args.terminate:
        tq.set_mode( tq.TERMINATE )

    if args.finish:
        tq.set_mode( tq.FINISH )

    if args.reset_pending:
        tq.reset_pending()

    if args.cleanup:
        tq.cleanup()

    if args.purge:
        if not args.i_meant_that:
            sys.stdout.write('Are you sure you want to purge the specified strs from %r?  (y/N): ' \
                                % namespace)
            ch = getch()
            if ch.lower() != 'y':
                print(' ... Aborting.')
                sys.exit()

        if not args.quiet:
            print('\nPurging ...')

        if args.purge == '-':
            args.purge = sys.stdin
        else:
            args.purge = open(args.purge)

        count = 0
        for i_str in args.purge:
            if not args.quiet:
                print('purging: %r' % i_str.strip())
            tq.purge(i_str)
            count += 1

        print('Done purging %d strs' % count)

    if args.counts:
        counts = tq.counts
        print('\n'.join(['\t%s:\t%s' % (k, v) for k, v in counts.items()]))
        print('Total: %d' % sum([counts[k] for k in ['available', 'pending', 'completed']]))
        print('Num Tasks: %d' % counts['tasks'])

    if args.list_completed:
        for completed in tq.completed:
            print 'in:  %s\nout: %s' % (completed['i_str'], completed['result'])
