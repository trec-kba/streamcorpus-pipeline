#!/usr/bin/python
'''
See help(Pipeline) for details on configuring a Pipeline.

This software is released under an MIT/X11 open source license.

Copyright 2012-2013 Diffeo, Inc.
'''
import os
import re
import sys
import json
import logging
from _getch import getch
from _task_queues import ZookeeperTaskQueue
from streamcorpus import make_stream_time

if __name__ == '__main__':
    import yaml
    import argparse
    parser = argparse.ArgumentParser(
        description='',
        usage='python -m streamcorpus.pipeline.load config.yaml')
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
        '--terminate', action='store_true', default=False,
        help='End all current task workers.')
    parser.add_argument(
        '--finish', action='store_true', default=False,
        help='Finish the queue and then stop all current task workers.')
    parser.add_argument(
        '--run-forever', action='store_true', default=False,
        help='set "mode" ot RUN_FOREVER for all task workers.')
    parser.add_argument(
        '--counts', action='store_true', default=False,
        help='Display counts for the queue.')
    parser.add_argument(
        '--detailed', action='store_true', default=False,
        help='Must be used with --counts or --list-completed; scans all tasks to count partials.')
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
        help='List all completed tasks.')
    parser.add_argument(
        '--list-not-completed', action='store_true', default=False,
        help='List all not completed tasks.')
    parser.add_argument(
        '--list-failures', action='store_true', default=False,
        help='List all tasks with "failure_log".')
    parser.add_argument(
        '--list-pending', action='store_true', default=False,
        help='List full details of all "pending" tasks.')
    parser.add_argument(
        '--list-details', 
        help='List full details of a task specified by its i_str.')
    parser.add_argument(
        '--cleanup', action='store_true', default=False,
        help='Cleans up "available" and "pending" to match task["state"].')
    parser.add_argument(
        '--reset-pending', action='store_true', default=False,
        help='Move all tasks from "pending" back to "available".')
    parser.add_argument(
        '--reset-all-to-available', action='store_true', default=False,
        help='Move all tasks back to "available".')
    parser.add_argument(
        '--reset-failures', action='store_true', default=False,
        help='Move all tasks from "completed" that have failure_log back to "available".')
    parser.add_argument(
        '--reset-wrong-output-path', metavar='REGEX', default=None,
        help='Reset to "available" tasks for which any output path matches REGEX.')
    parser.add_argument(
        '--reset-regex', action='store_true', default=False,
        help='Reset to "available" tasks for which the i_str matches --regex=REGEX.')
    parser.add_argument(
        '--list-regex', action='store_true', default=False,
        help='list tasks for which the i_str matches --regex=REGEX.')
    parser.add_argument(
        '--regex', metavar='REGEX',
        help='use with --list-regex or --reset-regex.')    
    parser.add_argument(
        '--key-prefix', default='', metavar='PREFIX', 
        help='apply command to all keys starting with PREFIX.  Works with --reset-wrong-output-path, --reset-pending, --reset-all-to-available, and --cleanup')
    parser.add_argument(
        '--clear-registered-workers', action='store_true', default=False,
        help='Delete registered worker nodes from Zookeeper... the worker might still continue running, so be careful.')
    parser.add_argument(
        '--worker-data', action='store_true', default=False,
        help='Show full data for each pending task.')
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
    parser.add_argument(
        '--verbosity', default=None,
        help='log level.')
    args = parser.parse_args()

    assert os.path.exists(args.config), '%s does not exist' % args.config
    config = yaml.load(open(args.config))

    ## setup loggers
    if args.verbosity:
        log_level = args.verbosity
    else:
        log_level = config['streamcorpus.pipeline']['log_level']
    log_level = getattr(logging, log_level)

    logger = logging.getLogger('kba')
    logger.setLevel( log_level )

    ch = logging.StreamHandler()
    ch.setLevel( log_level )
    #ch.setFormatter(formatter)
    logger.addHandler(ch)

    tq = ZookeeperTaskQueue(config['streamcorpus.pipeline']['zookeeper'])
    namespace = config['streamcorpus.pipeline']['zookeeper']['namespace']

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

    if args.clear_registered_workers:
        tq.clear_registered_workers()

    if args.terminate:
        tq.set_mode( tq.TERMINATE )

    if args.finish:
        tq.set_mode( tq.FINISH )

    if args.run_forever:
        tq.set_mode( tq.RUN_FOREVER )

    if args.cleanup:
        tq.cleanup(args.key_prefix)

    if args.reset_pending:
        tq.reset_pending(args.key_prefix)

    if args.reset_all_to_available:
        tq.reset_all_to_available(args.key_prefix)

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
        if args.detailed:
            counts = tq.counts_detailed
        else:
            counts = tq.counts
        print('\ncounts for %s at %s' % (namespace, tq.addresses))
        print(repr(make_stream_time()))
        print('\n'.join(['\t%s:\t%s' % (k, v) for k, v in counts.items()]))
        
        available_pending_completed = sum([counts[k] for k in ['available', 'pending', 'completed']])
        print('%d len(available+pending+completed)' % available_pending_completed)
        print('%d len(tasks)' % counts['tasks'])
        print('%d missing' % (counts['tasks'] - available_pending_completed))


    if args.list_details:
        data = tq.details(args.list_details)
        #print '\n'.join(data['results'])
        print data

    if args.list_completed:
        for completed in tq.completed:
            print '#%s\n%s' % (
                completed['i_str'], 
                '\n'.join(completed['results']))

            if args.detailed:
                print json.dumps(completed, indent=4, sort_keys=True)

    if args.list_pending:
        for task in tq.pending:
            if task['end_count'] == 0:
                print task['i_str']
                #print json.dumps(task, indent=4, sort_keys=True)

    if args.list_not_completed:
        for task in tq.all_tasks:
            if task['state'] != 'completed':
                print json.dumps(task, indent=4, sort_keys=True)

    if args.reset_failures or args.list_failures:
        logger.critical( 'starting' )
        for num, task in enumerate(tq.completed):
            if 'failure_log' in task and task['failure_log']:
                if args.reset_failures:
                    logger.critical('reseting %s because %s' % (task['i_str'], task['failure_log']))
                    num = tq.push(task['i_str'], redo=True)
                    assert num == 1
                elif args.list_failures:
                    logger.critical(json.dumps(task, indent=4, sort_keys=True))
                else:
                    raise Exception('must be either --list-failures or --reset-failures')

            if num % 100 == 0:
                logger.critical( '%d finished' % num )

    if args.reset_regex or args.list_regex:
        logger.critical( 'starting' )
        for num, task in enumerate(tq.completed):
            if re.match(args.regex, task['i_str']):
                if args.reset_regex:
                    logger.critical('reseting %s' % task['i_str'])
                    num = tq.push(task['i_str'], redo=True)
                    assert num == 1
                elif args.list_regex:
                    logger.critical(json.dumps(task, indent=4, sort_keys=True))
                else:
                    raise Exception('must be either --list-regex or --reset-regex')

            if num % 100 == 0:
                logger.critical( '%d finished' % num )

    if args.reset_wrong_output_path:

        for num, (task_key, task) in enumerate(tq.get_tasks_with_prefix(args.key_prefix)):
            should_reset = False
            for o_path in task['results']:
                if re.search(args.reset_wrong_output_path, o_path):
                    should_reset = True
                    break

            if should_reset:
                for result in task['results']:
                    logger.critical('abandoning output result: %s' % result)

                num = tq.push(task['i_str'], redo=True)
                assert num == 1

    if args.worker_data:
        for data in tq.pending:
            print json.dumps(data, indent=4, sort_keys=True)
