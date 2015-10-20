#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright (c) 2014,掌阅科技
All rights reserved.

摘    要: queue_task.py
创 建 者: ZengDuju
创建日期: 2015-10-10
'''
import logging

from zyqueue import QueueJob


@QueueJob(exchange='zyqueue_rmq', exchange_type='direct', queue="zyqueue_test", routing_keys='route1')
def queue_task_test(worker, job):
    """gearman tast execute
    """
    try:
        logging.info("task execute success! job data: %s" % (','.join(["{}: {}".format(key, value) for key, value in job.data.iteritems()])))
    except Exception, e:
        logging.error(msg="task execute failed! error: %s" % (e))
    return True
