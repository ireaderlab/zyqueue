#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright (c) 2014,掌阅科技
All rights reserved.

摘    要: queue_job.py
创 建 者: ZengDuju
创建日期: 2015-10-10
'''
from queue_task import queue_task_redis, queue_task_gearman, queue_task_rabbitmq


redis_url = "redis://192.168.6.184:6389"
gearman_host = "192.168.6.7:18888"
job_data = {'test1': 'redis', 'test2': 'gearman'}

for i in range(1):
    job_data['num'] = i
    # queue_task_test.submit(server='redis', connection=redis_url, job_data=job_data, queue='default')
    # queue_task_test.submit(server='redis', connection=redis_url, job_data=job_data, queue='high')
    # queue_task_test.submit(server='redis', connection=redis_url, job_data=job_data, queue='low')
    # queue_task_test.submit(server='gearman', connection=gearman_host, job_data=job_data)
    # queue_task_test.submit(server='rabbitmq', connection='192.168.6.7', job_data=job_data, exchange='zyqueue_rmq', exchange_type='direct', routing_keys='route1')

    queue_task_redis.submit(job_data=job_data, queue='default')
    queue_task_redis.submit(job_data=job_data, queue='high')
    queue_task_redis.submit(job_data=job_data, queue='low')
    # queue_task_gearman.submit(job_data=job_data)
    queue_task_rabbitmq.submit(job_data=job_data, exchange='zyqueue_rmq', exchange_type='direct', routing_keys='route1')
