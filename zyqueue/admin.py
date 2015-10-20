#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright (c) 2014,掌阅科技
All rights reserved.

摘    要: admin.py
创 建 者: ZengDuju
创建日期: 2015-10-13
'''
import arrow

from gearman import GearmanAdminClient
from redis import Redis
from rq import (Queue, Worker, cancel_job, get_failed_queue, pop_connection,
                push_connection, requeue_job)

from .utils import pretty_table
from .register import Register


def queue_admin(options):
    """队列管理, 信息查询
    """
    return Register.get_registered()[options.server.lower()]['admin'](options)


@Register('gearman', 'admin')
def get_gminfo(options):
    """获取gearman服务信息
    """
    gm_client = GearmanAdminClient([options.connection])
    if options.status:
        status = gm_client.get_status()
        return pretty_table(['task', 'queued', 'running', 'workers'], status)
    elif options.workers:
        workers = gm_client.get_workers()
        return pretty_table(['ip', 'client_id', 'tasks', 'file_descriptor'],
                            workers)
    elif options.shutdown:
        shutdown_info = gm_client.send_shutdown()
        return shutdown_info
    elif options.gmversion:
        version = gm_client.get_version()
        return version


@Register('redis', 'admin')
def get_rqinfo(options):
    """获取rq队列信息
    """
    redis_conn = Redis.from_url(options.connection)
    push_connection(redis_conn)
    # RQ队列信息获取操作
    if options.status:
        workers = Worker.all()
        queues = Queue.all()
        return workers, queues
    if options.queue:
        queue = Queue(options.queue)
        return queue
    if options.cancel_job:
        cancel_job(options.cancel_job)
        return 'OK'
    if options.requeue_job:
        requeue_job(options.requeue_job)
        return 'OK'
    if options.requeue_all:
        return requeue_all()
    if options.empty_queue:
        empty_queue(options.empty_queue)
        return 'OK'
    if options.compact_queue:
        compact_queue(options.compact_queue)
        return 'OK'
    if options.queues:
        return list_queues()
    if options.jobs:
        return list_jobs(options.jobs)
    if options.workers:
        return list_workers()
    pop_connection()


def requeue_all():
    """rq admin method
    """
    fq = get_failed_queue()
    job_ids = fq.job_ids
    count = len(job_ids)
    for job_id in job_ids:
        requeue_job(job_id)
    return dict(status='OK', count=count)


def empty_queue(queue_name):
    """rq admin method
    """
    q = Queue(queue_name)
    q.empty()
    return dict(status='OK')


def compact_queue(queue_name):
    """rq admin method
    """
    q = Queue(queue_name)
    q.compact()
    return dict(status='OK')


def list_queues():
    """rq admin method
    """
    queues = serialize_queues(sorted(Queue.all()))
    return pretty_table(['name', 'count'], queues)


def list_jobs(queue_name):
    """rq admin method
    """
    queue = Queue(queue_name)
    # total_items = queue.count
    jobs = [serialize_job(job) for job in queue.get_jobs()]
    return pretty_table(['origin',
                         'id',
                         'description',
                         'enqueued_at',
                         'created_at',
                         'ended_at',
                         'result',
                         'exc_info'], jobs)


def list_workers():
    """rq admin method
    """

    def serialize_queue_names(worker):
        """rq admin method
        """
        return [q.name for q in worker.queues]

    workers = [dict(name=worker.name,
                    queues=serialize_queue_names(worker),
                    state=worker.get_state())
               for worker in Worker.all()]
    return pretty_table(['state', 'queues', 'name'], workers)


def serialize_queues(queues):
    """rq admin method
    """
    return [dict(name=q.name,
                 count=q.count)
            for q in queues]


def serialize_date(dt):
    """rq admin method
    """
    if dt is None:
        return None
    return arrow.get(dt).to('UTC').datetime.isoformat()


def serialize_job(job):
    """rq admin method
    """
    return dict(
        id=job.id,
        created_at=serialize_date(job.created_at),
        enqueued_at=serialize_date(job.enqueued_at),
        ended_at=serialize_date(job.ended_at),
        origin=job.origin,
        result=job._result,
        exc_info=job.exc_info,
        description=job.description)
