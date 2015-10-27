#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=unused-argument

'''
Copyright (c) 2014,掌阅科技
All rights reserved.

摘    要: workers.py
创 建 者: ZengDuju
创建日期: 2015-10-12
'''
import os
import sys
import time
import signal
from multiprocessing import Process, Queue, cpu_count, active_children
from Queue import Empty
import logging
import ujson as json
from collections import defaultdict

import gearman
from redis import Redis
from rq import Connection
from rq import Worker as RqWorker
from rq import Queue as RqQueue
import pika

from .utils import JSONDataEncoder, Storage
from .decorators import load
from .register import Register

__all__ = ['QueueWorkerServer']
listen = ['high', 'default', 'low']

QUEUE_WORKERS = defaultdict(set)  # 记录所有进程的信息


class GearmanWorker(gearman.GearmanWorker):
    """接收数据使用JSON格式
    """
    data_encoder = JSONDataEncoder

    def after_poll(self, any_activity):
        """回调
        """
        # logging.info('callback: any_activity=%s', any_activity)
        if any_activity:
            pass
        return True


class QueueWorkerServer(object):
    """多进程启动worker
    """

    def __init__(self,
                 options,
                 use_sighandler=True,
                 verbose=False):
        self.tasks, self.brokers = load(task_file=options.task_file)
        # self.connection = options.connection
        self.verbose = verbose
        self.id_prefix = ''
        if options.max_workers:
            self.max_workers = int(options.max_workers)
        else:
            self.max_workers = cpu_count()
        if use_sighandler:
            self._setup_sighandler()

        file_name = os.path.split(options.task_file)[1]
        self.id_prefix = "{}.".format(file_name)

    def start_process(self, doneq, server, connection, process_counter):
        target_kwargs = {}
        target_kwargs['doneq'] = doneq
        target_kwargs['tasks'] = self.tasks.get((server, connection), [])
        target = Register.get_registered()[server]['worker']
        target_kwargs['connection'] = connection
        client_id = '{}.{}{}'.format(server, self.id_prefix, process_counter)
        target_kwargs['client_id'] = client_id
        proc = Process(target=target, kwargs=target_kwargs)
        proc.start()
        if self.verbose:
            logging.info("Server: %s. Num workers: %s of %s", server, process_counter, self.max_workers)
        return proc

    def serve_forever(self):
        """启动多进程服务处理队列任务
        """
        global QUEUE_WORKERS
        QUEUE_WORKERS = defaultdict(set)
        # 队列初始化
        doneq = Queue()
        # 记录创建的进程数,保证每个进程id唯一
        process_counter = 0
        try:
            while True:
                for server, connection in self.brokers:
                    while len(QUEUE_WORKERS[(server, connection)]) < self.max_workers:
                        QUEUE_WORKERS[(server, connection)].add(self.start_process(doneq, server, connection, process_counter))
                        process_counter += 1
                try:
                    r = doneq.get(True, 5)
                except Empty:
                    r = None
                if r is not None:
                    if isinstance(r, gearman.errors.ServerUnavailable):
                        if self.verbose:
                            logging.info("Reconnecting.")
                        time.sleep(2)
                    elif r is True:
                        logging.info('Normal process exit (May actually be a problem)')
                time.sleep(0.1)
                for server, connection in self.brokers:
                    QUEUE_WORKERS[(server, connection)] = set([w for w in active_children() if w in QUEUE_WORKERS[(server, connection)]])
        except KeyboardInterrupt:
            logging.error('EXIT.  RECEIVED INTERRUPT')

    @staticmethod
    def _setup_sighandler():
        """设置中断信号
        """
        signal.signal(signal.SIGINT, _interrupt_handler)
        signal.signal(signal.SIGTERM, _interrupt_handler)


def _interrupt_handler(signum, frame):
    """获取信号后响应
    """
    global QUEUE_WORKERS
    logging.info('get signal: signum=%s, frame=%s', signum, frame.f_exc_value)
    if signum in (signal.SIGTERM, signal.SIGINT):
        for workers in QUEUE_WORKERS.values():
            for worker in workers:
                if worker in active_children():
                    worker.terminate()
        sys.exit(0)
    raise KeyboardInterrupt()


@Register('gearman', 'worker')
def _gearman_worker_process(doneq, connection, tasks=None, client_id=None, verbose=False, **kwargs):
    """多进程处理器
    """
    try:
        worker_class = GearmanWorker
        try:
            # 连接
            host_list = connection.split(',')
            gm_worker = worker_class(host_list=host_list)
            if client_id:
                gm_worker.set_client_id(client_id)
            # 加载任务
            for task in tasks:
                taskname = callback = None
                if isinstance(task, dict):
                    taskname = task['task']
                    callback = task['callback']
                elif isinstance(task, (list, tuple)):
                    taskname, callback = task
                else:
                    taskname = task.task
                    callback = task
                if verbose:
                    logging.info("Registering %s task %s", client_id, taskname)
                # 注册任务和对应的回调函数
                gm_worker.register_task(taskname, callback)
            gm_worker.work()
        except gearman.errors.ServerUnavailable, e:
            # gearman服务不可用
            doneq.put(e)
            return
        doneq.put(True)
    except KeyboardInterrupt:
        # 捕获中断信号
        pass


@Register('redis', 'worker')
def _redis_worker_process(doneq, connection, client_id=None, verbose=False, **kwargs):
    '''多进程处理器, 启动RQ Worker
    '''
    try:
        worker_class = RqWorker
        # 连接
        redis_conn = Redis.from_url(connection)
        with Connection(redis_conn):
            worker = worker_class(map(RqQueue, listen))
            worker.work()
        doneq.put(True)
    except KeyboardInterrupt:
        # 捕获中断信号
        pass


@Register('rabbitmq', 'worker')
def _rabbitmq_worker_process(doneq, connection, tasks=None, client_id=None, verbose=False, **kwargs):
    """启动rabbitmq worker
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(connection))
    channel = connection.channel()
    for task in tasks:
        # taskname = task.task
        callback = task

        rabbitmq_kwargs = task.rabbitmq_kwargs or {}
        exchange = rabbitmq_kwargs.get('exchange')
        exchange_type = rabbitmq_kwargs.get('exchange_type')
        queue = rabbitmq_kwargs.get('queue')
        routing_keys = rabbitmq_kwargs.get('routing_keys')

        def rmq_cb(ch, method, properties, body):
            """rabbitmq callback函数, 收到服务端分发的消息后调用
            """
            logging.info('Received message # %s', body)
            body = json.loads(body)
            job_data = Storage({'data': body})
            callback('placeholder', job_data)  # 存在bug, 第一个参数需实现shutdown方法
            # ch.basic_ack(delivery_tag = method.delivery_tag)

        # 定义交换机
        channel.exchange_declare(exchange=exchange, type=exchange_type)
        if queue:
            queue_name = queue
            channel.queue_declare(queue=queue_name)
        else:
            # 生成临时队列
            # exclusive参数: 接收端退出时，销毁临时产生的队列
            result = channel.queue_declare(exclusive=True)
            queue_name = result.method.queue
        # 绑定队列到交换机上
        for routing_key in routing_keys.split(','):
            channel.queue_bind(exchange=exchange,
                               queue=queue_name,
                               routing_key=routing_key)

        # channel.basic_qos(prefetch_count=1)  # Worker公平调度
        channel.basic_consume(rmq_cb, queue=queue, no_ack=True)

        channel.start_consuming()
