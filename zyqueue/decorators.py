#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=unused-argument

'''
Copyright (c) 2014,掌阅科技
All rights reserved.

摘    要: decorators.py
创 建 者: ZengDuju
创建日期: 2015-10-09
'''
import os
import sys
import logging
import traceback
from functools import wraps

import gearman
from gearman.constants import PRIORITY_NONE
from gearman.errors import ExceededConnectionAttempts, ServerUnavailable
from rq import Queue
from redis import Redis
import ujson as json
import pika

from .utils import JSONDataEncoder, Storage
from .register import Register


class Task(object):

    """gearman任务对象封装
    Attributes:
        task: 任务名字
        callback: 任务回调函数
        verbose: 是否输出详细错误日志
    """

    def __init__(self, task, callback,
                 exchange='',
                 exchange_type='',
                 queue='',
                 routing_keys='',
                 verbose=False):
        """初始化操作
        """
        self.task = task
        self.callback = callback
        self.verbose = verbose

        # rabbitmq 任务专用参数
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue = queue
        self.routing_keys = routing_keys

    def __call__(self, worker, job):
        try:
            return self.callback(worker, job)
        except Exception, e:
            if self.verbose:
                logging.error('WORKER FAILED:  %s, %s\n%s',
                              self.task,
                              e,
                              traceback.format_exc())
            worker.shutdown()  # 关闭worker使任务重新回到队列中
            raise


class GearmanJobDataTypeError(Exception):
    """错误异常封装
    """

    def __init__(self, errmsg):
        self.errmsg = errmsg

    def __str__(self):
        return "gearman task data type error,msg is :%s" % self.errmsg


class GearmanClient(gearman.GearmanClient):
    """扩展gearmanclient的编码方式
    """
    data_encoder = JSONDataEncoder


@Register('gearman', 'submit')
def submit_gearman_job(connection, job_data,
                       job_name='', timeout=None, priority=PRIORITY_NONE,
                       background=True, wait_until_complete=False,
                       max_retries=0, **kwargs):
    """提交任务至gearman队列
    """
    # 多余的未使用kwargs参数是为了兼容不同方法参数而添加的
    gearman_addr = connection.split(',')
    gearman_client = GearmanClient(gearman_addr)
    try:
        if isinstance(job_data, list):
            submit_job_data_list = [dict(task=job_name, data=data, priority=priority) for data in job_data]
            gearman_client.submit_multiple_jobs(submit_job_data_list,
                                                background=background,
                                                wait_until_complete=wait_until_complete,
                                                max_retries=max_retries,
                                                poll_timeout=timeout)
        elif isinstance(job_data, dict):
            gearman_client.submit_job(job_name,
                                      job_data,
                                      priority=priority,
                                      background=background,
                                      wait_until_complete=wait_until_complete,
                                      max_retries=max_retries,
                                      poll_timeout=timeout)
        else:
            # 任务类型错误
            raise GearmanJobDataTypeError('type can only be list and dict')
    except ServerUnavailable as e:
        error_str = "gearman 连接失败: ServerUnavailable: {}, job_name: {}".format(e, job_name)
        logging.error("gearman 连接失败. ServerUnavailable: {}, job_name: {}", exc_info=True)
    except ExceededConnectionAttempts as e:
        error_str = "gearman 任务添加失败: ExceededConnectionAttempts: {}, job_name: {}".format(e, job_name)
        logging.error(error_str, exc_info=True)


@Register('redis', 'submit')
def submit_rq_job(connection, job_data,
                  queue='default', timeout=None, func=None, **kwargs):
    """提交任务至redis队列
    """
    redis_conn = Redis.from_url(connection)
    q = Queue(queue, connection=redis_conn)
    job_data = Storage({'data': job_data})
    q.enqueue_call(func, ["placeholder", job_data], timeout=timeout)


@Register('rabbitmq', 'submit')
def submit_rmq_job(connection, job_data,
                   exchange='', exchange_type='',
                   routing_keys='',
                   **kwargs):
    """提交任务至rabbitmq队列
    """
    rmq_conn = pika.BlockingConnection(pika.ConnectionParameters(connection))
    channel = rmq_conn.channel()

    # 定义交换机
    channel.exchange_declare(exchange=exchange, type=exchange_type)
    body = json.dumps(job_data)
    #将消息发送到交换机
    for routing_key in routing_keys.split(','):
        channel.basic_publish(exchange=exchange,
                              routing_key=routing_key,
                              body=body)
    rmq_conn.close()


class QueueJob(object):
    """job添加修饰器
    """
    _tasks = []

    def __init__(self,
                 exchange='',
                 exchange_type='',
                 queue='',
                 routing_keys=''):
        """初始化
        """
        self.queue = queue
        self.exchange = exchange
        self.exchange_type= exchange_type
        self.queue = queue
        self.routing_keys = routing_keys

    def __call__(self, _func):
        """增加submit方法
        """
        job_name = _func.__name__
        self._tasks.append(Task(job_name, _func,
                                exchange=self.exchange,
                                exchange_type=self.exchange_type,
                                queue=self.queue,
                                routing_keys=self.routing_keys))

        @wraps(_func)
        def submit(server, connection, job_data, **kwargs):
            """
            Args:
                job_data支持list和dict两种结构
            """
            kwargs['func'] = _func
            kwargs['job_name'] = job_name
            if server.lower() in Register.get_reg_server():
                # 不同server共用参数加上自有参数
                Register.get_registered()[server.lower()]['submit'](connection, job_data, **kwargs)
            else:
                error_str = "queue 任务添加失败: 不支持指定server: {}, job_name: {}".format(server, job_name)
                logging.error(error_str, exc_info=True)
        _func.submit = submit
        return _func

    @classmethod
    def get_tasks(cls):
        """获取全部task
        """
        return cls._tasks


def load(task_file):
    """加载目录
    """
    path, file_name = os.path.split(task_file)
    sys.path.append(path)
    __import__(file_name.replace('.py', ''))

    # 通过修饰器获取配置
    tasks = QueueJob.get_tasks()
    return tasks
