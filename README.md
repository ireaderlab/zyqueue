# ZYQUEUE

zyqueue是一个专注于实时处理任务队列的分布式多进程任务调度系统。它使用上与celery非常相似，通过装饰器模式将队列的生产者与消费者进行关联，使用上简单易用，同时支持gearman，redis，rabbitmq等常用队列做中间调度层。


## 项目描述

- zyqueue 目前支持使用gearman和redis及rabbitmq做为后端, 可友好扩展其他队列服务, 完成任务队列的添加和后台执行功能.
- 支持多进程启动任务执行worker, 进程数由参数确定
- 通过在执行方法上添加修饰器的方式进行任务加入队列的操作, 与celery类似


## 项目依赖

- gearman >= 2.0.2.
- rq >= 0.5.5.


## 安装

```console
python setup.py install
```


## 使用

### 任务配置

参考example内示例


### worker启动

```console
usage: zyqueue [-h] -S SERVER [-C CONNECTION] [--status] [--workers]
               [--max_workers MAX_WORKERS] [--task_file TASK_FILE] [--debug]
               [--shutdown] [--gmversion] [--queue QUEUE] [--queues]
               [--jobs JOBS] [--cancel_job CANCEL_JOB]
               [--requeue_job REQUEUE_JOB] [--requeue_all]
               [--empty_queue EMPTY_QUEUE] [--compact_queue COMPACT_QUEUE]

Zhangyue queue client.

optional arguments:
  -h, --help            show this help message and exit
  -S SERVER, --server SERVER
                        Queue Server, Redis or Gearman

Common arguments:
  -C CONNECTION, --connection CONNECTION
                        Queue server address to use for connection
  --status              Status for the queue server
  --workers             Workers for the queue server
  --max_workers MAX_WORKERS
                        The maximum number of queue worker
  --task_file TASK_FILE
                        The queue task file
  --debug               Debug log level

Gearman command arguments:
  --shutdown            Shutdown gearman server
  --gmversion           The version number of the gearman server

RQ command arguments:
  --queue QUEUE         RQ queue info
  --queues              All RQ queues infos
  --jobs JOBS           All RQ jobs infos of one queue
  --cancel_job CANCEL_JOB
                        Cancels the job with the given job ID
  --requeue_job REQUEUE_JOB
                        Requeues the job with the given job ID
  --requeue_all         Requeue all failed jobs
  --empty_queue EMPTY_QUEUE
                        Empty given queues.
  --compact_queue COMPACT_QUEUE
                        Compact given queues.
```

Worker Server启动:

```console
zyqueue -S redis|gearman|rabbitmq -C redis_url|gearman_url --max_workers 5 --debug --task_file TASK_FILE
```


## 版本变更

- v0.0.1
