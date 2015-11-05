# [ZyQueue 1.0.0 文档](http://techblog.ireader.com.cn/blog/zyqueue/)

任务队列

---

## ZyQueue简介 ##
zyqueue是一个专注于实时处理任务队列的分布式多进程任务调度系统。它使用上与celery非常相似，通过装饰器模式将队列的生产者与消费者进行关联，使用上简单易用，同时支持gearman，redis，rabbitmq等常用队列做中间调度层。且易于扩展其他队列服务做为中间调度层。


简单：ZyQueue易于使用和维护，并且不需要使用配置文件。下面是一个最简单的任务应用实现：

```python
@QueueJob(server='gearman', connection='192.168.6.7:18888')
def queue_task(worker, job):
"""gearman tast execute
"""
    print "task execute success! job data: {}s"。format(','.join(["{}: {}".format(
        key,
        value
    ) for key, value in job.data.iteritems()]))
    return True
```

高可用性：ZyQueue保持了中间调度层的各种高可用特性。

快速：单个 ZyQueue 进程每分钟可数以百万计的任务至队列。

灵活：ZyQueue可以使用任意支持的队列服务作为中间调度层，并可轻松扩展定制新的服务作为中间调度层。

ZyQueue目前支持
中间调度层：gearman，redis，rabbitmq
多进程：父子进程模式

依赖包：gearman，rq，redis，arrow

## ZyQueue初步 ##

### 安装 ###
    python setup.py install

### 应用 ###
ZyQueue使用简单。首先我们会需要创建一个独立模块来编写我们所有的队列任务。
创建tasks.py
```python
from zyqueue import QueueJob

@QueueJob(server='gearman', connection='192.168.6.7:18888')
def queue_task(worker, job):
"""gearman tast execute
"""
    print "task execute success! job data: %s" % (','.join(["{}: {}".format(
        key,
        value
    ) for key, value in job.data.iteritems()]))
    return True
```
修饰器的server参数确定使用哪种中间调度层，connection指定中间调度层的URL，如此便定义好了一个任务，称为queue_task。打印出任务数据。任务的参数被固定为worker和job，其中job为包装后的任务数据，通过job.data可以取出任务数据字典。

### 运行ZyQueue职程服务器 ###
```
zyqueue --max_workers 1 --task_file tasks.py
```
max_worker指定启动多少个进程来监听定义的任务，task_file指定定义好的任务模块。
想要查看完整的命令行参数列表，执行命令：
```
zyqueue --help
```
### 提交任务 ###
可以调用submit()方法来提交任务：
```python
from tasks import queue_task

gearman_host = "192.168.6.7:18888"
job_data = {'test1': 'redis', 'test2': 'gearman'}
queue_task.submit(job_data=job_data)
```
这个任务将有之前运行的职程服务器执行，并且可以查看职程的控制台输出来验证。


### 中间调度层 ###
#### Gearman ####
需要安装和启动Gearman服务。可以通过：
```
zyqueue -S gearman -C 192.168.6.7:18888 --status
zyqueue -S gearman -C 192.168.6.7:18888 --workers
```
查看Gearman服务状态信息。任务定义和提交任务见上两节。
#### Redis ####
需安装和启动Redis服务。可以通过：
```
zyqueue -S redis -C redis://192.168.6.184:6389 --workers
```
查看Redis服务状态信息。任务定义和提交与Gearman一致。
#### RabbitMQ ####
需要安装和启动RabbitMQ。
任务定义：
```python
@QueueJob(server='rabbitmq', connection='192.168.6.7', exchange='zyqueue_rmq',
          exchange_type='direct', queue="zyqueue_test", routing_keys='route1')
def queue_task(worker, job):
"""gearman tast execute
"""
    print "task execute success! job data: %s" % (','.join(["{}: {}".format(
        key,
        value
    ) for key, value in job.data.iteritems()]))
    return True
```
任务提交：
```python
queue_task.submit(server='rabbitmq', connection='192.168.6.7', job_data=job_data,
                  exchange='zyqueue_rmq', exchange_type='direct', routing_keys='route1')
```
与Gearman和Redis中间调度层不同的是RabbitMQ需要加上自有的一些队列参数，以完善RabbitMQ队列的功能使用。
