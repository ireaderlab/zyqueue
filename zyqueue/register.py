#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright (c) 2014,掌阅科技
All rights reserved.

摘    要: register.py
创 建 者: ZengDuju
创建日期: 2015-10-15
'''
from collections import defaultdict
from functools import wraps

tree = lambda: defaultdict(tree)


class Register(object):
    """不同服务端注册器
    """
    _registered = tree()
    _servers = ['redis', 'rabbitmq', 'gearman']  # 注册的服务
    _function = ['submit', 'worker', 'admin']  # 服务需要的方法功能

    def __init__(self, server, function):
        """初始化
        """
        self.server = server
        self.function = function

    def __call__(self, _func):
        self._registered[self.server][self.function] = _func

        @wraps(_func)
        def wrapped(*args, **kwargs):
            """修饰器
            """
            return _func(*args, **kwargs)
        return wrapped

    @classmethod
    def get_registered(cls):
        """获取注册方法
        """
        return cls._registered

    @classmethod
    def get_reg_server(cls):
        """获取已注册的服务
        """
        return cls._servers
