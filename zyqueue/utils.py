#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright (c) 2014,掌阅科技
All rights reserved.

摘    要: utils.py
创 建 者: ZengDuju
创建日期: 2015-10-08
'''
import json
import decimal

from gearman import DataEncoder


def pretty_table(fields, data):
    """生成表格形式输出
    """
    widths = _compute_widths(fields, data)
    lines = []
    if len(data) == 0:
        return ""
    # Add Border
    bits = []
    bits.append("+")
    for field in fields:
        bits.append('-' * widths[field])
        bits.append("+")
    bits.append("\n")
    border = "".join(bits)
    lines.append(border)
    # Add Hearder
    bits = []
    bits.append("|")
    for field in fields:
        bits.append(('{:^' + str(widths[field]) + '}').format(field))
        bits.append("|")
    bits.append("\n")
    hearder = "".join(bits)
    lines.append(hearder)
    lines.append(border)
    # Add Rows
    for line in data:
        bits = []
        bits.append("|")
        for field in fields:
            bits.append(('{:^' + str(widths[field]) + '}').format(line[field]))
            bits.append("|")
        bits.append("\n")
        lines.append("".join(bits))
    lines.append(border)
    return ("").join(lines)


def _compute_widths(fields, rows):
    """计算宽度
    """
    widths = {field: len(field) + 2 for field in fields}
    for row in rows:
        for field in fields:
            widths[field] = max(widths[field], len(str(row[field])) + 2)
    return widths


class _JSONEncoder(json.JSONEncoder):
    """JSON编码
    """

    def default(self, obj):
        """添加对decimal的支持
        """
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        return super(_JSONEncoder, self).default(obj)


class JSONDataEncoder(DataEncoder):
    """JSON解码
    """

    @classmethod
    def encode(cls, encodable_object):
        """编码
        """
        return json.dumps(encodable_object, cls=_JSONEncoder)

    @classmethod
    def decode(cls, decodable_string):
        """解码
        """
        return json.loads(decodable_string)


class Storage(dict):
    """增加data属性, 统一不同server任务数据
    """

    @property
    def data(self):
        """增加data属性
        """
        return self['data']
