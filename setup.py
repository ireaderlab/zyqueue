#! /usr/bin/env python
import os
import sys
from distutils.core import setup
from distutils.command.install_data import install_data
from distutils.command.install import INSTALL_SCHEMES
# from zyqueue import __version__

# perform the setup action

packages, data_files = [], []

cmdclasses = {'install_data': install_data}

for scheme in INSTALL_SCHEMES.values():
    scheme['data'] = scheme['purelib']


def fullsplit(path, result=None):
    """
    Split a pathname into components (the opposite of os.path.join) in a
    platform-neutral way.
    """
    if result is None:
        result = []
    head, tail = os.path.split(path)
    if head == '':
        return [tail] + result
    if head == path:
        return result
    return fullsplit(head, [tail] + result)


def is_not_module(filename):
    """check filename
    """
    return os.path.splitext(filename)[1] not in ['.py', '.pyc', '.pyo']

for zyqueue_dir in ['zyqueue']:
    for dirpath, dirnames, filenames in os.walk(zyqueue_dir):
        # Ignore dirnames that start with '.'
        for i, dirname in enumerate(dirnames):
            if dirname.startswith('.'):
                del dirnames[i]
        if '__init__.py' in filenames:
            packages.append('.'.join(fullsplit(dirpath)))
            data = [f for f in filenames if is_not_module(f)]
            if data:
                data_files.append([dirpath, [os.path.join(dirpath, f) for f in data]])
        elif filenames:
            data_files.append([dirpath, [os.path.join(dirpath, f) for f in filenames]])
    data_files.append(['.', ['README.md']])


setup_args = {
    'name': 'zyqueue',
    # 'version': __version__,
    'version': '0.0.1',
    'description': 'ireader queue client',
    'long_description': open('README.md').read(),
    'author': 'WangLichao,Zengduju',
    'author_email': "zengduju@zhangyue.com",
    'packages': packages,
    'data_files': data_files,
    # 'include_package_data': True,
    'scripts': ["zyqueue/bin/zyqueue"],
}

setup(**setup_args)
