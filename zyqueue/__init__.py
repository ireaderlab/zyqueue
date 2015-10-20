# zyqueue
from admin import queue_admin
from decorators import QueueJob
from workers import QueueWorkerServer

__version__ = '0.0.1'
VERSION = tuple(map(int, __version__.split('.')))
