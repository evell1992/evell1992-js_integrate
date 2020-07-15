import threading
import time
import traceback
from queue import Empty, Full
from threading import Thread
from .logger import logger


class pool(object):

    def __init__(self, nums, name):
        self.nums = nums
        self.name = name

    def __call__(self, func):
        def warpper(*args, **kwargs):
            for _ in range(self.nums):
                t = Thread(target=func, args=args, kwargs=kwargs, name=self.name)
                t.start()

        return warpper


def crawl_handler(work_func):
    def hold(self, *args, **kwargs):
        total_num = 0
        while True:
            try:
                keyword, task_num = work_func(self, *args, **kwargs)
                total_num += task_num
                logger.info("关键字 {} 爬取完成: 数据总数{}".format(keyword, task_num))
            except Empty:
                logger.info('所有关键字爬取完成: 数据总数{}'.format(total_num))
                break
            except Exception as e:
                self.log.error("爬取异常:{}".format(traceback.format_exc()))

    return hold


def clean_handler(work_func):
    def hold(self, *args, **kwargs):
        while True:
            try:
                work_func(self, *args, **kwargs)
            except Empty:
                names = [t.name for t in threading.enumerate()]
                if 'spider' not in names:
                    logger.info('线程{}清洗完成,退出'.format(threading.current_thread().ident))
                    break
                else:
                    time.sleep(5)
            except Exception as e:
                logger.error("清洗异常:{}".format(traceback.format_exc()))

    return hold


def snow_handler(work_func):
    def hold(self, *args, **kwargs):
        while True:
            try:
                work_func(self, *args, **kwargs)
            except Full:
                names = [t.name for t in threading.enumerate()]
                if 'clean' not in names:
                    logger.info('清洗完成,id生成线程{}退出'.format(threading.current_thread().ident))
                    break
                else:
                    time.sleep(5)
            except Exception as e:
                logger.error("雪花id生成异常:{}".format(traceback.format_exc()))

    return hold
