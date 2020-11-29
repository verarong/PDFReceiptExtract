import time
import logging

##
logger = logging.getLogger(__name__)
ch = logging.FileHandler("/logs/logging.log")
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def debug(fn):
    def wrapper(*args, **kwargs):
        start = time.time()
        score = fn(*args, **kwargs)
        logger.warning("{} cost {}".format(fn.__name__, time.time() - start))
        return score

    return wrapper


def sum_dict(a, b):
    temp = dict()
    for key in a.keys() | b.keys():
        temp[key] = sum([d.get(key, 0) for d in (a, b)])
    return temp