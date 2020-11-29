from fastapi import FastAPI
from concurrent.futures import ThreadPoolExecutor
from app.utils import debug
from app.items import InputItem
from app.handler import ReceiptParse
from app.config import MAX_WORKERS
from ufile import config
import os
import time
from app.config import CALLBACK_URL

app = FastAPI()
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
try:
    callback_url = os.environ['CALLBACK_URL']
except KeyError:
    callback_url = CALLBACK_URL

'''
# 设置上传host后缀,外网可用后缀形如 .cn-bj.ufileos.com（cn-bj为北京地区，其他地区具体后缀可见控制台：对象存储-单地域空间管理-存储空间域名）
config.set_default(uploadsuffix='_AiGroup')
# 设置下载host后缀，普通下载后缀即上传后缀，CDN下载后缀为 .ufile.ucloud.com.cn
config.set_default(downloadsuffix='_AiGroup')
'''
# 设置请求连接超时时间，单位为秒
config.set_default(connection_timeout=60)
# 设置私有bucket下载链接有效期,单位为秒
config.set_default(expires=60)
# 设置上传文件是否校验md5
config.set_default(md5=True)


def PDF_parse_test_env(params):
    tasks, request_time = params
    for task in tasks:
        parse_task = ReceiptParse(**task.to_dict(), request_time=request_time)
        # try:
        return parse_task.extractor(callback_url)
        # parse_task.call_back()
        # except Exception as e:
        #    parse_task.call_back(callback_url, repr(e))


def PDF_parse(params):
    tasks, request_time = params
    for task in tasks:
        parse_task = ReceiptParse(**task.to_dict(), request_time=request_time)
        try:
            parse_task.extractor(callback_url)
            # parse_task.call_back()
        except Exception as e:
            parse_task.call_back(callback_url, repr(e))


@app.get("/health")
def read_root():
    return {"project": "alive"}


@app.post('/parse')
def predict(item: InputItem):
    # print(callback_url)
    receipts = item.Receipts
    executor.submit(PDF_parse, (receipts, int(time.time())))
    # score = PDF_parse(receipts)
    return {"Code": "succeed", "State": "progressing"}


@app.post('/parse_test_env')
def parse_test_env(item: InputItem):
    # print(callback_url)
    receipts = item.Receipts
    # executor.submit(PDF_parse, (receipts, int(time.time())))
    score = PDF_parse_test_env((receipts, int(time.time())))
    return {"Code": "succeed", "State": "progressing", "score": score}
