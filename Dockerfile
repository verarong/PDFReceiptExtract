FROM harbor.yzf.best:1180/library/python:3.6.12

WORKDIR /api
COPY ./requirements.txt /api
RUN pip install -r /api/requirements.txt -i https://pypi.douban.com/simple/

COPY ./app /api/app
RUN mkdir /logs/

ENTRYPOINT ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--log-level", "warning"]
############################     命令行启动     ################################
#       uvicorn app.main:app --host 0.0.0.0 --log-level warning
