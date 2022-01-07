FROM python:3.8-buster

ADD requirements.txt /tmp/requirements.txt

RUN pip3 install -r /tmp/requirements.txt


WORKDIR /app

# 保证时区正确
ENV TZ=Asia/Shanghai

ADD . /app

CMD python3 /app/program/TgRobot.py