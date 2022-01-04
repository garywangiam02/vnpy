FROM python:3.8-buster

ADD requirements.txt /tmp/requirements.txt

RUN pip3 install -r /tmp/requirements.txt

WORKDIR /usr/local/src

RUN wget  http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && tar -zxvf ta-lib-0.4.0-src.tar.gz && cd ta-lib && ./configure && make && make install && pip3 install ta-lib && ldconfig && rm -rf /usr/local/src/ta-lib*

WORKDIR /app

# 保证时区正确
ENV TZ=Asia/Shanghai

ADD . /app

CMD python3 /app/program/CoinAlphaRotationCtaMain.py
