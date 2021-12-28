FROM python:3.8-buster

ADD requirements.txt /tmp/requirements.txt

RUN pip3 install -r /tmp/requirements.txt

RUN apt-get update && apt-get install -y --no-install-recommends build-essential gcc wget vim

# 安装ssh
RUN apt-get install -y openssh-server openssh-client && \
    echo root:999999 | chpasswd && \
    echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config   && \
    echo "PermitRootLogin yes" >> /etc/ssh/sshd_config  && \
    service ssh restart && \
    systemctl enable ssh 

WORKDIR /usr/local/src

#编译安装talib
RUN wget  http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && tar -zxvf ta-lib-0.4.0-src.tar.gz && cd ta-lib && ./configure && make && make install && pip3 install ta-lib && ldconfig && rm -rf /usr/local/src/ta-lib*




