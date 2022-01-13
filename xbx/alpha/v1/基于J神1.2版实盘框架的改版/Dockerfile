FROM ccxt_trader_37:20210515.1.0.0

COPY program/ /opt/app/program/
RUN rm /opt/app/program/trading/config.json

WORKDIR /opt/app/
RUN mkdir data

VOLUME ["/opt/app/program/trading/config.json"]
ENV PYTHONUNBUFFERED 1

WORKDIR /opt/app/program/trading/
ENTRYPOINT [ "python", "-u", "select_compound_coin.py" ]