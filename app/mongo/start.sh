#!/bin/bash
cur_dir=`pwd`
docker stop vnpy-mongo
docker rm vnpy-mongo
docker run --restart=always -d -v ${cur_dir}/data/db:/data/db -p 27017:27017 --name vnpy-mongo mongo
