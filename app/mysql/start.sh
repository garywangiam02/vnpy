#!/bin/bash
cur_dir=`pwd`
docker stop mysql-data-farm
docker rm mysql-data-farm
docker run --restart=always -d -v ${cur_dir}/conf/my.cnf:/etc/mysql/my.cnf -v ${cur_dir}/logs:/logs -v ${cur_dir}/data/mysql:/var/lib/mysql -p 3306:3306 --name mysql-data-farm -e MYSQL_ROOT_PASSWORD=Aa123456 mysql:8.0
