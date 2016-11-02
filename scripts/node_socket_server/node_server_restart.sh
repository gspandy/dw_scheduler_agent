#!/bin/bash
SERVER_HOME=$(cd `dirname $0`; pwd);
log_date=`date +%Y-%m-%d`;

#杀掉正在跑的进程
pid=`ps aux | grep "$SERVER_HOME/node_server.js" | sed -r 's/ +/ /g' | cut -f2 -d" "`
sudo kill $pid

nohup node $SERVER_HOME/node_server.js >> /data/log/dwlogs/socket_log/socket_log.$log_date 2>&1 &
