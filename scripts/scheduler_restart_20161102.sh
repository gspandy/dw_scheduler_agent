#!/bin/bash
#杀掉正在跑的进程
pid=`ps aux | grep 'dw_scheduler_agent.jar' | sed -r 's/ +/ /g' | cut -f2 -d" "`
sudo kill $pid
SCHEDULE_HOME=$(cd `dirname $0`; pwd);
log_date=`date +%Y-%m-%d`;
#nohup java -Dfile.encoding=utf-8 -jar \
java -Dfile.encoding=utf-8 -jar \
${SCHEDULE_HOME}/run_jar/dw_scheduler_agent.jar >> /data/log/dwlogs/schedule_log/scheduler_run_log/scheduler.out.$log_date 2>&1 &