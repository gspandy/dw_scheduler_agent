#!/bin/bash

#调度系统 scheduler_runner.py scheduler_shell_hadoop.sh 实体运行方法

PATH_NAME="$1"

if [ -z "$PATH_NAME" ]; then
    echo "script not found"
    exit 1
fi

MODULE_NAME=$(basename $PATH_NAME)
MODULE_NAME=${MODULE_NAME%.*}

shift

# 打印要执行的脚本路径
echo $PATH_NAME $@

# 执行脚本
$PATH_NAME $@

# 脚本执行状态, 打开有问题, 关闭
# RETURN_CODE=1
