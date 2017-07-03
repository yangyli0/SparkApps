#!/bin/bash

# HDFS 命令
HDFS="/home/lee/libs/hadoop-2.6.2/bin/hdfs dfs"

# Streaming监听目录
streaming_dir="/spark/streaming"

# 清空旧数据
$HDFS -rm "${streaming_dir}"'/tmp/*' > /dev/null 2>&1
$HDFS -rm "${streaming_dir}"'/*'    > /dev/null 2>&1

#一直运行
while [ 1 ]; do
    ./sample_web_log.py > test.log

    # 给日志文件加上时间戳
    tmplog="access.`date '+%s'`.log"
    
    # 先放在临时目录，再放至监控目录，保证原子性
    $HDFS -put test.log ${streaming_dir}/tmp/$tmplog
    $HDFS -mv ${streaming_dir}/tmp/$tmplog ${streaming_dir}/
    echo "`date  "+%F %T"` put $tmplog to HDFS succeed"
    sleep 1
done
