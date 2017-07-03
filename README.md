## Spark应用程序（陆续更新）
### 来源: 《Spark最佳实践》,修正了原书中相应部分的代码小的错误
### 环境依赖
- hadoop-2.6.2
- spark-1.5.2-bin-hadoop2.6
- scala-2.10.5
####  基于Spark streaming的 模拟 网站日志实时分析系统
1. 基本描述:
- 日志来源:用Python脚本随机生成Nginx访问日志，并通过Bash脚本上传至HDFS
- Spark Streaming应用监控HDFS目录
- 每10s产生的日志文件作为一个批次
- 输出的流量分析包括指定时间内:
  + 总的PV
  + 各IP的PV
  + 来自各搜索引擎和关键字的PV
  + 终端类型(PC, ios, Android)PV

2. 使用：
- 项目根目录下:<code>mvn clean package</code>
- 启动hadoop hdfs
- 根据自身Spark和Hadoop配置情况，修改log_analysis.sh
- 项目根目录下，打开两个终端窗口，分别运行
  + <code>./create_log.sh</code>
  + <code>./log_analysis.sh</code>


