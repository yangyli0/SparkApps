package li.sparkapps

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by lee on 7/3/17.
  */
object LogAnalysisSc {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("please specify the hdrf URI");
      System.exit(1);
    }
    /* 设定计算周期, 每10s产生的日志文件作为一个批次 */
    val batch = 10
    val conf = new SparkConf().setAppName("WebLogAnalysis").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(batch))

    /* 创建输入DStream */
    val path = args(0)
    val lines = ssc.textFileStream(path) // 需要指定端口号

    /* 统计各项指标 */
    // 1.总PV
    lines.count().print()

    // 2.各IP的PV, 按PV倒序, 第一个字段是ip

    lines.map(line => {(line.split(" ")(0), 1)}).reduceByKey(_ + _).transform(rdd =>
    {rdd.map(ip_pv => (ip_pv._2, ip_pv._1)).sortByKey(false)
      .map(ip_pv => (ip_pv._2, ip_pv._1))}).print()

    // 3.搜索引擎pv
    val refer = lines.map(_.split("\"")(3))

    // 先输出搜索引擎和查询关键字,避免统计搜索关键字时重复计算
    // 输出(host, query_keys)

    val searchEngineInfo = refer.map(r => {
      val f = r.split('/')

      val searchEngines = Map(
        "www.google.cn" -> "q",
        "www.yahoo.com" -> "p",
        "cn.bing.com" -> "q",
        "www.baidu.com" -> "wd",
        "www.sougou.com" -> "query"
      )

      if (f.length > 2) {
        val host = f(2)

        if (searchEngines.contains(host)) {
          val query = r.split('?')(1)
          if (query.length > 0) {
            val arr_search_q =
              query.split('&').filter(_.indexOf(searchEngines(host)+"=") == 0)

            if (arr_search_q.length > 0)
              (host, arr_search_q(0).split('=')(1)) // 返回值
            else
              (host, "")

          } else {
            (host, "")
          }
        } else
          ("", "")
      } else
        ("", "")
    })

    // 输出搜索引擎PV
    searchEngineInfo.filter(_._1.length > 0).map(
      p => {(p._1, 1)}).reduceByKey(_ + _).print()

    // 输出关键词PV
    searchEngineInfo.filter(_._2.length > 0).map(
      p => {(p._2, 1)}).reduceByKey(_ + _).print()


    // 终端类型PV
    lines.map(_.split("\"")(5)).map(agent => {
      val types = Seq("iPhone", "Android")
      var r = "Default"
      for (t <- types) {
        if (agent.indexOf(t) != -1)
          r = t
      }
      (r, 1)
    }).reduceByKey(_ + _).print()

    // 各页面PV
    lines.map(line => {(line.split("\"")(1).split(" ")(1), 1)}).reduceByKey(_ + _).print()

    // 启动计算, Ctrl+C退出
    ssc.start()
    ssc.awaitTermination()

  }
}
