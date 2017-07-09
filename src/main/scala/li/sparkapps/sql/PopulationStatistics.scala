package li.sparkapps.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by lee on 7/8/17.
  */
object PopulationStatistics {
  def main(args: Array[String]) :Unit = {
    if (args.length < 1) {
      println("Please specify the <hdfsFilepath>");
      System.exit(1)
    }
    val schemaString = "id, gender, height"
    val conf = new SparkConf().setAppName("PopulationStatistics").setMaster("local[4]")
    val ctx = new SparkContext(conf)
    val hdfsFilePath = args(0)
    val populationRDD = ctx.textFile(hdfsFilePath) // 这里使用HDFS
    val sqlCtx = new SQLContext(ctx)

    // 获得RDD -> DataFrame的隐式转换支持
    import sqlCtx.implicits._
    val schemaArray = schemaString.split(",")
    // 创建Schema
    val schema = StructType(schemaArray.map(fieldName => StructField(fieldName.trim(), StringType, false)))
    println(schema) // 需要用trim去除空白，否则列名是包括空白字符的

    // 将普通RDD变成RDD[Row]
    val rowRDD: RDD[Row] = populationRDD.map(_.split(" ")).map(record => Row(record(0), record(1), record(2)))
    // 生成DataFrame
    val populationDF = sqlCtx.createDataFrame(rowRDD, schema)

    populationDF.registerTempTable("population")

    // 统计男性身高超过180的人数
    val cntManOver180 = sqlCtx.sql("SELECT id FROM population WHERE height > 180 " +
      "AND gender='M'").count()
    println("#man height than 180 " + cntManOver180 )

    // 统计男性人数
    populationDF.groupBy(populationDF("gender")).count().show() // 这个count()是GroupData类的方法

    // 身高超过210的男性人数
    populationDF.filter(populationDF("height") > 210).filter(populationDF("gender").equalTo("M"))
                .show(50)

    // 打印身高前50的人
    populationDF.sort($"height".desc).take(50).foreach(row => println(row(0)+", " + row(1) + ", " + row(2)))

    // 男性身高平均值
    println("男性身高平均值")
    populationDF.filter(populationDF("gender").equalTo("M")).agg(Map("height" -> "avg")).show()

    // 女性身高最大值
    populationDF.filter(populationDF("gender").equalTo("F")).agg(Map("height" -> "max")).show()
    
  }

}
