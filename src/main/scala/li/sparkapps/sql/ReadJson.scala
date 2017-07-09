package li.sparkapps.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lee on 7/5/17.
  */
object ReadJson {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReadJson").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("src/main/resources/people.json") // 返回DataFrame
    df.show()
    df.printSchema()
    df.select("name").show()
    df.filter(df("age") > 21).show()
    df.groupBy(df("age")).count().show()

    println("hell")
    df.registerTempTable("people")

    val result = sqlContext.sql("SELECT * FROM people")

    result.show()


  }

}
