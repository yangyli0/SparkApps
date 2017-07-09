package li.sparkapps.sql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lee on 7/6/17.
  */
object CreateSchema {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CreateSchema").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //create an RDD
    val people = sc.textFile("src/main/resources/people.txt")

    // the schema is encoded in a string
    val schemaString = "name age"

    // Import Row
    /*
    val schema = StructType(
      schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    */
    val schema =
    StructType(
      schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val rowRDD = people.map(_.split(",")).map(p =>Row(p(0), p(1).trim))

    // apply the schema to the RDD
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // Register the DataFrame as a table
    peopleDataFrame.registerTempTable("people")

    val results = sqlContext.sql("SELECT name FROM  people")

    results.show();
    results.map(t => "Name: " + t(0)).collect().foreach(println)

    val df = sqlContext.read.format("json").load("src/main/resources/people.json")

    df.select("name", "age").write.format("parquet").save("src/main/resources/nameAndAges.parquet")


  }

}












