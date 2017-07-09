package li.sparkapps.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lee on 7/8/17.
  */
// 用户case class
case class User(userId: String, gender: String, age: Int,
                registerDate: String, role: String, region: String)

// 交易记录case class
case class Order(orderId: String, orderDate: String, productId: Int,
                price: Int, userId: String)

object UserDealDataStatistics {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Please Specify <userDataFilePath> <dealDataFilePath>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("userDealDataStatistics").setMaster("local[4]")
    // 采用Kryo序列化
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ctx = new SparkContext(conf)
    val sqlCtx = new SQLContext(ctx)
    // 获取将RDD转换的DataFrame的隐式转换支持
    import sqlCtx.implicits._
    val userDataPath = args(0)
    val orderDataPath = args(1)
    // 将用户数据RDD转换成DataFrame
    val userDF = ctx.textFile(userDataPath).map(_.split(" "))
                        .map(user => User(user(0), user(1), user(2).toInt, user(3),user(4), user(5)))
                          .toDF()
    // 将用户DataFrame注册为临时表
    userDF.registerTempTable("user")
    // 将交易数据RDD转换成DataFrame
    val orderDF = ctx.textFile(orderDataPath).map(_.split(" "))
                      .map(order => Order(order(0), order(1), order(2).toInt, order(3).toInt, order(4)))
                        .toDF()
    orderDF.registerTempTable("deals")

    // 将数据持久化在缓存中
    userDF.persist(StorageLevel.MEMORY_ONLY_SER)
    orderDF.persist(StorageLevel.MEMORY_ONLY_SER)

    /* SQL 操作*/

    // 15年的参与购物的用户人数
    val peopleCnt15 = orderDF.filter(orderDF("orderDate").contains("2015"))
                                .join(userDF, orderDF("userId").equalTo(userDF("userId"))).count()
    println("The num Of people who have orders in 2015: " + peopleCnt15)
    // 14年交易总数
    //val numOfOrder14 = sqlCtx.sql("SELECT * FROM order WHERE orderDate LIKE '2014%'").count()
    val numOfOrder14 = sqlCtx.sql("SELECT * FROM deals where orderDate like '2014%'").count()

    println("The num of orders occured int 2014 is: " + numOfOrder14)

    // 用户Id为1的总消费情况
    println("用户Id为1的消费情况")
    val orderOfUserI = sqlCtx.sql("SELECT o.orderId, o.productId, o.price, u.userId FROM " +
          "deals o, user u WHERE u.userId = 1 AND o.userId = u.userId").show()

    // 用户Id为10的交易最高价，最低价，平均价
    val orderOfUserX = sqlCtx.sql("SELECT max(o.price) as maxPrice, min(o.price) as minPrice," +
      "avg(o.price) as averPrice, u.userId FROM deals o, user u WHERE u.userId=10 AND " +
      "o.userId = u.userId GROUP BY u.userId")
    println("用户Id为10的消费情况")
    //orderOfUserX.show()
    orderOfUserX.collect().map(record => "maxPrice: " + record.getAs("maxPrice") + " minPrice: " +
      record.getAs("minPrice") + " averPrice: "+record.getAs("averPrice") + " userId: " +
      record.getAs("userId")).foreach(result => println(result))


  }

}
