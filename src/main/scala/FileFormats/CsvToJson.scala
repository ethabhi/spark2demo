package FileFormats

import utils.sparkCont
import org.apache.spark.{SparkConf, SparkContext}

object CsvToJson extends App with sparkCont{

  val orders = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:\\data\\retail_db\\orders")
 .toDF("orderId", "date", "customerId", "status")
  //orders.printSchema()

  orders.coalesce(1).
    write.json("C:\\data\\retail_db\\orders\\ordersJson")


}
