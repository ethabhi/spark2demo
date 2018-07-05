package FileFormats

import utils.sparkCont

import org.apache.spark.sql.SparkSession

import org.apache.spark.{SparkConf, SparkContext}
object JsontoText extends App with sparkCont{


  val orders = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "true").
    json("C:\\data\\retail_db_json\\orders").
    toDF("orderId", "date", "customerId", "status")
import sparkSession.implicits._
  orders.map(x=>x.mkString("|")).coalesce(1).write.
    csv("C:\\data\\retail_db_json\\orders\\ordersJsonTotext")

}