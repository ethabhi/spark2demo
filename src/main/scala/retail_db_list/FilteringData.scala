package retail_db_list

import SparkSql.SparkSql.sparkSession
import utils.sparkCont


object FilteringData extends App with sparkCont {

  val orders = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("C:\\data\\retail_db\\orders")
    .toDF("orderId", "date", "customerId", "status")

  val ordersFil =orders.filter( e => e(3) == "COMPLETE")

  ordersFil.take(10).foreach(println)

}
