package SparkSql

import utils.sparkCont

object SparkSql extends App with sparkCont {

  val orders = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("C:\\data\\retail_db\\orders")
    .toDF("orderId", "date", "customerId", "status")
  //orders.take(10).foreach(println)
//orders.filter(e => e.split(","))
}

