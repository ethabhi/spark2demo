package retail_db

import org.apache.spark.{SparkConf, SparkContext}

object Orders {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("order")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val orderItems = sc.textFile("C:\\data\\retail_db\\order_items")
    val revenuePerOrder = orderItems.
      map(oi => (oi.split(",")(1).toInt, oi.split(",")(4).toFloat)).
      reduceByKey(_ + _).
      map(oi => oi._1 + "," + oi._2)

    revenuePerOrder.take(10).foreach(println)
  }
}
