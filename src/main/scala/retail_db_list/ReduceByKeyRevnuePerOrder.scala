package retail_db_list

import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKeyRevnuePerOrder {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("local").
      setAppName("Get Orders Items Subtotal using Group by key Map ")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val order_items = sc.textFile("C:\\data\\retail_db\\order_Items", 4)
    val orderItemsMap = order_items.
      map(e => (e.split(",")(1).toInt, e.split(",")(4).toFloat))
    // orderItemsMap.take(10).foreach(println)

    //reduce by key aggregates the data
    val revenuePerOrder = orderItemsMap.reduceByKey(_ + _)

    revenuePerOrder.take(10).foreach(println)
  }
}