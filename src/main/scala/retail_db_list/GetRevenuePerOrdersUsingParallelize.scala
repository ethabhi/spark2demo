package retail_db_list

import org.apache.spark.{SparkConf, SparkContext}

object GetRevenuePerOrdersUsingParallelize {

  def main(args: Array[String]): Unit = {

    val orderItemsPath = args(1)
    val orderId = args(2).toInt

    val conf = new SparkConf().
      setMaster(args(0)).
      setAppName("Get revenue per orderid" + orderId + "using parallalize")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val orderItems = scala.io.Source.fromFile(orderItemsPath).
      getLines().toList

    val orderItemsFilterd = orderItems.filter(e => e.split(",")(1).toInt == orderId)

    //orderItemsFilterd.foreach(println)

    val orderItemSubtotal = orderItemsFilterd.map(e => e.split(",")(4).toFloat)
    //orderItemSubtotal.foreach(println)

    val orderRevenue = orderItemSubtotal.reduce((curr, next) => curr + next)

    println("Order revenue for orderid  " + orderId + "is" + orderRevenue)
  }
}
