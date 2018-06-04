package retail_db_list

import org.apache.spark.{SparkConf, SparkContext}

object GetTopNproductsPerDay {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster(args(0)).
      setAppName("Get Top N Products per day by revenue ")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //Reading Data From File System
    //Read Orders,order_items and products from file
    val inputBaseDir = args(1)
    val orders = sc.textFile(inputBaseDir + "/orders")
    val order_items = sc.textFile(inputBaseDir + "/order_items")
    val products = sc.textFile(inputBaseDir + "/products")

    //Row level transformation
    //orders Filtered apply Map
    val ordersMap = orders.filter(e => List("COMPLETE", "CLOSED")
      .contains(e.split(",")(3)))
      .map(e => (e.split(",")(0).toInt, e.split(",")(1)))

    //order items use map to transform data into K(,V)
    val orderItemsMap = order_items.map(e => (e.split(",")(1).toInt,
      (e.split(",")(2).toInt, e.split(",")(4).toFloat)))

    val regx = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
    val productsMap = products.map(e => (e.split(regx,-1)(0).toInt,
      e.split(regx,-1)(2)))
    productsMap.take(10).foreach(println)
  }
}
