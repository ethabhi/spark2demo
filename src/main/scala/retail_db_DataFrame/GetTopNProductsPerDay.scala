package retail_db_DataFrame

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetTopNProductsPerDay {

  def main(args: Array[String]): Unit = {

    val spark =SparkSession
      .builder()
      .master(args(0))
      .appName("Spark Session SQL")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._
    spark.conf.set("spark.sql.shuffle.operations", "2")

    val inputBasedir =args(1)
    val ordersDF =spark.read.json(inputBasedir + "/orders")
    //order_id order_date
    val orderscomplete =ordersDF.
      where($"order_status".isin("COMPLETE","CLOSED"))

    val orderItemsDF=spark.read.json(inputBasedir + "/order_items")
    val ordersjoin =orderscomplete.
      join(orderItemsDF,orderscomplete("order_id") === orderItemsDF("order_item_order_id"))

    val revenueperDatePerProduct =ordersjoin.groupBy("order_date","order_item_product_id").
      agg(round(sum("order_item_subtotal"),2).alias("revenue_perdat_per_prod"))
    revenueperDatePerProduct.show()
    val ProductsDF =spark.read.json(inputBasedir + "/products")
//product_id ,product name



  }
}

