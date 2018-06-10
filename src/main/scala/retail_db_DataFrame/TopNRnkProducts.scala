package retail_db_DataFrame

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{rank, round, sum}

object TopNRnkProducts {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master(args(0))
      .appName("Spark Session SQL")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._
    spark.conf.set("spark.sql.shuffle.operations", "2")

    val inputBasedir = args(1)
    val TopNPr = args(2).toInt
    val ordersDF = spark.read.json(inputBasedir + "/orders")
    //order_id order_date
    val orderscomplete = ordersDF.
      where($"order_status".isin("COMPLETE", "CLOSED"))

    val orderItemsDF = spark.read.json(inputBasedir + "/order_items")
    val ordersjoin = orderscomplete.
      join(orderItemsDF, orderscomplete("order_id") === orderItemsDF("order_item_order_id"))

    val revenueperDatePerProduct = ordersjoin.groupBy("order_date", "order_item_product_id").
      agg(round(sum("order_item_subtotal"), 2).alias("revenue_perdat_per_prod"))
    val spec = Window.partitionBy("order_date").
      orderBy($"revenue_perdat_per_prod".desc)
    val rnk = rank().over(spec).alias("rnk")
    val TopNProductIdsPerDay = revenueperDatePerProduct.select($"*", rnk).where("rnk <=" + TopNPr)
    TopNProductIdsPerDay.sort("order_date", "rnk").show()
    //val ProductsDF =spark.read.json(inputBasedir + "/products")
    //product_id ,product name

  }
}
