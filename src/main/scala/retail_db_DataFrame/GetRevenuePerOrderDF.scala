package retail_db_DataFrame

import org.apache.spark.sql.SparkSession

object GetRevenuePerOrderDF {
  def main(args: Array[String]): Unit = {

    val spark =SparkSession
      .builder()
      .master(args(0))
      .appName("Spark Session SQL")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._

    val orderItems = spark.read.json(args(1))

    val OrderItemsDF= orderItems.groupBy($"order_item_order_id").sum("order_item_subtotal")

    OrderItemsDF.write.csv(args(2))
  }
}
