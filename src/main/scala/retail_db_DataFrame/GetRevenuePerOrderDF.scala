package retail_db_DataFrame

import org.apache.spark.sql.SparkSession

object GetRevenuePerOrderDF {
  def main(args: Array[String]): Unit = {

    val spark =SparkSession
      .builder()
      .master("local")
      .appName("Spark Session SQL")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._

    val orderItems = spark.read.json("C:\\data\\retail_db_json\\order_items")

    orderItems.groupBy($"order_item_order_id").sum("order_item_subtotal").show()
  }
}
