package FileFormats

import utils.sparkCont

object CsvToParquet extends App with sparkCont {

  val orders = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv("C:\\data\\retail_db\\orders")
    .toDF("orderId", "date", "customerId", "status")

  //spark.setConf("spark.sql.avro.compression.codec", "snappy")
  orders.coalesce(1).write.
    parquet("C:\\data\\retail_db\\orders\\OrdersParquet")

}
