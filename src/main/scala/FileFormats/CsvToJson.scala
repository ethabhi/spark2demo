package FileFormats

import utils.sparkCont
import org.apache.spark.{SparkConf, SparkContext}

object CsvToJson extends App with sparkCont{

  val orders = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "true")
    .textFile("C:\\data\\retail_db\\orders")
  val ordersFiltered=orders.filter(e=>e.split(",")(3)=="COMPLETE")

 // ordersFiltered.take(5).foreach(println)
  import sparkSession.implicits._

   val ordersFilteredDF=ordersFiltered.map(x=> (x.split(",")(0).toInt,x.split(",")(1),
      x.split(",")(2).toInt,x.split(",")(3)))
      .toDF("orderId", "date", "customerId", "status")
  val res = ordersFilteredDF.map(x=>x.mkString("\t"))
  res.coalesce(1).write
    .json("C:\\data\\retail_db\\orders\\ordersFilteredJson")


}
