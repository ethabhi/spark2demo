package utils


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait sparkCont {

   val sparkConf = new SparkConf()
    .setAppName("Learn Spark")
    .setMaster("local[*]")
    .set("spark.cores.max", "2")

  val sparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
}


