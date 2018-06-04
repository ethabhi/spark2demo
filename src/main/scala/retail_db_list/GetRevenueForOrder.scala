package retail_db_list

object GetRevenueForOrder {

  def main(args: Array[String]): Unit = {

    val orderItemsPath=args(0)
    val orderId =args(1).toInt

    val orderItems=scala.io.Source.fromFile(orderItemsPath).
      getLines().toList

    val orderItemsFilterd=orderItems.filter(e=> e.split(",")(1).toInt == orderId)

    //orderItemsFilterd.foreach(println)

    val orderItemSubtotal=orderItemsFilterd.map(e=>e.split(",")(4).toFloat)
    //orderItemSubtotal.foreach(println)

    val orderRevenue=orderItemSubtotal.reduce((curr,next)=> curr+next)

    println("Order revenue for orderid  " +orderId+ "is" +orderRevenue)
  }
}
