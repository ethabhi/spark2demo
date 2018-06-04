package retail_db_list

object StringProcessing {

  def main(args: Array[String]): Unit = {
    //one record form orders
    //Fields :order_id,order_date,order_customer_id,order_status
    val o = "1,2013-07-25 00:00:00.0,11599,CLOSED"
    //Get Date
    val firstcomma = o.indexOf(",")
    println("Index of first , is" + firstcomma)
    val secondcomma = o.indexOf(",", firstcomma + 1)
    println("Index of second , is" + secondcomma)
    println("Date using substring" +
      o.substring(firstcomma + 1, secondcomma))
    //Get Date using split
    val orderDate = o.split(",")(1)
    println("Date using split and index" + orderDate)
    //Get order id and order customer id as Int
    val orderId = o.split(",")(0).toInt
    println("Order Id is " + orderId)
    val orderCustId = o.split(",")(2).toInt
    println("Order Customer Id is " + orderCustId)
    //compare whether order_status is closed

    val orderStatus = o.split(",")(3)
    val isclosed = orderStatus == "CLOSED"
    println("Is status of order id" + orderId + "closed?" + isclosed)

    //Get the date as integer as year in YYYYMMDD
    val orderDateAsInt = orderDate.substring(0, 10).replace("-", "").toInt
    println("oder date in YYYYMMDD is " + orderDateAsInt)

    //Filtering Data

  }
}

