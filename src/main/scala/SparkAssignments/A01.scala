package SparkAssignments


import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions.{collect_list, collect_set, expr, lit}





object A01  {


  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("SparkSql")
      .master("local[*]")
      .getOrCreate()



    val userDf = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("src/main/resorces/assignment01/user.csv")

    //userDf.printSchema()
    //userDf.show(false)

    val transactionDf = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("src/main/resorces/assignment01/transaction.csv")

   // transactionDf.show()

    val joinedDf = userDf.join(transactionDf,userDf("user_id")===transactionDf("userid"))  //inner join by default

    //joinedDf.show()


    val df1 = joinedDf.groupBy("product_description").agg(collect_list("location")
      .as("uniqueLoc"))
    println(" unique locations where each product is sold ")
    df1.show(false)

    val df2 = joinedDf.groupBy("userid").agg(collect_list("product_description")
      .as("uniqueProductBought"))
    println("products bought by each user")

    df2.show(true)

    val df3 = joinedDf.groupBy("userid").agg(collect_list("price")
      .as("totalSpending"))

    println("Total spending done")

    df3.withColumn("total", expr("aggregate (totalSpending, 0, (acc, val) -> acc + val) "))
    .show(false)





    spark.stop()


  }


}
