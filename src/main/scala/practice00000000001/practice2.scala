package practice00000000001
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark




object practice2 extends App{
 case class Person (id: Int, name:String, age:Int , friends:Int)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()                  //to build our Session
    .appName("SparkSQL")  // app name as "SparkSQL
    .master("local[*]")   // run on local cpu  core
    .getOrCreate()                // we can create a new or use the existing one

  // load our DATA
  import spark.implicits._
  val readData:DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true") // read schema from the csv file itself
    .csv("data-new/fakefriends.csv")
   // .as[Person]  //DF ===> DS

  readData.printSchema()
  val readDF = readData
  //readData.show()

  // create a database view on it by saying " CreateOrReplaceTempView"
  //db table called PEOPLE
  readData.createOrReplaceTempView("people")
  //spark.catalog.listTables.show()

  val teenagerDF = readData.filter($"age">=11 && $"age"<=19)
  teenagerDF.show(30, false)



  //val teenager = spark.sql("SELECT * FROM people WHERE age >= 11 AND age<= 19")
 // val results:Array[Row] = teenager.collect()
  //results.foreach(println)


 /*readData.createOrReplaceGlobalTempView("globalPeople")
  val r = spark.sql("SELECT * FROM global_temp.globalPeople WHERE age >= 11 AND age<= 29")
  val r2 =r.collect()
  r2.foreach(println)

  */

  spark.stop()





}
