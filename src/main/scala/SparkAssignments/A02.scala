package SparkAssignments

import org.apache.spark._
import org.apache.log4j._


object A02 extends App{
  def parseLine(line: String): (String) = {
    val fields = line.split(",")
    fields(0)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "Assignment02")

  val logData = sc.textFile("src/main/resorces/assignment02/ghtorrent-logs.txt")


  def linesContain: Long = {
    val rddLines = logData.map(parseLine)
    val lines = rddLines.collect()
    rddLines.count()
  }

  println(s"-------------rdd file contains:- $linesContain Lines")

  def countNoOFWarnings: Unit = {
    val rdd = logData.map(parseLine)
    val countTheWarning = rdd.filter(x => x == "WARN")
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
    countTheWarning.foreach(println)

  }
  countNoOFWarnings

  def countAPIRepositories: Unit = {
    val rdd = logData.flatMap(x => x.split(" "))
      .filter(x => x == "api_client.rb:")
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y).foreach(println)

  }

  println(" -------Repositories processed in total-------")
  countAPIRepositories

  def countmaxHTTP: Unit = {
    val rdd = logData.flatMap(x => x.split(","))
      .filter(x => x.contains("URL:") && x.contains("ghtorrent-"))
      .map(x => (x.substring(0, 13), 1))
      .reduceByKey((x, y) => x + y).sortBy(x => x._2, true).foreach(println)
  }

  println(" ------Client with most HTTP requests------")
  countmaxHTTP

  def failedHTTPrequests: Unit = {
    val rdd = logData.flatMap(x => x.split(", "))
      .filter(x => x.contains("Failed") && x.contains("ghtorrent-"))
      .map(x => (x.substring(0, 13), 1))
      .reduceByKey((x, y) => x + y).foreach(println)
  }

  println("-------FailedHTTPrequests--------")
  failedHTTPrequests


  def countMostActiveHours = {
    val rdd = logData.flatMap(x => x.split(","))
      .filter(x => x.contains("+00"))
      .map(x => (x.substring(0, 11), ((x.substring(12, 14)), 1)))
      .reduceByKey((x, y) => (x._1, x._2 + y._2))

      .foreach(println)
  }
  println(" ------Most active hour of day-------")
  countMostActiveHours

  def countMostRepo ={
    val dataDaily = logData.flatMap(x=> x.split(","))
      .filter(x=> x.contains("ghtorrent.rb: Repo")&& x.contains("exists"))
      .map(x=>(x.substring(x.indexOf("Repo")+ 5, x.indexOf("exists")-1), 1))
      .reduceByKey((x,y) => x+y).sortBy(x=>x._2, false).take(10)
      .foreach(println)
  }
  println(" ------Most active  repository-------")
  countMostRepo

}


