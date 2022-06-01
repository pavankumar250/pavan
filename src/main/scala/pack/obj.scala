package pack

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sys.process._

object obj {
  def main(args: Array[String]): Unit = {



   /* val spark= SparkSession.builder()
      .master("local[*]")
      .appName("SparkByExamples.com")
      .getOrCreate()
*/
    /*val data=spark.textFile("C:\\Users\\ppasula1\\Desktop\\test.txt")
    data.foreach(println)*/
    println("Started")
    val a=2
    println(a)
    val list=List(1,2,3,4,5,6,7)
    val newlis=list.filter(x=>x>3)
    newlis.foreach(println)

  }


}