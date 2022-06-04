package pack



import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util._


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.avro
import org.apache.spark.sql.functions._

import sys.process._

object may29 {
  case class schema(id: String, name: String, comp_name: String)
  def main(args: Array[String]): Unit =
  {


    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .getOrCreate()


   /* println("This is Parquet data Default file format of spark")
    val parqdf = spark.read.load("C:\\Users\\ppasula1\\Desktop\\Spring boot\\data.parquet")
    parqdf.show()
    println("This is DF converted to RDD")
    val convertedrdd=parqdf.rdd;
    convertedrdd.foreach(println)
*/

/*  println("This is RD CSV data")
   val rdcsvdf = spark.read.format("csv").load("C:\\Users\\ppasula1\\Desktop\\Spring boot\\rd.csv")
    rdcsvdf.show()*/
    println("This is RDD CSV")
    val file=sc.textFile("C:\\Users\\ppasula1\\Desktop\\Spring boot\\rd.csv")
   // val mapslit=file.map(x=>x.split(","))
    //val schemafile=file.map(x=> schema(x(0),x(1),x(2)))
    //schemafile.foreach(println)



  }

}
