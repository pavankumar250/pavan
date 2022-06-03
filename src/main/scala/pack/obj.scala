package pack


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util._


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import sys.process._

object obj {
  case class schema(id:String,date:String,category:String,product:String)

  def main(args: Array[String]): Unit = {


val conf=new SparkConf().setAppName("first").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .getOrCreate()


    val file=sc.textFile("C:\\Users\\ppasula1\\Desktop\\data.txt")
    file.foreach(println)
  /*  println("Started with flatMap")
    val fildata=file.flatMap(x=>x.split(","))
    fildata.foreach(println)*/

    println("Started with Map")
    val splitdata=file.map(x=>x.split(","))
    val schemardd=splitdata.map(x=> schema(x(0),x(1),x(2),x(3)))
    schemardd.foreach(println)

    val filterdata=schemardd.filter(x=>x.product.contains("product1"))
    println("filtered data of schemardd")
    filterdata.foreach(println)
import spark.implicits._
    println("Convert RDD to dataframe")
    val df=filterdata.toDF()
    df.show()
    println("write  dataframe to parquet file")
    //df.write.parquet("C:\\Users\\ppasula1\\Desktop\\Spring boot\\parqdata")

    println("Done")
    val mapsplit=file.map(x=>x.split(","))
    val rowrdd=mapsplit.map(x=> Row(x(0),x(1),x(2),x(3)))


    println("===================TASK 1==============================")
    println("Using StructType ")
    val schemastruct=StructType(Array(
      StructField("id",StringType),
     StructField("tdate",StringType),
      StructField("category",StringType),
      StructField("product",StringType)

    ))

val data1=  spark.createDataFrame(rowrdd, schemastruct)


    data1.show()

    println("===================TASK 2==============================")
    println("===================Writing dataframe to parquet==============================")
    data1.write.mode("overwrite").parquet("C:\\Users\\ppasula1\\Desktop\\Spring boot\\parqdata")
    /*val data=spark.textFile("C:\\Users\\ppasula1\\Desktop\\test.txt")
    data.foreach(println)*/

 /*   val a=2
    println(a)
    val list=List(1,2,3,4,5,6,7)
    val newlis=list.filter(x=>x>3)
    newlis.foreach(println)*/

  }


}