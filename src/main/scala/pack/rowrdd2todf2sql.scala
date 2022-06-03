package pack

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
object rowrdd2todf2sql {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .getOrCreate()
println("Raw Data")
    val file=sc.textFile("C:\\Users\\ppasula1\\Desktop\\data.txt")
    file.foreach(println)
    println("Map split Data")
    val mapsplit=file.map(x=>x.split(","))
    println("Row RDD Data")
    val rowrdd=mapsplit.map(x=>Row(x(0),x(1),x(2),x(3)))

    val schemastruct=StructType(Array(
      StructField("id",StringType),
      StructField("tdate",StringType),
      StructField("category",StringType),
      StructField("product",StringType)

    ))
    println("convert Row RDD Data to dataframe")
    val df=spark.createDataFrame(rowrdd,schemastruct)
    df.show()
    println("Dataframe to SQL")
    df.createOrReplaceTempView("rowdf")
    val filterdf=spark.sql("select * from rowdf")
    filterdf.show()
  }
  }
