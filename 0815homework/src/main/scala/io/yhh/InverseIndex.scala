package io.yhh

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object InverseIndex  extends  App{

  val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
  var sc = new SparkContext(sparkConf)
  sc.setLogLevel("WARN")

  // compose file list with input file path
  val inputPath = "./src/data"
  val fs = FileSystem.get(sc.hadoopConfiguration)

  val fileList = fs.listFiles(new Path(inputPath),false)

  //val array = ArrayBuffer[String]
  var rdd = sc.emptyRDD[(String, String)]
  //iter with the fileList
  while (fileList.hasNext){
    //array.apply(fileList.next.getPath.toString)
    val path = fileList.next
    val fileName = path.getPath.getName
    rdd = rdd.union(sc.textFile(path.getPath.toString)
      .flatMap(_.split("\\s+"))
        .map((fileName, _)))
    // out put will be [filename, word]
  }

  // a rdd with Union RDD
//  println("--"* 100)
//  rdd.foreach(println)
//  println("--"* 100)
  val rdd2 = rdd.map((_,1)).reduceByKey(_+_).sortByKey()
  // ((文件名，单词)，词频)
  println("--"* 100)
  rdd2.foreach(println)
  println("--"* 100)

  val rdd3 = rdd2.map(data=> (data._1._2,
    String.format("(%s,%s)", data._1._1, data._2.toString)))
    .reduceByKey(_+ "," + _)
  println("--"* 100)
  rdd3.foreach(println)
  println("--"* 100)

  val rdd4 = rdd3.map(data=>String.format("\"%s\", {%s}", data._1, data._2))
  println("--"* 100)
  rdd4.foreach(println)
  println("--"* 100)
  // create word count

  // output data accroding to the requirement

//  def main(args: Array[String]): Unit ={
//  }
}
