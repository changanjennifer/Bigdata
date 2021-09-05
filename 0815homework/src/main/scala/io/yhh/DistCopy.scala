package io.yhh

import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object DistCopy {

  def main(args: Array[String]): Unit ={
    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
    var sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val input = "file:/F:/02-spark/sparktest1/src/data"
    val output = "file:/F:/02-spark/sparktest1/src/dest"

    //parse args user Apache Commons-CLI
    val options = new Options()
    options.addOption("i", "ignore failure", false,"ignore option")
    options.addOption("m", "max concurrence", true, "concurrency")
    val parser = new DefaultParser()
    val cmd = parser.parse(options, args)

    // compose file list with input file path
    val fs = FileSystem.get(sc.hadoopConfiguration)

    //use FileSystem.listFiles will ignore the empty file. 空文件会丢失
    val fileList = fs.listFiles(new Path(input),true)
    val arrayBuffer = ArrayBuffer[String]()

    while(fileList.hasNext){
      val path = fileList.next().getPath.toString
      arrayBuffer.append(path)
    }

    val INGNORE_FAILE  = cmd.hasOption("i")
    val MAX_CONNCURRENCES = if (cmd.hasOption("m")) cmd.getOptionValue("m").toInt else 2

    val rdd = sc.parallelize(arrayBuffer, MAX_CONNCURRENCES)

    rdd.foreachPartition((it=>{
      // handle the parameters needed for FileUtil.copy
      val conf = new Configuration()
      val sfs = FileSystem.get(conf)

      while(it.hasNext){
        val src = it.next()
        println(src)
        // here input and output variables must use the absolute path,don't use the relative path
        // otherwise the String.replace will not work.
        val tgt = src.replace(input, output)
        try{
          FileUtil.copy(sfs, new Path(src), sfs , new Path(tgt), false, conf)
        }catch {
          case ex:Exception=>
            if (INGNORE_FAILE) println("ignore fail")
        }

      }
    }))
  }
}
