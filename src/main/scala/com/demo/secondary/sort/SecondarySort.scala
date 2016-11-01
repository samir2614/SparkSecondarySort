package com.demo.secondary.sort
import org.apache.spark.{Partitioner, SparkConf, SparkContext, SparkJobInfo}
/**
  * Created by samir on 25/10/16.
  */


case class TimeSeriesDataCompositeKey(year: Int, month: Int, temp: Double)

object TimeSeriesDataCompositeKey {
  implicit def orderingByYearMonthTemp[A <: TimeSeriesDataCompositeKey] : Ordering[A] = {
    Ordering.by(fk => (fk.month, fk.month, fk.temp * -1))
  }
}

class MyPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[TimeSeriesDataCompositeKey]
    (k.year + k.month).hashCode() % numPartitions
  }
}


object SecondarySort {
  //supporting code
  def createKeyValueTuple(data: Array[String]) :(TimeSeriesDataCompositeKey,List[String]) = {
    (createKey(data),listData(data))
  }

  def createKey(data: Array[String]): TimeSeriesDataCompositeKey = {
    TimeSeriesDataCompositeKey((data(0).trim().toInt), (data(1).trim().toInt), (data(3).trim().toDouble))
  }

  def listData(data: Array[String]): List[String] = {
    List(data(1), data(2), data(3))
  }

  def runSecondarySortExample(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SecondarySorting")
    conf.set("spark.files.overwrite","true")
    val sc = new SparkContext(conf)
    val hdfsInputPath="hdfs://localhost:9000/input/time_series_data.csv"
    val hdfsOutputPath="hdfs://localhost:9000/output/secondarysort"


    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:9000"), hadoopConf)
    try {
         hdfs.delete(new org.apache.hadoop.fs.Path(hdfsOutputPath), true)
    } catch {
         case _ : Throwable => { }

    }


    val rawDataArray = sc.textFile(hdfsInputPath).map(line => line.split(","))
    val templineData = rawDataArray.map(arr => createKeyValueTuple(arr))

    val keyedDataSorted = templineData.repartitionAndSortWithinPartitions(new MyPartitioner(1))

    //only done locally for demo purposes, usually write out to HDFS
    keyedDataSorted.collect().foreach(println)
    keyedDataSorted.saveAsTextFile(hdfsOutputPath)
  }

  def main(args: Array[String]): Unit = {
    // hdfs://localhost:9000/output/secondarysort

    runSecondarySortExample(args)
  }
}
