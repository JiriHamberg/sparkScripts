import scala.util.Properties

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.mllib.fpm.FPGrowth

import java.io._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import mllibTest.models.samples._

object AppCounts {

	def initSpark(): SparkSession = {
		val sparkMaster = Properties.envOrNone("SPARK_MASTER").get

		SparkSession.builder()
			.appName("AppCounts")
			.master(sparkMaster)
			.getOrCreate()
	}

  def readCaratRates(sampleDir: String)(implicit sc: SparkContext): RDD[fi.helsinki.cs.nodes.carat.sample.Rate] = {
    sc.objectFile[fi.helsinki.cs.nodes.carat.sample.Rate](s"${sampleDir}")
  }

	def main(args: Array[String]): Unit = {

		implicit val spark = initSpark()
		implicit val sc = spark.sparkContext
		implicit val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    implicit val formats = DefaultFormats

    val ratePath = "/cs/work/group/carat/appusage/single-samples-all-until-2016-08-22-cc-mcc-obj-rates/"
    val appCountsDirectory = "/cs/home/jirihamb/carat/app-counts-all-until-2016-08-22"
    val outfilename = s"${appCountsDirectory}/app_counts.json"

		val rates = readCaratRates(ratePath)

    val appCounts: Seq[(String, Int)] = rates
      .flatMap(rate => for(appName <- rate.allApps()) yield(appName, 1))
      .reduceByKey(_ + _)
      .collect()

    val outfile = new java.io.File(outfilename)
    val writer = new java.io.FileWriter(outfile)

    writer.write(pretty(render {
      Extraction.decompose(appCounts.toMap)
    }))

    writer.close()

	}

}
