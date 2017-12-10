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

import scala.concurrent.duration._

import mllibTest.utils.TimeUtils
import mllibTest.models.samples._


object RecentRates {

	def initSpark(): SparkSession = {
		val sparkMaster = Properties.envOrNone("SPARK_MASTER").get

		SparkSession.builder()
			.appName("MLlib Test")
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

    val ratePath = "/cs/work/group/carat/appusage/single-samples-all-until-2016-08-22-cc-mcc-obj-rates/"
    //val outPath = "/cs/work/group/carat/jirihamb/single-samples-2016-06-22-until-2016-08-22-cc-mcc-obj-rates/"
    val outPath = "/cs/work/group/carat/jirihamb/single-samples-2016-06-22-until-2016-08-22-cc-mcc-obj-rates-fixed/"

    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")

    val acceptAfterUnixTime = Duration(dateFormat.parse("2016-06-22").getTime(), MILLISECONDS).toSeconds

    val recentRates = readCaratRates(ratePath).filter { rate =>
      rate.rate() >= 0 && rate.sp._2.time > acceptAfterUnixTime
    }

    recentRates.saveAsObjectFile(outPath)

	}

}
