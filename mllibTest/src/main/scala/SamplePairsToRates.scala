import fi.helsinki.cs.nodes.util.Spark2Main
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import fi.helsinki.cs.nodes.carat.sample.Sample
import fi.helsinki.cs.nodes.carat.sample.SamplePair
import fi.helsinki.cs.nodes.carat.sample.Rate
import fi.helsinki.cs.nodes.carat.rate.RateHelpers
import fi.helsinki.cs.nodes.carat.sample.Registration

import org.apache.spark.rdd.RDD

/**
 *
 */
object SamplePairsToRates extends Spark2Main {

  val shortOptions = ""

  val longOptions = Seq("output=", "samplePairsPath=", "regsPath=")

  val sparkOutputCompression = false

  def sparkMain(spark: SparkSession) {

    val samplePairsPath = mandatoryOption("samplePairsPath")
    val regsPath = mandatoryOption("regsPath")
    val output = mandatoryOption("output")
    println(s"samplePairsPath $samplePairsPath")
    println(s"Output $output")

    import spark.implicits._
    val sc = spark.sparkContext
    
    def samplePairsToRates(): RDD[Rate] = {
      val samplePairs = sc.objectFile[SamplePair](samplePairsPath) //.repartition(1000)
      val regs = sc.objectFile[Registration](regsPath)
      RateHelpers.samplePairsToRatesFi(sc, samplePairs, regs)
    }

    val rates = samplePairsToRates()
              //.repartition(1000)
    rates.saveAsObjectFile(output)
  }

}