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
object SamplesToRates extends Spark2Main {

  val shortOptions = ""

  val longOptions = Seq("output=", "samplesPath=", "regsPath=")

  val sparkOutputCompression = false

  def sparkMain(spark: SparkSession) {

    val ratePath = mandatoryOption("samplesPath")
    val regsPath = mandatoryOption("regsPath")
    val output = mandatoryOption("output")
    println(s"ratePath $ratePath")
    println(s"Output $output")

    import spark.implicits._
    val sc = spark.sparkContext
    
    def samplesToRates(): RDD[Rate] = {
      val samples = sc.objectFile[Sample](ratePath) //.repartition(1000)
      val regs = sc.objectFile[Registration](regsPath)
      val android = samples.filter { x => x.uuid.length() <= 16 }
      val samplepairs = RateHelpers.samplesToSamplePairs(android)
      val rates = RateHelpers.samplePairsToRatesFi(sc, samplepairs, regs)
      rates
    }

    val rates = samplesToRates()
                //.repartition(1000)
                .saveAsObjectFile(output)
  }

}