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
 * Get all used applications with usage counts
 */
object ExtractAppStats extends Spark2Main {

  val shortOptions = ""

  val longOptions = Seq("output=", "ratePath=")

  val sparkOutputCompression = false

  def sparkMain(spark: SparkSession) {

    val ratePath = mandatoryOption("ratePath")
    //val regsPath = mandatoryOption("regsPath")
    val output = mandatoryOption("output")
    println(s"ratePath $ratePath")
    println(s"Output $output")

    import spark.implicits._
    val sc = spark.sparkContext
    
    def loadrates(): RDD[Rate] = {
      sc.objectFile[Rate](ratePath)
    }

    val rates = loadrates()
    val appCounts: RDD[(String, Int)] = rates.flatMap( x => x.allApps().map(app => (app, 1)))
                                        .reduceByKey( _ + _ )
                                        .sortByKey(false) //descending order

    val appCountsLocal = appCounts.collect.map { case (app, count) =>
      s"${app}\t${count}\n"
    }

    import java.io._
    val writer = new PrintWriter(new File(output))

    appCountsLocal.foreach { line =>
      writer.write(line)
    }
    writer.close
  }

}