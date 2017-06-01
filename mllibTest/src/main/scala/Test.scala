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


object Test {

	def initSpark(): SparkSession = {
		val sparkMaster = Properties.envOrNone("SPARK_MASTER").get

		SparkSession.builder()
			.appName("MLlib Test")
			.master(sparkMaster)
			.getOrCreate()
	}

	def timeIt[T](block: => T): (T, Long) = {
		val t0 = System.currentTimeMillis()
		val result = block
		val t1 = System.currentTimeMillis()
		(result, t1 - t0)
	}

	def main(args: Array[String]): Unit = {

		implicit val spark = initSpark()
		implicit val sc = spark.sparkContext
		implicit val sqlContext = new org.apache.spark.sql.SQLContext(sc)

		val dataPath = "/home/carat/singlesamples-from-2016-08-26-to-2016-10-03-facebook-and-features-text-discharge-noinfs.csv"

		val minSupport = args(0).toDouble
		val minConfidence = args(1).toDouble
		//val ruleOutFile = args(2)

		val samples = Sample.parseCSV(dataPath, sep = ";")
		val (features, quantiles) = Discretization.getFeatures(samples)

		val fpg = new FPGrowth()
			.setMinSupport(minSupport)
			.setNumPartitions(10)
		val model = fpg.run(features)

		//val outFile = new File(ruleOutFile)
		//val writer = new BufferedWriter(new FileWriter(outFile))

		val rulesJSON = model.generateAssociationRules(minConfidence)
		//take only rules which contain energy rate in the consequent
		.filter { rule =>
			rule.consequent.find { item =>
				item.startsWith("rate=")
			}.isDefined
		}
		//sort rules by descending confidence
		.sortBy( - _.confidence)
		.map { rule =>
			("antecedents" -> rule.antecedent.toSeq) ~
			("consequents" -> rule.consequent.toSeq) ~
			("confidence" -> rule.confidence)
		}
		
		val (rules, time) = timeIt(rulesJSON.collect().toSeq)

		//val rendered = pretty(render(rulesJSON.collect().toSeq))
		val quantilesFormatted = quantiles.map { case (k,v) =>
			k -> v.toSeq
		}

		val rendered = pretty(render {
			("executionTime" -> time) ~
			("quantiles" -> quantilesFormatted) ~
			("rules" -> rules)
		})

		println(rendered)

		//.foreach { rule =>
		//	val antecedents = rule.antecedent.mkString(",")
		//	val consequents = rule.consequent.mkString(",")
		//	val confidence = rule.confidence
			//writer.write(s"${antecedents}\t${consequents}\t${confidence}\n")
			//println(s"${antecedents}\t${consequents}\t${confidence}")
		//}

		//writer.close()

	}

}