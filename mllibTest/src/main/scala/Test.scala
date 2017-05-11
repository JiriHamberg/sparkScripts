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

import mllibTest.models.samples._

object Test {

	def initSpark(): SparkSession = {
		val sparkMaster = Properties.envOrNone("SPARK_MASTER").get

		SparkSession.builder()
			.appName("MLlib Test")
			.master(sparkMaster)
			.getOrCreate()
	}

	def main(args: Array[String]): Unit = {

		implicit val spark = initSpark()
		implicit val sc = spark.sparkContext
		implicit val sqlContext = new org.apache.spark.sql.SQLContext(sc)

		val dataPath = "/home/carat/singlesamples-from-2016-08-26-to-2016-10-03-facebook-and-features-text-discharge-noinfs.csv"

		val minSupport = args(0).toDouble
		val minConfidence = args(1).toDouble
		val ruleOutFile = args(2)

		val samples = Sample.parseCSV(dataPath, sep = ";")
		val features = Discretization.getFeatures(samples)

		val fpg = new FPGrowth()
			.setMinSupport(minSupport)
			.setNumPartitions(10)
		val model = fpg.run(features)

		/*model.freqItemsets.collect().foreach { itemset =>
			println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
		}*/

		val outFile = new File(ruleOutFile)
		val writer = new BufferedWriter(new FileWriter(outFile))

		model.generateAssociationRules(minConfidence).collect().foreach { rule =>
			/*println(
			rule.antecedent.mkString("[", ",", "]")
				+ " => " + rule.consequent .mkString("[", ",", "]")
				+ ", " + rule.confidence)
			*/
			val antecedents = rule.antecedent.mkString(",")
			val consequents = rule.consequent.mkString(",")
			writer.write(s"${antecedents}\t${consequents}\n")
		}

	}

}