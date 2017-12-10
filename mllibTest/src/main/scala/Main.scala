import scala.util.Properties

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.mllib.fpm.AssociationRules.Rule

import java.io._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import mllibTest.utils.TimeUtils
import mllibTest.models.samples._


object Main {

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

  /* Filter out rules that are supersets of other rules with same consequents and confidence.
   * We are always interested about the most general rules.
   *
   */
  def pruneRules(rules: RDD[Rule[String]]): RDD[Rule[String]] = {
    val pruneCandidateGroups = rules.groupBy(rule => (rule.consequent.sorted.mkString, rule.confidence))

    pruneCandidateGroups.flatMap { case (key, group) =>
      val groupSorted = group.toSeq.sortBy(rule => rule.consequent.length)
      var groupAsSets = group.map(rule => (rule, rule.antecedent.toSet))

      val toPrune: Set[Rule[String]] = (for {
        testRule <- groupAsSets
        otherRule <- groupAsSets
        if testRule != otherRule && testRule._2.subsetOf(otherRule._2)
      } yield(otherRule._1)).toSet

      groupSorted.filter(rule => !toPrune.contains(rule))
    }

  }

  //def pruneRules(rules: RDD[Rule[String]]): RDD[Rule[String]] = rules

	def main(args: Array[String]): Unit = {

		implicit val spark = initSpark()
		implicit val sc = spark.sparkContext
		implicit val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //val ratePath = "/cs/work/group/carat/jirihamb/single-samples-2016-06-22-until-2016-08-22-cc-mcc-obj-rates/"
    val ratePath = "/dev/shm/tmp/spark/jirihamb/single-samples-2016-06-22-until-2016-08-22-cc-mcc-obj-rates/"


    if (args.length < 3) {
      throw new Exception("Invalid number of arguments.")
    }

    val applicationName = args(0)
		val minSupport = args(1).toDouble
		val minConfidence = args(2).toDouble
    val excluded: Set[String] = if (args.length < 4) Set() else args(3).trim.split(",").toSet

    val samples = readCaratRates(ratePath).collect {
      case rate if rate.allApps().contains(applicationName) => Sample.fromCaratRate(rate)
    }

    val (features, bins) = Discretization.getFeatures(samples, excluded)

		val fpg = new FPGrowth()
			.setMinSupport(minSupport)
			//.setNumPartitions(10)
    val (model, fPGrowthRunTime) = TimeUtils.timeIt{ fpg.run(features) }

    val rulesFiltered = model.generateAssociationRules(minConfidence)
		//take only rules which contain energy rate in the consequent
		.filter { rule =>
			rule.consequent.find { item =>
				item.startsWith("rate=")
			}.isDefined
		}

    val rulesPruned = pruneRules(rulesFiltered)
		//sort rules by descending confidence
		.sortBy( - _.confidence)

    val rulesJSON = rulesPruned.map { rule =>
			("antecedents" -> rule.antecedent.toSeq) ~
			("consequents" -> rule.consequent.toSeq) ~
			("confidence" -> rule.confidence)
		}

		val (rules, time) = TimeUtils.timeIt(rulesJSON.collect().toSeq)


		/* By default a Map will become a list of objects
		 * with one field each when converted to JSON.
		 * We of course want one object with one field
		 * per key value pair of the Map.
		 */
		val binsFormatted = bins.foldLeft(JObject()) {
			case (prev, (k, v)) => prev ~ (k -> v)
		}

		val rendered = pretty(render {
      ("applicationName" -> applicationName) ~
      ("minSupport" -> minSupport) ~
      ("minConfidence" -> minConfidence) ~
			("FPGrowthRunTime" -> fPGrowthRunTime) ~
      ("executionTime" -> time) ~
			("bins" -> binsFormatted) ~
			("rules" -> rules)
		})

		println(rendered)

	}

}
