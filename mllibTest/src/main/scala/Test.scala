import scala.util.Properties

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD

import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.mllib.fpm.FPGrowth



object Test {

	/*def initSpark(): SparkContext = {
		val sparkMaster = Properties.envOrNone("SPARK_MASTER").get

		val conf = new SparkConf()
			.setAppName("MLlib Test")
			.setMaster(sparkMaster)

		new SparkContext(conf)
	}*/

	def initSpark(): SparkSession = {
		val sparkMaster = Properties.envOrNone("SPARK_MASTER").get

		SparkSession.builder()
			.appName("MLlib Test")
			.master(sparkMaster)
			.getOrCreate()
	}

	def discretizeCol(df: DataFrame, colName: String, numBuckets: Int): DataFrame = {
		val discretizer = new QuantileDiscretizer()
			.setInputCol(colName)
			.setOutputCol(s"${colName}D")
			.setNumBuckets(numBuckets)

		discretizer.fit(df).transform(df).drop(colName)
	}

	def dataFrameToFeatures(df: DataFrame): RDD[Array[String]] = {
		val names = df.schema.fields.map(field => field.name)
		val len = names.length
		df.rdd.map { row =>
			(for( i <- 0 to (len - 1) ) yield s"${names(i)}=${row(i)}").toArray
		}
	}

	def main(args: Array[String]): Unit = {

		val spark = initSpark()

		val dataPath = "/home/carat/singlesamples-from-2016-08-26-to-2016-10-03-facebook-and-features-text-discharge-noinfs.csv"

		val schema = StructType(
			Seq(
				StructField("rate", DoubleType, false),
				StructField("cpu", DoubleType, false),
				StructField("distance", DoubleType, false),
				StructField("temp", DoubleType, false),
				StructField("voltage", DoubleType, false),
				StructField("screen", DoubleType, false), //ShortType really
				StructField("mobileNetwork", StringType, false), 
				StructField("network", StringType, false),
				StructField("wifiStrength", DoubleType, false), //ShortType really
				StructField("wifiSpeed", DoubleType, false)  //ShortType really
			)
		)

		var samples = spark.read
			.format("com.databricks.spark.csv")
			.option("header", "false")
			.option("delimiter", ";")
			.schema(schema)
			.load(dataPath)

		samples = discretizeCol(samples, "rate", 4)
		samples = discretizeCol(samples, "cpu", 4)
		samples = discretizeCol(samples, "distance", 4)
		samples = discretizeCol(samples, "temp", 4)
		samples = discretizeCol(samples, "voltage", 4)
		//this is wrong - negative value = automatic brightness should be handled separately
		samples = discretizeCol(samples, "screen", 4)
		samples = discretizeCol(samples, "wifiStrength", 4)
		samples = discretizeCol(samples, "wifiSpeed", 4)
		//println(samples.show())

		//convert to features
		val features = dataFrameToFeatures(samples)
		println(features.take(5))

		val fpg = new FPGrowth()
			.setMinSupport(0.005)
			.setNumPartitions(10)
		val model = fpg.run(features)

		model.freqItemsets.collect().foreach { itemset =>
			println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
		}

		val minConfidence = 0.7
		model.generateAssociationRules(minConfidence).collect().foreach { rule =>
			println(
			rule.antecedent.mkString("[", ",", "]")
				+ " => " + rule.consequent .mkString("[", ",", "]")
				+ ", " + rule.confidence)
		}

	}

}