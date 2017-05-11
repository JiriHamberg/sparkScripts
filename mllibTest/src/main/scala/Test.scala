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

//import mllibTest.models.discretization._

import mllibTest.models.samples._

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

	/*def discretizeCol(df: DataFrame, colName: String, numBuckets: Int): DataFrame = {
		val discretizer = new QuantileDiscretizer()
			.setInputCol(colName)
			.setOutputCol(s"${colName}D")
			.setNumBuckets(numBuckets)

		discretizer.fit(df).transform(df).drop(colName)
	}*/

/*	def applyQuantileDiscretization(
		df: DataFrame,
		colName: String,
		numBuckets: Int,
		relativeError: Double = 0.05,
		specialCase: PartialFunction[Double, String] = Map.empty)
		(implicit spark: SparkSession): DataFrame = {
		
		val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
		import sqlContext.implicits._
		import org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID

		val discretization = new MyQuantileDiscretizer(numBuckets, relativeError)
		val newCol: RDD[String] = discretization.discretize( df.select(colName).rdd.map(row => row.getAs[Double](colName) ), sqlContext, specialCase = specialCase )
		val asDF = newCol.toDF(colName).withColumn("id", MonotonicallyIncreasingID())
		df.drop(colName).withColumn("id", MonotonicallyIncreasingID()).join(asDF, "id", "outer").drop("id") //.withColumn(colName, newCol.toDF("TEMP")("TEMP"))
	}*/

	/*def dataFrameToFeatures(df: DataFrame): RDD[Array[String]] = {
		val names = df.schema.fields.map(field => field.name)
		val len = names.length
		df.rdd.map { row =>
			(for( i <- 0 to (len - 1) ) yield s"${names(i)}=${row(i)}").toArray
		}
	}*/

	def main(args: Array[String]): Unit = {

		implicit val spark = initSpark()
		implicit val sc = spark.sparkContext
		implicit val sqlContext = new org.apache.spark.sql.SQLContext(sc)

		val dataPath = "/home/carat/singlesamples-from-2016-08-26-to-2016-10-03-facebook-and-features-text-discharge-noinfs.csv"
		//args.map(println)
		val minSupport = args(0).toDouble
		val minConfidence = args(1).toDouble
		val ruleOutFile = args(2)

		/*val schema = StructType(
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

		val discretization = new QuantileDiscretization(4)

		//samples = discretization.discretize(samples, "rate")  //discretizeCol(samples, "rate", 4)
		samples = applyQuantileDiscretization(samples, "rate", 4)
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
		//println(features.take(5))
		*/

		val samples = Sample.parseCSV(dataPath, sep = ";")

		val rates = samples.map(_.rate)

		//val rateQuantiles = Discretization.getQuantiles(rates, 4)

		//samples.take(5).map(println)

		//rateQuantiles.map(println)

		val features = Discretization.getFeatures(samples)

		//features.take(10).map(_.mkString(" ")).map(println)

		val fpg = new FPGrowth()
			.setMinSupport(minSupport)  //.setMinSupport(0.005)
			.setNumPartitions(10)
		val model = fpg.run(features)

		/*model.freqItemsets.collect().foreach { itemset =>
			println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
		}*/

		val outFile = new File(ruleOutFile)
		val writer = new BufferedWriter(new FileWriter(outFile))

		//val minConfidence = 0.7
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