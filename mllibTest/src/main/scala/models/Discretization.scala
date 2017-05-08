package mllibTest.models.discretization

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SQLContext

import org.apache.spark.mllib.clustering.{KMeans}
import org.apache.spark.mllib.linalg.{Vector}

//import sqlContext.implicits._


/*trait Discretization {
	def discretize(df: DataFrame, colName: String): DataFrame
}

class QuantileDiscretization(val numBuckets: Int) extends Discretization {
	override def discretize(df: DataFrame, colName: String) = {
		val tempName = s"${colName}TEMP"
		val discretizer = new QuantileDiscretizer()
			.setInputCol(colName)
			.setOutputCol(tempName)
			.setNumBuckets(numBuckets)
		discretizer
			.fit(df)
			.transform(df)
			.drop(colName)
			.withColumnRenamed(tempName, s"${colName}")
	}
}

class QuantileDiscretization(val numBuckets: Int, val special: () ) extends Discretization {
	override def discretize(df: DataFrame, colName: String) = {
		val tempName = s"${colName}TEMP"
		val discretizer = new QuantileDiscretizer()
			.setInputCol(colName)
			.setOutputCol(tempName)
			.setNumBuckets(numBuckets)
		discretizer
			.fit(df)
			.transform(df)
			.drop(colName)
			.withColumnRenamed(tempName, s"${colName}")
	}
}


class KMeansDiscretization(val numClusters: Int, val numIterations: Int = 20) extends Discretization {
	override def discretize(df: DataFrame, colName: String) = {
		val data = df(colName).rdd.map(x => Vector(x))

		val model = KMeans.train(data, numClusters, numIterations)
		val predicted = model.predict(data)

		df.drop(colName).withColumn(colName, predicted)
	}
}*/

class QuantileDiscretization(val numBuckets: Int) {
	def discretize(df: DataFrame, colName: String) = {
		val tempName = s"${colName}TEMP"
		val discretizer = new QuantileDiscretizer()
			.setInputCol(colName)
			.setOutputCol(tempName)
			.setNumBuckets(numBuckets)
		discretizer
			.fit(df)
			.transform(df)
			.drop(colName)
			.withColumnRenamed(tempName, s"${colName}")
	}
}


trait Discretizer {
	def discretize(data: RDD[Double], sqlContext: SQLContext, specialCase: PartialFunction[Double, String] = Map.empty): RDD[String]
}

/** Discretizes an RDD applying "special rule". 
* 
*
*
*/
class MyQuantileDiscretizer(val buckets: Int, val relativeError: Double = 0.05) extends Discretizer {
	override def discretize(data: RDD[Double], sqlContext: SQLContext, specialCase: PartialFunction[Double, String] = Map.empty) = {
		
		//val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
		//import sqlContext.implicits._
		import sqlContext.implicits._

		val percentiles = (for(i <- 1 to (buckets - 1)) yield (1.0 / buckets) * i).toArray
		val notSpecialCase = data.filter(x => !specialCase.isDefinedAt(x))
		val quantiles = notSpecialCase.toDF("col").stat.approxQuantile("col", percentiles, relativeError)

		data.map { d =>
			if(specialCase.isDefinedAt(d)) specialCase(d)
			else {
				val q = quantiles.indexWhere(q => d >= q)
				s"q${q + 1}"
			}
		}
	}
}
