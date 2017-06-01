package mllibTest.models.samples

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

case class Sample(
	rate: Double,
	cpu: Double,
	distance: Double,
	temp: Double,
	voltage: Double,
	screen: Double,
	mobileNetwork: String, 
	network: String,
	wifiStrength: Double,
	wifiSpeed: Double
)
object Sample {
	def parseCSV(filePath: String, sep: String = ",")(implicit sc: SparkContext): RDD[Sample] = {
		sc.textFile(filePath).map{ line => 
			val cols = line.trim.split(sep)
			Sample(
				rate = cols(0).toDouble,
				cpu = cols(1).toDouble,
				distance = cols(2).toDouble,
				temp = cols(3).toDouble,
				voltage = cols(4).toDouble,
				screen = cols(5).toDouble,
				mobileNetwork = cols(6),
				network = cols(7),
				wifiStrength = cols(8).toDouble,
				wifiSpeed = cols(9).toDouble
			) 
		}
	}
}

object Discretization {

	private def getQuantiles(
		data: RDD[Double],
		buckets: Int,
		relativeError: Double = 0.05,
		partial: PartialFunction[Double, Option[String]] = Map.empty)
		(implicit sqlContext: SQLContext): Array[Double] = {

		import sqlContext.implicits._

		val percentiles = (for(i <- 1 to (buckets - 1)) yield (1.0 / buckets) * i).toArray
		val notDefined = data.filter(x => !partial.isDefinedAt(x))
		val quantiles = notDefined.toDF("col").stat.approxQuantile("col", percentiles, relativeError)
		notDefined.toDF("col").stat.approxQuantile("col", percentiles, relativeError)
	}

	private def getFeatureFromQuantiles(dataPoint: Double, featureName: String, quantiles: Array[Double], partial: PartialFunction[Double, Option[String]] = Map.empty): Option[String] = {
		if(partial.isDefinedAt(dataPoint)) return partial(dataPoint).map(x => s"${featureName}=${x}")
		var index = quantiles.indexWhere(q => q >= dataPoint) + 1
		if (index == 0) index += (quantiles.length + 1)
		Some(s"${featureName}=q${index}")
	}

	private def getFeatureFromPartial[T](dataPoint: T, featureName: String, partial: PartialFunction[T, Option[String]]): Option[String] = {
		if(partial.isDefinedAt(dataPoint))
			partial(dataPoint).map(x => s"${featureName}=${x}")
		else 
			None
	}

	def getFeatures(samples: RDD[Sample])(implicit sqlContext: SQLContext): (RDD[Array[String]], scala.collection.mutable.Map[String, Array[Double]]) = {
		val quantiles = scala.collection.mutable.Map[String, Array[Double]]()

		// RATE
		val rateQuantiles = getQuantiles(samples.map(_.rate), 4)
		quantiles("rate") = rateQuantiles
		
		// CPU
		val cpuPartial: PartialFunction[Double, Option[String]] = {
			case x if x < 0.0 => None
			case x if x > 100.0 => None
		}
		val cpuQuantiles = getQuantiles(samples.map(_.cpu), 4, partial = cpuPartial)
		quantiles("cpu") = cpuQuantiles

		// TRAVEL
		val travelDistancePartial: PartialFunction[Double, Option[String]] = {
			case x if x >= 100 => Some("yes")
			case x if x < 100 => Some("no")
		}

		// TEMPERATURE
		val temperaturePartial: PartialFunction[Double, Option[String]] = {
			case x if x < 5 => None
			case x if x > 100 => None
		}
		val temperatureQuantiles = getQuantiles(samples.map(_.temp), 4, partial = temperaturePartial)
		quantiles("temperature") = temperatureQuantiles

		// VOLTAGE
		val voltageQuantiles = getQuantiles(samples.map(_.voltage), 3)

		// SCREEN
		val screenPartial: PartialFunction[Double, Option[String]] = {
			case x if x == -1 => Some("auto")
			case x if x < -1 => None
			case x if x > 255 => None
		}
		val screenQuantiles = getQuantiles(samples.map(_.screen), 4, partial = screenPartial)
		quantiles("screen") = screenQuantiles

		// MOBILE NETWORK TYPE
		val mobileNetworkPartial: PartialFunction[String, Option[String]] = {
			case "unknown" | "null" | "0" | "16" | "18" | "19" | "30" => Some("unknown")
			case x => Some(x)
		}

		// NETWORK TYPE
		val networkPartial: PartialFunction[String, Option[String]] = {
			case "unknown" | "null" => Some("unknown")
			case x => Some(x)
		}

		//WIFI STRENGTH
		val wifiStrengthPartial: PartialFunction[Double, Option[String]] = {
			case x if x < -100 => None
			case x if x > 0 => None
		}
		val wifiStrengthQuantiles = getQuantiles(samples.map(_.wifiStrength), 4, partial = wifiStrengthPartial)
		quantiles("wifiStrength") = wifiStrengthQuantiles

		//WIFI SPEED
		val wifiSpeedPartial: PartialFunction[Double, Option[String]] = {
			case x if x < 0 => None
		}
		val wifiSpeedQuantiles = getQuantiles(samples.map(_.wifiSpeed), 4, partial = wifiSpeedPartial)
		quantiles("wifiSpeed") = wifiSpeedQuantiles

		val features = samples.map { sample => 
			Array(
				getFeatureFromQuantiles(sample.rate, "rate", rateQuantiles),
				getFeatureFromQuantiles(sample.cpu, "cpu", cpuQuantiles, partial = cpuPartial),
				getFeatureFromQuantiles(sample.distance, "distance", Array.empty, partial = travelDistancePartial),
				getFeatureFromQuantiles(sample.temp, "temp", temperatureQuantiles, partial = temperaturePartial),
				getFeatureFromQuantiles(sample.voltage, "voltage", voltageQuantiles),
				getFeatureFromQuantiles(sample.screen, "screen", screenQuantiles, partial = screenPartial),
				getFeatureFromPartial(sample.mobileNetwork, "mobileNetType", mobileNetworkPartial),
				getFeatureFromPartial(sample.network, "netType", networkPartial),
				getFeatureFromQuantiles(sample.wifiStrength, "wifiStrength", wifiStrengthQuantiles, partial = wifiStrengthPartial),
				getFeatureFromQuantiles(sample.wifiSpeed, "wifiSpeed", wifiSpeedQuantiles, partial = wifiSpeedPartial)
			).collect { case Some(s) => s }
		}
		(features, quantiles)
	}

}