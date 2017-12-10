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

  def fromCaratRate(caratRate: fi.helsinki.cs.nodes.carat.sample.Rate) = {
    Sample(
      rate = caratRate.rate(),
      cpu = caratRate.sp._2.androidFields.cpuUsage,
      distance = caratRate.sp._2.distanceTraveled,
      temp = caratRate.sp._2.androidFields.battery.temperature,
      voltage = caratRate.sp._2.androidFields.battery.voltage,
      screen = caratRate.sp._2.androidFields.screenBrightness,
      mobileNetwork = caratRate.sp._2.androidFields.networkInfo.mobileNetworkType,
      network = caratRate.sp._2.androidFields.networkInfo.networkType,
      wifiStrength = caratRate.sp._2.androidFields.networkInfo.wifiSignalStrength.toDouble,
      wifiSpeed = caratRate.sp._2.androidFields.networkInfo.wifiLinkSpeed.toDouble
    )
  }
}

case class SampleDiscretization(
  rate: Array[Double],
  cpu: Array[Double],
  distance: Array[Double],
	temp: Array[Double],
	voltage: Array[Double],
	screen: Array[Double],
	wifiStrength: Array[Double],
	wifiSpeed: Array[Double]
)

object SampleDiscretization {
  def fromSamples(samples: RDD[Sample], nBins: Int = 4)(implicit sqlContext: SQLContext): SampleDiscretization = {

      SampleDiscretization(
        rate = Discretization.getQuantiles(samples.map(s => s.rate), nBins),
        cpu = Discretization.getQuantiles(samples.map(s => s.cpu), nBins, partial = Discretization.cpuPartial),
        distance = Discretization.getQuantiles(samples.map(s => s.distance), nBins, partial = Discretization.travelDistancePartial),
        temp = Discretization.getQuantiles(samples.map(s => s.temp), nBins, partial = Discretization.temperaturePartial),
        voltage = Discretization.getQuantiles(samples.map(s => s.voltage), nBins),
        screen = Discretization.getQuantiles(samples.map(s => s.screen), nBins, partial = Discretization.screenPartial),
        wifiStrength = Discretization.getQuantiles(samples.map(s => s.wifiStrength), nBins, partial = Discretization.wifiStrengthPartial),
        wifiSpeed = Discretization.getQuantiles(samples.map(s => s.wifiSpeed), nBins, partial = Discretization.wifiSpeedPartial)
      )
  }

}


object Discretization {

  val cpuPartial: PartialFunction[Double, Option[String]] = {
    case x if x < 0.0 => None
    case x if x > 100.0 => None
	}

  val travelDistancePartial: PartialFunction[Double, Option[String]] = {
    case x if x >= 100 => Some("yes")
    case x if x < 100 => Some("no")
  }

  val temperaturePartial: PartialFunction[Double, Option[String]] = {
    case x if x < 5 => None
    case x if x > 100 => None
  }

  val screenPartial: PartialFunction[Double, Option[String]] = {
    case x if x == -1 => Some("auto")
    case x if x < -1 => None
    case x if x > 255 => None
  }

  val mobileNetworkPartial: PartialFunction[String, Option[String]] = {
    case "unknown" | "null" | "0" | "16" | "18" | "19" | "30" => Some("unknown")
    case x => Some(x)
  }

  val networkPartial: PartialFunction[String, Option[String]] = {
    case "unknown" | "null" => Some("unknown")
    case x => Some(x)
  }

  val wifiStrengthPartial: PartialFunction[Double, Option[String]] = {
    case x if x < -100 => None
    case x if x > 0 => None
  }

  val wifiSpeedPartial: PartialFunction[Double, Option[String]] = {
    case x if x < 0 => None
  }

	def getQuantiles(
		data: RDD[Double],
		buckets: Int,
		relativeError: Double = 0.0001,
		partial: PartialFunction[Double, Option[String]] = Map.empty)
		(implicit sqlContext: SQLContext): Array[Double] = {

		import sqlContext.implicits._

		val percentiles = (for(i <- 1 to (buckets - 1)) yield (1.0 / buckets) * i).toArray
		val notDefined = data.filter(x => !partial.isDefinedAt(x))
    //val quantiles = notDefined.toDF("col").stat.approxQuantile("col", percentiles, relativeError)

    try {
		  notDefined.toDF("col").stat.approxQuantile("col", percentiles, relativeError)
    } catch{
      //see https://issues.apache.org/jira/browse/SPARK-21550
      case ex: java.util.NoSuchElementException => Array[Double]()
    }
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

	def getFeatures(samples: RDD[Sample], excluded: Set[String])(implicit sqlContext: SQLContext): (RDD[Array[String]], scala.collection.mutable.Map[String, Seq[Double]]) = {
		val bins = scala.collection.mutable.Map[String, Seq[Double]]()

		// RATE
		val rateQuantiles = getQuantiles(samples.map(_.rate), 4)
		bins("rate") = Seq(0.0) ++ rateQuantiles.toSeq ++ Seq(1.0)

		val cpuQuantiles = getQuantiles(samples.map(_.cpu), 4, partial = cpuPartial)
    if(!excluded.contains("cpu"))
		  bins("cpu") = Seq(0.0) ++ cpuQuantiles.toSeq ++ Seq(100.0)

		// TEMPERATURE
		val temperatureQuantiles = getQuantiles(samples.map(_.temp), 4, partial = temperaturePartial)
		if(!excluded.contains("temperature"))
    bins("temperature") = Seq(5.0) ++ temperatureQuantiles.toSeq ++ Seq(100.0)

		// VOLTAGE
		val voltageQuantiles = getQuantiles(samples.map(_.voltage), 3)

		// SCREEN
		val screenQuantiles = getQuantiles(samples.map(_.screen), 4, partial = screenPartial)
		if(!excluded.contains("screen"))
      bins("screen") = Seq(0.0) ++ screenQuantiles.toSeq ++ Seq(255.0)

		// MOBILE NETWORK TYPE

		// NETWORK TYPE

		//WIFI STRENGTH
		val wifiStrengthQuantiles = getQuantiles(samples.map(_.wifiStrength), 4, partial = wifiStrengthPartial)
		if(!excluded.contains("wifiStrength"))
      bins("wifiStrength") = Seq(-100.0) ++ wifiStrengthQuantiles.toSeq ++ Seq(0.0)

		//WIFI SPEED
		val wifiSpeedQuantiles = getQuantiles(samples.map(_.wifiSpeed), 4, partial = wifiSpeedPartial)
		if(!excluded.contains("wifiSpeed"))
      bins("wifiSpeed") = Seq(0.0) ++ wifiSpeedQuantiles.toSeq ++ Seq(Double.PositiveInfinity)

		val features = samples.map { sample =>
			Array(
				getFeatureFromQuantiles(sample.rate, "rate", rateQuantiles),
				if(!excluded.contains("cpu"))
          getFeatureFromQuantiles(sample.cpu, "cpu", cpuQuantiles, partial = cpuPartial)
        else
          None,
        if(!excluded.contains("distance"))
				  getFeatureFromQuantiles(sample.distance, "distance", Array.empty, partial = travelDistancePartial)
        else None,
        if(!excluded.contains("temp"))
				  getFeatureFromQuantiles(sample.temp, "temp", temperatureQuantiles, partial = temperaturePartial)
        else None,
        if(!excluded.contains("voltage"))
				  getFeatureFromQuantiles(sample.voltage, "voltage", voltageQuantiles)
        else None,
        if(!excluded.contains("screen"))
				  getFeatureFromQuantiles(sample.screen, "screen", screenQuantiles, partial = screenPartial)
        else None,
        if(!excluded.contains("mobileNetType"))
				  getFeatureFromPartial(sample.mobileNetwork, "mobileNetType", mobileNetworkPartial)
        else None,
        if(!excluded.contains("netType"))
				  getFeatureFromPartial(sample.network, "netType", networkPartial)
        else None,
				if(!excluded.contains("wifiStrength"))
          getFeatureFromQuantiles(sample.wifiStrength, "wifiStrength", wifiStrengthQuantiles, partial = wifiStrengthPartial)
        else None,
				if(!excluded.contains("wifiSpeed"))
          getFeatureFromQuantiles(sample.wifiSpeed, "wifiSpeed", wifiSpeedQuantiles, partial = wifiSpeedPartial)
        else None
			).collect { case Some(s) => s }
		}
		(features, bins)
	}

}
