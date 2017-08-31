
package caratAPIBackend.services

import scala.sys.process._
import scala.concurrent.{Future, ExecutionContext}

import com.typesafe.config._

object SparkRunner {

	val conf = ConfigFactory.load()
	val sparkSubmitScript = conf.getString("sparkSubmit.script")
	val sparkSubmitClass = conf.getString("sparkSubmit.class")
	val sparkSubmitJar = conf.getString("sparkSubmit.jar")
	val defaultMinConfidence = conf.getDouble("sparkSubmit.defaults.minConfidence")
	val defaultMinSupport = conf.getDouble("sparkSubmit.defaults.minSupport")

	implicit val executor =  ExecutionContext.global

	def runSpark(minSupport: Option[Double] = None, minConfidence: Option[Double] = None, excluded: String = ""): Future[String] = {
		val support = minSupport.getOrElse(defaultMinSupport)
		val confidence = minConfidence.getOrElse(defaultMinConfidence)

		Future {
      s"${sparkSubmitScript} ${sparkSubmitClass} ${sparkSubmitJar} ${support} ${confidence} ${excluded}" !!
		}
	}

}
