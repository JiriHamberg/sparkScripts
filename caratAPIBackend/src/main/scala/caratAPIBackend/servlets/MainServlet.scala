
package caratAPIBackend.servlets

import scala.util.Try
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._

import com.typesafe.config._

import org.scalatra._
import org.scalatra.FutureSupport

import caratAPIBackend.services.SparkRunner
//import scalate.ScalateSupport
//import org.fusesource.scalate.{ TemplateEngine, Binding }
//import org.fusesource.scalate.layout.DefaultLayoutStrategy

import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._

class MainServlet extends ScalatraServlet with FutureSupport with JacksonJsonSupport {

	val conf = ConfigFactory.load()
	override val asyncTimeout = conf.getInt("timeout") seconds
	protected implicit lazy val jsonFormats: Formats = DefaultFormats
	implicit val executor =  ExecutionContext.global

	before() {
    contentType = formats("json")
  }

	get("/") {
		val minSupport = Try(params("minSupport").toDouble).toOption
		val minConfidence = Try(params("minConfidence").toDouble).toOption
    val excluded = Try(params("excluded")).toOption.getOrElse("")

		//contentType =  formats("json") //"application/json"

		SparkRunner.runSpark(
			minSupport = minSupport,
			minConfidence = minConfidence,
      excluded = excluded
		)
	}

}
