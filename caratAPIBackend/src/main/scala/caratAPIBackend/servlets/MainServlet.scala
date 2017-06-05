
package caratAPIBackend.servlets

import scala.util.Try
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._

import com.typesafe.config._

import org.scalatra._
import org.scalatra.FutureSupport

import caratAPIBackend.services.SparkRunner
import scalate.ScalateSupport
import org.fusesource.scalate.{ TemplateEngine, Binding }
import org.fusesource.scalate.layout.DefaultLayoutStrategy

class MainServlet extends ScalatraServlet with FutureSupport {
	
	val conf = ConfigFactory.load()
	override val asyncTimeout = conf.getInt("timeout") seconds

	implicit val executor =  ExecutionContext.global

	get("/") {
		val minSupport = params.get("minSupport").map(_.toDouble)
		val minConfidence = params.get("minConfidence").map(_.toDouble)

		contentType = "application/json"

		SparkRunner.runSpark(
			minSupport = minSupport, 
			minConfidence = minConfidence
		)
	}

}