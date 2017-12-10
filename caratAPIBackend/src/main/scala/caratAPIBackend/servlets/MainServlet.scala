
package caratAPIBackend.servlets

import scala.util.Try
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._

import com.typesafe.config._

import org.scalatra._
import org.scalatra.FutureSupport

import caratAPIBackend.services.{ SparkRunner }
//import scalate.ScalateSupport
//import org.fusesource.scalate.{ TemplateEngine, Binding }
//import org.fusesource.scalate.layout.DefaultLayoutStrategy

import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._

class MainServlet extends ScalatraServlet with FutureSupport with JacksonJsonSupport {

	val conf = ConfigFactory.load()
	override val asyncTimeout = conf.getInt("timeout") seconds
	protected implicit lazy val jsonFormats: Formats = DefaultFormats
	implicit val executor = ExecutionContext.global

	before() {
    contentType = formats("json")
  }


  /* Returns preprocessed app usage
   * information in JSON format.
   *
   */
  /*get("/app-stats") {
    AppStats.getAppStats
  }*/

  /* Submits a association rule mining job to spark
   * and returns the discovered rules in JSON format.
   *
   */
	get("/") {

    println("ALIVE 1")

    val applicationName = Try(params("applicationName")).toOption
		val minSupport = Try(params("minSupport").toDouble).toOption
		val minConfidence = Try(params("minConfidence").toDouble).toOption
    val excluded = Try(params("excluded")).toOption.getOrElse("")

    println("ALIVE 2")

    applicationName.map { applicationName =>
      SparkRunner.runSpark(
        applicationName,
        minSupport = minSupport,
        minConfidence = minConfidence,
        excluded = excluded
      )
    }.getOrElse {
      BadRequest(reason = "Missing 'applicationName'")
    }
	}

}
