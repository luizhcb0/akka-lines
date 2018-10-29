package br.com.bb.horus.microservice.lines.executors

import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.{Executor, ExecutorResult, JSON}
import br.com.bb.horus.core.model.operation.Type.PullInternalOperationType
import br.com.bb.horus.core.model.payload.{FailPayload, InputPayload, OutputPayload}
import br.com.bb.horus.microservice.lines.dao.LineDAO
import br.com.bb.horus.microservice.lines.executors.LinesPriorizeUserExecutor.LinesPriorizeUserOutputPayload
import br.com.bb.horus.microservice.lines.executors.LinesUpdateExecutor.LinesUpdateInputPayload
import br.com.bb.horus.microservice.lines.models.{Line, User}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

/** LinesUpdateExecutor companion object.
  *
  * This object provide access to the LinesUpdateOperation, input and output payloads, and other stuffs.
  *
  *
  */
object LinesUpdateExecutor {
  lazy val linesUpdatePullOperation: PullInternalOperationType[LinesUpdateInputPayload] =
    new PullInternalOperationType[LinesUpdateInputPayload](
      operationName = "update",
      serviceName = None,
      typeDirection = None,
      dataType = JSON,
      executor = LinesUpdateExecutor()
    )


  final case class LinesUpdateInputPayload(key: String,
                                           name: String,
                                           metadata: String) extends InputPayload

  final case class LinesUpdateOutputPayload(key: String,
                                            name: String,
                                            timestamp: String,
                                            metadata: String) extends OutputPayload


  def apply(): LinesUpdateExecutor = new LinesUpdateExecutor()


  private implicit class ThrowableExtension(t: Throwable) {

    import java.io.{PrintWriter, StringWriter}

    def stackAsString: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }

}

/** LinesUpdateExecutor class.
  *
  * This class provide business logic for LinesUpdateOperation.
  *
  *
  */
class LinesUpdateExecutor extends Executor[LinesUpdateInputPayload] with LazyLogging {


  case  class respUpdateLine ( status:Boolean, resp:String, tstamp:Long )

  import LinesUpdateExecutor._

  override def run(wire: Wire, payload: LinesUpdateInputPayload): Future[ExecutorResult] = {

    import br.com.bb.horus.core.util.SerializationUtil
    import scala.util.{Failure, Success, Try}
    import br.com.bb.horus.core.functions.Producer

    import java.util.SimpleTimeZone
    val timestamp: String = System.currentTimeMillis.formatted(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC").toString)


    Future {
      try {

        val redis = LineDAO.getInstance()

        val updtLine = update(redis, payload.key, payload.name, payload.metadata)
        if (updtLine.status) {

          val retPayload: LinesUpdateOutputPayload = LinesUpdateOutputPayload(key = payload.key,
            name = payload.name, timestamp = updtLine.toString, metadata = payload.metadata)

          SerializationUtil.toJSON[LinesUpdateOutputPayload](retPayload) match {
            case Success(des) => Producer.doPushInternalToService(wire, des.getBytes())
              ExecutorResult(success = true, taskDescription = Some("Line atualizada " + payload.key + " com sucesso"))

            case Failure(error) => Producer.doFail(wire, FailPayload(-302, error.getMessage))
              ExecutorResult(success = false, taskDescription = Some(error.getMessage))
          }
        }
        else {
          Producer.doFail(wire, FailPayload(-302, updtLine.resp))
          ExecutorResult(success = false, taskDescription = Some(updtLine.resp))
        }
      }
    }

  }
//updateLine(redis, payload.key, payload.name, payload.metadata)

  def update ( redis:LineDAO, key:String, name:String, metadata:String ):respUpdateLine = {

    val _debug = false

    if ( _debug ) {
      respUpdateLine(status = false,resp = "Debug", tstamp = 0)
    }
    else {
      val timest = System.currentTimeMillis.toLong
      val ln: Line = Line(key, name, metadata, timest, timest)

      try {

        if (redis.update(key, ln)) {

          respUpdateLine(status = true, resp = "Atualizou a linha " + key, tstamp = timest)
        }
        else {
          respUpdateLine(status = false, resp = "Não encontrou a linha " + key, tstamp = timest)
        }
      } catch {
        case e: Exception => logger.debug("Não conseguiu Atualizar:" + e.getMessage)
          respUpdateLine(status = false, resp = e.getMessage, tstamp = timest)
      }
    }
  }

}
