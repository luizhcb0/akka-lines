package br.com.bb.horus.microservice.lines.executors

import br.com.bb.horus.core.functions.Producer
import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.{Executor, ExecutorResult, JSON}
import br.com.bb.horus.core.model.operation.Type.PullInternalOperationType
import br.com.bb.horus.core.model.payload.{FailPayload, InputPayload, OutputPayload}
import br.com.bb.horus.core.util.SerializationUtil
import br.com.bb.horus.microservice.lines.dao.LineDAO
import br.com.bb.horus.microservice.lines.executors.LinesListExecutor.LinesListInputPayload
import br.com.bb.horus.microservice.lines.models.Line
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** LinesListExecutor companion object.
  *
  * This object provide access to the LinesListOperation, input and output payloads, and other stuffs.
  *
  *
  */
object LinesListExecutor {
  lazy val linesListPullOperation: PullInternalOperationType[LinesListInputPayload] =
    new PullInternalOperationType[LinesListInputPayload](
      operationName = "list",
      serviceName = None,
      typeDirection = None,
      dataType = JSON,
      executor = LinesListExecutor()
    )

  final case class LinesListInputPayload(metadata: Array[String]) extends InputPayload

  final case class LinesListOutputPayload(lines: Array[String]) extends OutputPayload

  def apply(): LinesListExecutor = new LinesListExecutor()

  private implicit class ThrowableExtension(t: Throwable) {

    import java.io.{PrintWriter, StringWriter}

    def stackAsString: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }
}

/** LinesListExecutor class.
  *
  * This class provide business logic for LinesListOperation.
  *
  *
  */
class LinesListExecutor extends Executor[LinesListInputPayload] with LazyLogging {

  import LinesListExecutor._
  implicit val ec: ExecutionContext = ExecutionContext.global

  override def run(wire: Wire, payload: LinesListInputPayload): Future[ExecutorResult] = {
    val list = LineDAO.getInstance().getAll.filter(l => {
      if(payload.metadata.isEmpty)
        true
      else
        l.metadata.intersect(payload.metadata).length > 0
    })
    sendSuccessResult(wire, list, payload)
  }

  def sendSuccessResult(wire: Wire, list: Array[Line], payload: LinesListInputPayload): Future[ExecutorResult] ={
    val outputObj = LinesListOutputPayload(list.map(l => l.name))

    SerializationUtil.toJSON[LinesListOutputPayload](outputObj) match {
      case Success(output) =>
        Producer.doPushInternalToInstance(wire, output.getBytes) map {
          ack =>
            logger.debug("Enviando lista de filas.")
            ExecutorResult(success = ack.ack, taskDescription = ack.message)
        } recover {
          case e => ExecutorResult(success = false, taskDescription = Some(e.getMessage))
        }
      case Failure(e) =>
        sendFailResult(wire,500,"Falha ao serializar resposta de listagem de filas.", Some(e))
    }
  }

  def sendFailResult(wire: Wire, errorCode: Int, msg: String, error: Option[Throwable]): Future[ExecutorResult] ={
    Producer.doFail(wire, FailPayload(errorCode, msg))

    error match {
      case Some(e) =>
        logger.error("{} : {}", msg, e.getMessage)
        Future(ExecutorResult(success = false, taskDescription = Some(e.getMessage)))
      case None =>
        logger.error(msg)
        Future(ExecutorResult(success = false, taskDescription = Some(msg)))
    }
  }

}
