package br.com.bb.horus.microservice.lines.executors

import br.com.bb.horus.core.functions.Producer
import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.{Executor, ExecutorResult, JSON}
import br.com.bb.horus.core.model.operation.Type.PullInternalOperationType
import br.com.bb.horus.core.model.payload.{FailPayload, InputPayload, OutputPayload}
import br.com.bb.horus.core.util.SerializationUtil
import br.com.bb.horus.microservice.lines.dao.LineDAO
import br.com.bb.horus.microservice.lines.executors.LinesCreateExecutor.LinesCreateInputPayload
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}


/** LinesCreateExecutor companion object.
  *
  * This object provide access to the LinesCreateOperation, input and output payloads, and other stuffs.
  *
  *
  */
object LinesCreateExecutor {
  lazy val linesCreatePullOperation: PullInternalOperationType[LinesCreateInputPayload] =
    new PullInternalOperationType[LinesCreateInputPayload](
      operationName = "create",
      serviceName = None,
      typeDirection = None,
      dataType = JSON,
      executor = LinesCreateExecutor()
    )

  final case class LinesCreateInputPayload(key: String,
                                           name: String,
                                           metadata: String) extends InputPayload

  final case class LinesCreateOutputPayload(key: String,
                                            name: String,
                                            timestamp: String,
                                            metadata: String) extends OutputPayload

  def apply(): LinesCreateExecutor = new LinesCreateExecutor()

  private implicit class ThrowableExtension(t: Throwable) {

    import java.io.{PrintWriter, StringWriter}

    def stackAsString: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }

}

/** LinesCreateExecutor class.
  *
  * This class provide business logic for LinesCreateOperation.
  *
  *
  */
class LinesCreateExecutor extends Executor[LinesCreateInputPayload] with LazyLogging {

  import LinesCreateExecutor._
  implicit val ec: ExecutionContext = ExecutionContext.global

  override def run(wire: Wire, payload: LinesCreateInputPayload): Future[ExecutorResult] = {
    val p = Promise[ExecutorResult]()

    Future {
      val dao: LineDAO = LineDAO.getInstance()

      dao.insert(payload.key, payload.name, payload.metadata) match {
        case Some(line) =>
          sendSuccessResult(wire, payload) onComplete {
            case Success(ret) => p success ret
            case Failure(e)   => p success ExecutorResult(success = false, taskDescription = Some(e.getMessage))
          }
        case None =>
          sendFailResult(wire, 409, "Erro ao criar fila. Fila já existente.", None)
      }
    }

    p.future
  }

  def sendSuccessResult(wire: Wire, payload: LinesCreateInputPayload): Future[ExecutorResult] ={
    val p = Promise[ExecutorResult]()

    val outputObj = LinesCreateOutputPayload(payload.key, payload.name, "" + System.currentTimeMillis(), payload.metadata)

    logger.debug("Fila {} criada com sucesso.", payload.name)

    SerializationUtil.toJSON[LinesCreateOutputPayload](outputObj) match {
      case Success(output) =>

        Producer.doPushInternalToInstance(wire, output.getBytes) onComplete {
          case Success(ack) =>
            logger.debug("Enviando mensagem de sucesso de criação da fila {}.", payload.name)
            p success ExecutorResult(success = ack.ack, taskDescription = ack.message)
          case Failure(e) =>
            p failure e
        }

      case Failure(e) =>
        p failure e
    }

    p.future
  }

  def sendFailResult(wire: Wire, errorCode: Int, errorCustomMsg: String, fullError: Option[Throwable]): Future[ExecutorResult] ={
    val p = Promise[ExecutorResult]()

    Producer.doFail(wire, FailPayload(errorCode, errorCustomMsg)) onComplete {
      case Success(ack) =>

        fullError match {
          case Some(e) =>
            logger.error("{} : {}", errorCustomMsg, e.getMessage)
            p success ExecutorResult(success = false, taskDescription = Some(e.getMessage))
          case None =>
            logger.error(errorCustomMsg)
            p success ExecutorResult(success = false, taskDescription = Some(errorCustomMsg))
        }

      case Failure(e) =>
        p failure e
    }

    p.future
  }
}
