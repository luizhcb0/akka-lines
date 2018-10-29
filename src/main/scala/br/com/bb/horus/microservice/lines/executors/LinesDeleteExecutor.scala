package br.com.bb.horus.microservice.lines.executors

import br.com.bb.horus.core.functions.Producer
import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.{Executor, ExecutorResult, JSON}
import br.com.bb.horus.core.model.operation.Type.PullInternalOperationType
import br.com.bb.horus.core.model.payload.{FailPayload, InputPayload, OutputPayload}
import br.com.bb.horus.core.util.SerializationUtil
import br.com.bb.horus.microservice.lines.dao.LineDAO
import br.com.bb.horus.microservice.lines.executors.LinesDeleteExecutor.LinesDeleteInputPayload
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** LinesDeleteExecutor companion object.
  *
  * This object provide access to the LinesDeleteOperation, input and output payloads, and other stuffs.
  *
  *
  */
object LinesDeleteExecutor {
  lazy val linesDeletePullOperation: PullInternalOperationType[LinesDeleteInputPayload] =
    new PullInternalOperationType[LinesDeleteInputPayload](
      serviceName = None,
      operationName = "delete",
      typeDirection = None,
      dataType = JSON,
      executor = LinesDeleteExecutor()
    )


  final case class LinesDeleteInputPayload(key: String) extends InputPayload

  final case class LinesDeleteOutputPayload(key: String) extends OutputPayload


  def apply(): LinesDeleteExecutor = new LinesDeleteExecutor()


  private implicit class ThrowableExtension(t: Throwable) {

    import java.io.{PrintWriter, StringWriter}

    def stackAsString: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }

}

/** LinesDeleteExecutor class.
  *
  * This class provide business logic for LinesDeleteOperation.
  *
  *
  */
class LinesDeleteExecutor extends Executor[LinesDeleteInputPayload] with LazyLogging {

  import LinesDeleteExecutor._
  implicit val ec: ExecutionContext = ExecutionContext.global

  override def run(wire: Wire, payload: LinesDeleteInputPayload): Future[ExecutorResult] = {
    val p = Promise[ExecutorResult]()

    Future{
      val delResult = LineDAO.getInstance().remove(payload.key)

      if(delResult)
        sendSuccessResult(wire, payload) onComplete {
          case Success(ret) => p success ret
          case Failure(e)   => p success ExecutorResult(success = false, taskDescription = Some(e.getMessage))
        }
      else
        sendFailResult(wire, 404, "Erro ao remover fila. Fila não existe.", None)
    }

    p.future
  }

  def sendSuccessResult(wire: Wire, payload: LinesDeleteInputPayload): Future[ExecutorResult] ={
    val p = Promise[ExecutorResult]()

    val outputObj = LinesDeleteOutputPayload(payload.key)

    logger.debug("Fila {} removida com sucesso.", payload.key)

    SerializationUtil.toJSON[LinesDeleteOutputPayload](outputObj) match {
      case Success(output) =>
        Producer.doPushInternalToInstance(wire, output.getBytes) onComplete {
          case Success(ack) =>
            logger.debug("Enviando mensagem de sucesso de remoção da fila {}.", payload.key)
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
