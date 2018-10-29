package br.com.bb.horus.microservice.lines.executors

import br.com.bb.horus.core.functions.Producer
import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.{Executor, ExecutorResult, JSON}
import br.com.bb.horus.core.model.operation.Type.PullInternalOperationType
import br.com.bb.horus.core.model.payload.{FailPayload, InputPayload, OutputPayload}
import br.com.bb.horus.core.util.SerializationUtil
import br.com.bb.horus.microservice.lines.dao.LineDAO
import br.com.bb.horus.microservice.lines.executors.LinesPushUserExecutor.LinesPushUserInputPayload
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** LinesPushUserExecutor companion object.
  *
  * This object provide access to the LinesPushUserOperation, input and output payloads, and other stuffs.
  *
  *
  */
object LinesPushUserExecutor {
  lazy val linesPushUserPullOperation: PullInternalOperationType[LinesPushUserInputPayload] =
    new PullInternalOperationType[LinesPushUserInputPayload](
      operationName = "push-user",
      serviceName = None,
      typeDirection = None,
      dataType = JSON,
      executor = LinesPushUserExecutor()
    )


  final case class LinesPushUserInputPayload(key: String,
                                             userChannel: String,
                                             userID: String,
                                             userMetadata: String,
                                             priority: Int,
                                             atEnd: Boolean) extends InputPayload

  final case class LinesPushUserOutputPayload(key: String,
                                              userChannel: String,
                                              userID: String,
                                              userMetadata: String,
                                              priority: Int,
                                              timestamp: String) extends OutputPayload


  def apply(): LinesPushUserExecutor = new LinesPushUserExecutor()


  private implicit class ThrowableExtension(t: Throwable) {

    import java.io.{PrintWriter, StringWriter}

    def stackAsString: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }

}

/** LinesPushUserExecutor class.
  *
  * This class provide business logic for LinesPushUserOperation.
  *
  *
  */
class LinesPushUserExecutor extends Executor[LinesPushUserInputPayload] with LazyLogging {

  import LinesPushUserExecutor._
  implicit val ec: ExecutionContext = ExecutionContext.global

  override def run(wire: Wire, payload: LinesPushUserInputPayload): Future[ExecutorResult] = {
    LineDAO.getInstance().pushUser(payload.key, payload.userChannel, payload.userID, payload.userMetadata, payload.priority, payload.atEnd) match {
      case Some(user) => sendSuccessResult(wire, payload)
      case None       => sendFailResult(wire,500,"Erro ao inserir usuário.", None)
    }
  }

  def sendSuccessResult(wire: Wire, payload: LinesPushUserInputPayload): Future[ExecutorResult] ={
    val outputObj = LinesPushUserOutputPayload(payload.key, payload.userChannel, payload.userID, payload.userMetadata, payload.priority,
      "" + System.currentTimeMillis())

    logger.debug("Usuário {} no canal {} inserido com sucesso na fila {}. Prioridade: {}.",
      payload.userID, payload.userChannel, payload.key, payload.priority)

    SerializationUtil.toJSON[LinesPushUserOutputPayload](outputObj) match {
      case Success(output) =>
        Producer.doPushInternalToInstance(wire, output.getBytes) map {
          ack =>
            logger.debug("Enviando mensagem de sucesso de inserção do usuário {}-{} na fila {}.",
              payload.userID, payload.userChannel, payload.key)
            ExecutorResult(success = ack.ack, taskDescription = ack.message)
        } recover {
          case e => ExecutorResult(success = false, taskDescription = Some(e.getMessage))
        }
      case Failure(e) =>
        sendFailResult(wire,500,"Falha ao serializar resposta de inserção de usuário.", Some(e))
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
