package br.com.bb.horus.microservice.lines.executors

import br.com.bb.horus.core.functions.Producer
import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.Type.PullInternalOperationType
import br.com.bb.horus.core.model.operation.{Executor, ExecutorResult, JSON}
import br.com.bb.horus.core.model.payload.{FailPayload, InputPayload, OutputPayload}
import br.com.bb.horus.core.util.SerializationUtil
import br.com.bb.horus.microservice.lines.dao.LineDAO
import br.com.bb.horus.microservice.lines.executors.LinesRemoveUserExecutor.LinesRemoveUserInputPayload
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/** LinesRemoveUserExecutor companion object.
  *
  * This object provide access to the LinesRemoveUserOperation, input and output payloads, and other stuffs.
  *
  *
  */
object LinesRemoveUserExecutor {
  lazy val linesRemoveUserPullOperation: PullInternalOperationType[LinesRemoveUserInputPayload] =
    new PullInternalOperationType[LinesRemoveUserInputPayload](
      operationName = "lines-remove-user",
      serviceName = None,
      typeDirection = None,
      dataType = JSON,
      executor = LinesRemoveUserExecutor()
    )


  final case class LinesRemoveUserInputPayload(key: String,
                                               userChannel: String,
                                               UserID: String) extends InputPayload

  final case class LinesRemoveUserOutputPayload(key: String,
                                                userChannel: String,
                                                UserID: String,
                                                metadata: Array[String],
                                                timestamp: String) extends OutputPayload


  def apply(): LinesRemoveUserExecutor = new LinesRemoveUserExecutor()


  private implicit class ThrowableExtension(t: Throwable) {

    import java.io.{PrintWriter, StringWriter}

    def stackAsString: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }

}

/** LinesRemoveUserExecutor class.
  *
  * This class provide business logic for LinesRemoveUserOperation.
  *
  *
  */
class LinesRemoveUserExecutor extends Executor[LinesRemoveUserInputPayload] with LazyLogging {

  import LinesRemoveUserExecutor._

  val dao: LineDAO = LineDAO.getInstance()

  override def run(wire: Wire, payload: LinesRemoveUserInputPayload): Future[ExecutorResult] = {

    logger.info("Chave de entrada " + payload.key + " Canal de entrada " + payload.userChannel + " Usuário " + payload.UserID)


    //Simula usuários na fila
    //loadQueue()
    dao.listUsers( payload.key ).foreach(us => println("User",us.channel,us.id))

    val awaitTime = 60

    if (dao.removeUser(payload.key, payload.userChannel, payload.UserID)) {
      val timestamp = System.currentTimeMillis.toString
      val pushpayload = LinesRemoveUserOutputPayload(payload.key, payload.userChannel, payload.UserID, Array(""), timestamp)

      //Início - Consulta usuários após o remove
      dao.listUsers( payload.key ).foreach(us => println("User final",us.channel,us.id))
      //Fim - Consulta usuários após o remove

      SerializationUtil.toJSON[LinesRemoveUserOutputPayload](pushpayload) match {
        case Success(des) =>
          Try(Await.result(Producer.doPushInternalToInstance(wire, des.getBytes()), awaitTime.milliseconds)) match {
            case Success(pushed) => Future(ExecutorResult(success = pushed.ack, None))
            case Failure(error) => Future(ExecutorResult(success = false, Some(error.getMessage)))
          }
        case Failure(error) =>
          Future(ExecutorResult(success = false, Some(error.getMessage)))

      }
    } else {
      Producer.doFail(wire, FailPayload(400, "RemoveUser Failed"))
      Future(ExecutorResult(success = false, Some("RemoveUser Failed")))
    }
  }

}
