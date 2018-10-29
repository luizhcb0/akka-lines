package br.com.bb.horus.microservice.lines.executors

import br.com.bb.horus.core.functions.Producer
import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.{Executor, ExecutorResult, JSON}
import br.com.bb.horus.core.model.operation.Type.PullInternalOperationType
import br.com.bb.horus.core.model.payload.{FailPayload, InputPayload, OutputPayload}
import br.com.bb.horus.core.util.SerializationUtil
import br.com.bb.horus.microservice.lines.executors.LinesPurgeUserExecutor.LinesPurgeUserInputPayload
import com.typesafe.scalalogging.LazyLogging
import br.com.bb.horus.microservice.lines.dao.LineDAO

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

import scala.concurrent.ExecutionContext.Implicits.global

/** LinesPurgeUserExecutor companion object.
  *
  * This object provide access to the LinesPurgeUserOperation, input and output payloads, and other stuffs.
  *
  *
  */
object LinesPurgeUserExecutor {
  lazy val linesPurgeUserPullOperation: PullInternalOperationType[LinesPurgeUserInputPayload] =
    new PullInternalOperationType[LinesPurgeUserInputPayload](
      operationName = "purge-user",
      serviceName = None,
      typeDirection = None,
      dataType = JSON,
      executor = LinesPurgeUserExecutor()
    )


  final case class LinesPurgeUserInputPayload(userChannel: String,
                                              userID: String) extends InputPayload

  final case class LinesPurgeUserOutputPayload(userChannel: String,
                                               userID: String,
                                               metadata: Array[String],
                                               timestamp: Long) extends OutputPayload


  def apply(): LinesPurgeUserExecutor = new LinesPurgeUserExecutor()


  private implicit class ThrowableExtension(t: Throwable) {

    import java.io.{PrintWriter, StringWriter}

    def stackAsString: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }

}

/** LinesPurgeUserExecutor class.
  *
  * This class provide business logic for LinesPurgeUserOperation.
  *
  *
  */
class LinesPurgeUserExecutor extends Executor[LinesPurgeUserInputPayload] with LazyLogging {

  import LinesPurgeUserExecutor._

  override def run(wire: Wire, payload: LinesPurgeUserInputPayload): Future[ExecutorResult] = {

    val lineDAO: LineDAO = LineDAO.getInstance()

    //TODO: Metadata
    val metadata = Array("")
    val awaitTime = 60

    if (lineDAO.purgeUser(payload.userChannel, payload.userID)) {

      val timestamp = System.currentTimeMillis()
      val pushPayload = LinesPurgeUserOutputPayload(payload.userChannel, payload.userID, metadata, timestamp)

      SerializationUtil.toJSON[LinesPurgeUserOutputPayload](pushPayload) match {
        case Success(des) =>
          Try(Await.result(Producer.doPushInternalToInstance(wire, des.getBytes()), awaitTime.milliseconds)) match {
            case Success(pushed) => Future(ExecutorResult(success = pushed.ack, None))
            case Failure(error) => Future(ExecutorResult(success = false, Some(error.getMessage)))
          }
        case Failure(error) =>
          Future(ExecutorResult(success = false, Some(error.getMessage)))
      }

    } else {
      Producer.doFail(wire, FailPayload(400, "PurgeUserOperation Failed"))
      Future(ExecutorResult(success = false, Some("PurgeUserOperation Failed")))
    }
  }

}
