package br.com.bb.horus.microservice.lines.executors

import br.com.bb.horus.core.functions.Producer
import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.{Executor, ExecutorResult, JSON}
import br.com.bb.horus.core.model.operation.Type.PullInternalOperationType
import br.com.bb.horus.core.model.payload.{FailPayload, InputPayload, OutputPayload}
import br.com.bb.horus.core.util.SerializationUtil
import br.com.bb.horus.microservice.lines.dao.LineDAO
import br.com.bb.horus.microservice.lines.executors.LinesEnrichUserExecutor.LinesEnrichUserInputPayload
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}


import scala.concurrent.ExecutionContext.Implicits.global

/** LinesEnrichUserExecutor companion object.
  *
  * This object provide access to the LinesEnrichUserOperation, input and output payloads, and other stuffs.
  *
  *
  */
object LinesEnrichUserExecutor {
  lazy val linesEnrichUserPullOperation: PullInternalOperationType[LinesEnrichUserInputPayload] =
    new PullInternalOperationType[LinesEnrichUserInputPayload](
      operationName = "enrich-user",
      serviceName = None,
      typeDirection = None,
      dataType = JSON,
      executor = LinesEnrichUserExecutor()
    )


  final case class LinesEnrichUserInputPayload(key: String,
                                               userChannel: String,
                                               userID: String,
                                               metadata: String) extends InputPayload

  final case class LinesEnrichUserOutputPayload(key: String,
                                                userChannel: String,
                                                userID: String,
                                                metadata: String,
                                                timestamp: Long) extends OutputPayload


  def apply(): LinesEnrichUserExecutor = new LinesEnrichUserExecutor()


  private implicit class ThrowableExtension(t: Throwable) {

    import java.io.{PrintWriter, StringWriter}

    def stackAsString: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }

}

/** LinesEnrichUserExecutor class.
  *
  * This class provide business logic for LinesEnrichUserOperation.
  *
  *
  */
class LinesEnrichUserExecutor extends Executor[LinesEnrichUserInputPayload] with LazyLogging {

  import LinesEnrichUserExecutor._

  override def run(wire: Wire, payload: LinesEnrichUserInputPayload): Future[ExecutorResult] = {

    val lineDAO: LineDAO = LineDAO.getInstance()

    val awaitTime = 60

    if (lineDAO.enrichUser(payload.key,payload.userChannel,payload.userID, payload.metadata)) {

      val timestamp = System.currentTimeMillis()
      val pushPayload = LinesEnrichUserOutputPayload(payload.key, payload.userChannel, payload.userID, payload.metadata, timestamp)

      SerializationUtil.toJSON[LinesEnrichUserOutputPayload](pushPayload) match {
        case Success(des) =>
          Try(Await.result(Producer.doPushInternalToInstance(wire, des.getBytes()), awaitTime.milliseconds)) match {
            case Success(pushed) => Future(ExecutorResult(success = pushed.ack, None))
            case Failure(error) => Future(ExecutorResult(success = false, Some(error.getMessage)))
          }
        case Failure(error) =>
          Future(ExecutorResult(success = false, Some(error.getMessage)))
      }

    } else {
      Producer.doFail(wire, FailPayload(400, "EnrichUserOperation Failed"))
      Future(ExecutorResult(success = false, Some("EnrichUserOperation Failed")))
    }

  }
}
