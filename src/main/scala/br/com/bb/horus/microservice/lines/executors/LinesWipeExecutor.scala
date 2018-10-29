package br.com.bb.horus.microservice.lines.executors

import br.com.bb.horus.core.functions.Producer
import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.{Executor, ExecutorResult, JSON}
import br.com.bb.horus.core.model.operation.Type.PullInternalOperationType
import br.com.bb.horus.core.model.payload.{FailPayload, InputPayload, OutputPayload}
import br.com.bb.horus.core.util.SerializationUtil
import br.com.bb.horus.microservice.lines.dao.LineDAO
import br.com.bb.horus.microservice.lines.executors.LinesWipeExecutor.LinesWipeInputPayload
import br.com.bb.horus.microservice.lines.dao.LineDAO._
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

import scala.concurrent.ExecutionContext.Implicits.global

/** LinesWipeExecutor companion object.
  *
  * This object provide access to the LinesWipeOperation, input and output payloads, and other stuffs.
  *
  *
  */
object LinesWipeExecutor {
  lazy val linesWipePullOperation: PullInternalOperationType[LinesWipeInputPayload] =
    new PullInternalOperationType[LinesWipeInputPayload](
      operationName = "wipe",
      serviceName = None,
      typeDirection = None,
      dataType = JSON,
      executor = LinesWipeExecutor()
    )


  final case class LinesWipeInputPayload(key: String) extends InputPayload

  final case class LinesWipeOutputPayload(key: String) extends OutputPayload


  def apply(): LinesWipeExecutor = new LinesWipeExecutor()


  private implicit class ThrowableExtension(t: Throwable) {

    import java.io.{PrintWriter, StringWriter}

    def stackAsString: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }

}

/** LinesWipeExecutor class.
  *
  * This class provide business logic for LinesWipeOperation.
  *
  *
  */
class LinesWipeExecutor extends Executor[LinesWipeInputPayload] with LazyLogging {

  import LinesWipeExecutor._

  override def run(wire: Wire, payload: LinesWipeInputPayload): Future[ExecutorResult] = {

    val lineDAO: LineDAO = LineDAO.getInstance()

    val pushPayload = LinesWipeOutputPayload(payload.key)
    val awaitTime = 60

    if (lineDAO.wipe(payload.key)) {
      SerializationUtil.toJSON[LinesWipeOutputPayload](pushPayload) match {
        case Success(des) =>
          Try(Await.result(Producer.doPushInternalToInstance(wire, des.getBytes()), awaitTime.milliseconds)) match {
            case Success(pushed) => Future(ExecutorResult(success = pushed.ack, None))
            case Failure(error) => Future(ExecutorResult(success = false, Some(error.getMessage)))
          }
        case Failure(error) =>
          Future(ExecutorResult(success = false, Some(error.getMessage)))
      }
    } else {
      Producer.doFail(wire, FailPayload(400, "WipeOperation Failed"))
      Future(ExecutorResult(success = false, Some("WipeOperation Failed")))
    }


  }
}
