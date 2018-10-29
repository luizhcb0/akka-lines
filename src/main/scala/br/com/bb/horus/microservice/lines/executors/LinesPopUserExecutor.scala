package br.com.bb.horus.microservice.lines.executors

import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.{Executor, ExecutorResult, JSON}
import br.com.bb.horus.core.model.operation.Type.PullInternalOperationType
import br.com.bb.horus.core.model.payload.{InputPayload, OutputPayload}
import br.com.bb.horus.microservice.lines.executors.LinesPopUserExecutor.LinesPopUserInputPayload
import com.typesafe.scalalogging.LazyLogging


import scala.concurrent.Future

/** LinesPopUserExecutor companion object.
  *
  * This object provide access to the LinesPopUserOperation, input and output payloads, and other stuffs.
  *
  *
  */
object LinesPopUserExecutor {
  lazy val linesPopUserPullOperation: PullInternalOperationType[LinesPopUserInputPayload] =
    new PullInternalOperationType[LinesPopUserInputPayload](
      operationName = "pop-user",
      serviceName = None,
      typeDirection = None,
      dataType = JSON,
      executor = LinesPopUserExecutor()
    )


  final case class LinesPopUserInputPayload(key: String,
                                            fromEnd: String) extends InputPayload

  final case class LinesPopUserOutputPayload(key: String,
                                             userChannel: String,
                                             UserID: String,
                                             priority: String,
                                             metadata: Array[String],
                                             timestamp: String) extends OutputPayload


  def apply(): LinesPopUserExecutor = new LinesPopUserExecutor()


  private implicit class ThrowableExtension(t: Throwable) {

    import java.io.{PrintWriter, StringWriter}

    def stackAsString: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }

}

/** LinesPopUserExecutor class.
  *
  * This class provide business logic for LinesPopUserOperation.
  *
  *
  */
class LinesPopUserExecutor extends Executor[LinesPopUserInputPayload] with LazyLogging {

  import LinesPopUserExecutor._

  override def run(wire: Wire, payload: LinesPopUserInputPayload): Future[ExecutorResult] = ???

  /* TODO: Implementar LinesPopUser

  1 -
  2 -

   */

}
