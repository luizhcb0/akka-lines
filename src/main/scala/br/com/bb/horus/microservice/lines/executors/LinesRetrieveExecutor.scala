package br.com.bb.horus.microservice.lines.executors

import akka.japi.Option
import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.{Executor, ExecutorResult, JSON}
import br.com.bb.horus.core.model.operation.Type.PullInternalOperationType
import br.com.bb.horus.core.model.payload.{FailPayload, InputPayload, OutputPayload}
import br.com.bb.horus.microservice.lines.dao.LineDAO
import br.com.bb.horus.microservice.lines.executors.LinesRetrieveExecutor.LinesRetrieveInputPayload
import br.com.bb.horus.microservice.lines.executors.LinesUpdateExecutor.LinesUpdateOutputPayload
import br.com.bb.horus.microservice.lines.models.Line
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global

/** LinesRetrieveExecutor companion object.
  *
  * This object provide access to the LinesRetrieveOperation, input and output payloads, and other stuffs.
  *
  *
  */
object LinesRetrieveExecutor {
  lazy val linesRetrievePullOperation: PullInternalOperationType[LinesRetrieveInputPayload] =
    new PullInternalOperationType[LinesRetrieveInputPayload](
      operationName = "retrieve",
      serviceName = None,
      typeDirection = None,
      dataType = JSON,
      executor = LinesRetrieveExecutor()
    )


  final case class LinesRetrieveInputPayload(key: String) extends InputPayload

  final case class LinesRetrieveOutputPayload(key: String,
                                              name: String,
                                              timestampCreation: String,
                                              timestampLastUpdate: String,
                                              metadata: String) extends OutputPayload


  def apply(): LinesRetrieveExecutor = new LinesRetrieveExecutor()


  private implicit class ThrowableExtension(t: Throwable) {

    import java.io.{PrintWriter, StringWriter}

    def stackAsString: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }

}

/** LinesRetrieveExecutor class.
  *
  * This class provide business logic for LinesRetrieveOperation.
  *
  *
  */
class LinesRetrieveExecutor extends Executor[LinesRetrieveInputPayload] with LazyLogging {

  import LinesRetrieveExecutor._

  override def run(wire: Wire, payload: LinesRetrieveInputPayload): Future[ExecutorResult] = {

    import br.com.bb.horus.core.util.SerializationUtil
    import scala.util.{Failure, Success, Try}
    import br.com.bb.horus.core.functions.Producer

    import java.util.SimpleTimeZone
    //val timestamp: String = System.currentTimeMillis.formatted(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC").toString)
    val timestamp: String = System.currentTimeMillis.toString


    Future {
      try {

        val redis = LineDAO.getInstance()
        //loadQueue( redis )

        val retrieve= redis.retrieve(payload.key)
        //ExecutorResult(success = false, taskDescription = Some("cccc"))


        retrieve match {
          case Some(line)  =>  println("Line",line.name)
                        //ExecutorResult(success = true, taskDescription = Some("Line recuperada com sucesso"))


                        val retPayload: LinesRetrieveOutputPayload = LinesRetrieveOutputPayload(key = payload.key, name = line.name,
                        timestampCreation = line.createdAt.toString, timestampLastUpdate = line.createdAt.toString,
                        metadata = line.metadata  )

                        SerializationUtil.toJSON[LinesRetrieveOutputPayload](retPayload) match {
                          case Success(des) => Producer.doPushInternalToService(wire, des.getBytes())
                            ExecutorResult(success = true, taskDescription = Some("Line recuperada com sucesso"))

                          case Failure(error) => Producer.doFail(wire, FailPayload(-302, error.getMessage))
                            ExecutorResult(success = false, taskDescription = Some(error.getMessage))
                        }

          case _ => Producer.doFail(wire, FailPayload(-302, "Line não recuperada"))
                    ExecutorResult(success = false, taskDescription = Some("Line não recuperada"))
        }

      } catch {
        case e: Exception => Producer.doFail(wire, FailPayload(-302, e.getMessage))
          ExecutorResult(success = false, taskDescription = Some(e.getMessage))
      }
    }


  }

  def loadQueue(redis:LineDAO): Unit = {

    //redis.clearAll()
    redis.insert("1", "EMPRESTIMO 3", "PROV") match {

      case lineToAdd =>

        logger.debug("Name:" + lineToAdd.get.name)
        //val all = redis.getAll

        redis.pushUser(key = "1", userChannel = "CARTAO", userId = "000000089", userMetadata = "", priority = 10, atEnd = false) match {

          case (Some(value)) => logger.debug("Id usuário" + value.id)
          case None => logger.debug("Não conseguiu incluir i usuário")
        }
        redis.pushUser(key = "1", userChannel = "CARTAO", userId = "000000090", userMetadata = "", priority = 100, atEnd = false) match {

          case (Some(value)) => logger.debug("Id usuário" + value.id)
          case None => logger.debug("Não conseguiu incluir i usuário")
        }
        redis.pushUser(key = "1", userChannel = "CARTAO", userId = "000000091", userMetadata = "", priority = 1, atEnd = false) match {

          case (Some(value)) => logger.debug("Id usuário" + value.id)
          case None => logger.debug("Não conseguiu incluir i usuário")
        }

      case _ => logger.debug(" Não inclui a linha.")
    }
  }

}
