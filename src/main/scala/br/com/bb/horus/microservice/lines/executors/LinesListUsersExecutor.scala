package br.com.bb.horus.microservice.lines.executors

import akka.actor.FSM.Failure
import akka.actor.Status
import akka.stream.FlowMonitorState.Failed
import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.{Executor, ExecutorResult, JSON}
import br.com.bb.horus.core.model.operation.Type.PullInternalOperationType
import br.com.bb.horus.core.model.payload.{FailPayload, InputPayload, OutputPayload}
import br.com.bb.horus.microservice.lines.dao.LineDAO
import br.com.bb.horus.microservice.lines.executors.LinesListUsersExecutor.LinesListUsersInputPayload
import br.com.bb.horus.microservice.lines.executors.LinesRetrieveExecutor.LinesRetrieveOutputPayload
import br.com.bb.horus.microservice.lines.models.User
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, _}
import scala.util.Success

/** LinesListUsersExecutor companion object.
  *
  * This object provide access to the LinesListUserOperation, input and output payloads, and other stuffs.
  *
  *
  */
object LinesListUsersExecutor {
  lazy val linesListUsersPullOperation: PullInternalOperationType[LinesListUsersInputPayload] =
    new PullInternalOperationType[LinesListUsersInputPayload](
      operationName = "list-users",
      serviceName = None,
      typeDirection = None,
      dataType = JSON,
      executor = LinesListUsersExecutor()
    )


  final case class LinesListUsersInputPayload(key: String,
                                              userChannel: String,
                                              UserID: String,
                                              metadata: String) extends InputPayload

//  final case class LinesListUsersOutputPayload(key: String,
//                                               userChannel: String,
//                                               UserID: String,
//                                               metadata: Array[String],
//                                               timestamp: String) extends OutputPayload
final case class LinesListUsersOutputPayload(  key: String,
                                               usersQueue: Array[User]) extends OutputPayload


  final case class LinesListUsersAnswer (Users:Array[String], key:String)

  def apply(): LinesListUsersExecutor = new LinesListUsersExecutor()


  private implicit class ThrowableExtension(t: Throwable) {

    import java.io.{PrintWriter, StringWriter}

    def stackAsString: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }

}

/** LinesListUsersExecutor class.
  *
  * This class provide business logic for LinesListUserOperation.
  *
  *
  */
class LinesListUsersExecutor extends Executor[LinesListUsersInputPayload] with LazyLogging {

  import LinesListUsersExecutor._

  override def run(wire: Wire, payload: LinesListUsersInputPayload): Future[ExecutorResult] = {

    //println("Key:" + payload.key)
    //payload.metadata.foreach(y => println("Metadata:", y))

    import br.com.bb.horus.core.util.SerializationUtil
    import scala.util.{Failure, Success, Try}
    import br.com.bb.horus.core.functions.Producer

    import java.util.SimpleTimeZone
    val timestamp: String = System.currentTimeMillis.formatted(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC").toString)

    Future {
      try {

        val redis = LineDAO.getInstance()
        loadQueue( redis )

        val usersQueue:Seq[User] = redis.listUsers(payload.key)

        usersQueue match {
          case retrieveLine =>

            if ( retrieveLine.length > 0  ) {

              val retPayload: LinesListUsersOutputPayload = LinesListUsersOutputPayload(key = payload.key,
                usersQueue = retrieveLine.toArray)

              SerializationUtil.toJSON[LinesListUsersOutputPayload](retPayload) match {
                case Success(des) => Producer.doPushInternalToService(wire, des.getBytes())
                  ExecutorResult(success = true, taskDescription = Some("Usuáros recebidos com sucesso"))

                case Failure(error) => Producer.doFail(wire, FailPayload(-302, error.getMessage))
                  ExecutorResult(success = false, taskDescription = Some(error.getMessage))
              }
            }
            else {
              Producer.doFail(wire, FailPayload(-302, "Não existem usuários"))
              ExecutorResult(success = true, taskDescription = Some("Não existem usuários"))
            }

          case _ => Producer.doFail(wire, FailPayload(-302, "Usuários não recuperados"))
            ExecutorResult(success = false, taskDescription = Some("cccc"))
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
