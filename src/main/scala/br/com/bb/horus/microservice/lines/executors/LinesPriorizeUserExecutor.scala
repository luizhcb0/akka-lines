package br.com.bb.horus.microservice.lines.executors

import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.{Executor, ExecutorResult, JSON}
import br.com.bb.horus.core.model.operation.Type.PullInternalOperationType
import br.com.bb.horus.core.model.payload.{FailPayload, InputPayload, OutputPayload}
import br.com.bb.horus.microservice.lines.dao.LineDAO
import br.com.bb.horus.microservice.lines.executors.LinesListUsersExecutor.LinesListUsersOutputPayload
import br.com.bb.horus.microservice.lines.executors.LinesPriorizeUserExecutor.LinesPriorizeUserInputPayload
import br.com.bb.horus.microservice.lines.executors.LinesRetrieveExecutor.LinesRetrieveOutputPayload
import br.com.bb.horus.microservice.lines.models.User
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

/** LinesPriorizeUserExecutor companion object.
  *
  * This object provide access to the LinesPriorizeUserOperation, input and output payloads, and other stuffs.
  *
  *
  */
object LinesPriorizeUserExecutor {
  lazy val linesPriorizeUserPullOperation: PullInternalOperationType[LinesPriorizeUserInputPayload] =
    new PullInternalOperationType[LinesPriorizeUserInputPayload](
      operationName = "priorize-user",
      serviceName = None,
      typeDirection = None,
      dataType = JSON,
      executor = LinesPriorizeUserExecutor()
    )


  final case class LinesPriorizeUserInputPayload(key: String,
                                                 userChannel: String,
                                                 UserID: String,
                                                 priority: String,
                                                 atEnd: String) extends InputPayload

  final case class LinesPriorizeUserOutputPayload(key: String,
                                                  userChannel: String,
                                                  UserID: String,
                                                  priority: String,
                                                  timestamp:String) extends OutputPayload


  def apply(): LinesPriorizeUserExecutor = new LinesPriorizeUserExecutor()


  private implicit class ThrowableExtension(t: Throwable) {

    import java.io.{PrintWriter, StringWriter}

    def stackAsString: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }

}

/** LinesPriorizeUserExecutor class.
  *
  * This class provide business logic for LinesPriorizeUserOperation.
  *
  *
  */
class LinesPriorizeUserExecutor extends Executor[LinesPriorizeUserInputPayload] with LazyLogging {

  case  class respUpdatePriority ( status:Boolean, resp:String )

  import LinesPriorizeUserExecutor._

  override def run(wire: Wire, payload: LinesPriorizeUserInputPayload): Future[ExecutorResult]= {

    import br.com.bb.horus.core.util.SerializationUtil
    import scala.util.{Failure, Success, Try}
    import br.com.bb.horus.core.functions.Producer

    import java.util.SimpleTimeZone

    import java.util.SimpleTimeZone
    //val timest: String = System.currentTimeMillis.formatted(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC").toString)
    val timest: String = System.currentTimeMillis.toString

    // Simula usuarios na file.
    //loadQueue( redis )

    Future {

      try {

        val redis = LineDAO.getInstance()

        val respUpdt: respUpdatePriority = updatePriority(payload.priority.toInt, redis, payload.key, payload.userChannel, payload.UserID, payload.atEnd.toBoolean)
        if (respUpdt.status) {

          val retPayload: LinesPriorizeUserOutputPayload = LinesPriorizeUserOutputPayload(key = payload.key,
            userChannel = payload.userChannel, UserID = payload.UserID, priority = payload.priority, timestamp=timest)

          SerializationUtil.toJSON[LinesPriorizeUserOutputPayload](retPayload) match {
            case Success(des) => Producer.doPushInternalToService(wire, des.getBytes())
              ExecutorResult(success = true, taskDescription = Some(respUpdt.resp))

            case Failure(error) => Producer.doFail(wire, FailPayload(-302, error.getMessage))
              ExecutorResult(success = false, taskDescription = Some(error.getMessage))
          }
        }
        else {
          Producer.doFail(wire, FailPayload(-302, respUpdt.resp))
          ExecutorResult(success = false, taskDescription = Some(respUpdt.resp))
        }
      }
      catch {
        case e: Exception =>  Producer.doFail(wire, FailPayload(-302, e.getMessage))
          ExecutorResult(success = false, taskDescription = Some(e.getMessage))
      }
    }

  }

  def updatePriority ( priority:Int, redis:LineDAO, key:String, ch:String, userId:String, atEnd:Boolean ):respUpdatePriority = {

    try {
      val usrs:Option[User] = redis.getUser(key, ch, userId)

      usrs match {

        case  Some(l) =>
          logger.debug("Id :"+l.id +",Channel : "+l.channel+",Meta : "+l.metadata)

          val stDelete = redis.removeUser(key= key, userChannel=ch, userId=userId)
          if ( stDelete ) {
            logger.debug("Removeu o usuário")

            redis.pushUser( key, ch, userId, l.metadata, priority, atEnd) match {

              case  ( Some(value) ) =>  logger.debug("Inclui o novo usuário com nova prioridade: "+value.id)
                respUpdatePriority(status = true,resp = "Inclui o novo usuário "+userId+" com nova prioridade "+priority)
              case  None => logger.debug("Não conseguiu incluir o usuário")
                respUpdatePriority(status = false,resp = "Não conseguiu incluir o usuário "+userId)
            }
          }
          else  respUpdatePriority(status = false,resp = "Não existe o usuário "+userId)
        case None =>  logger.debug("Não achou")
          respUpdatePriority(status = false,resp = "Não existe o usuário "+userId)
      }
    }catch {
      case e: Exception => logger.debug( "Não conseguiu ler:"+e.getMessage)
        respUpdatePriority(status = false,resp = e.getMessage)
    }
  }



  def loadQueue(redis:LineDAO): Unit = {

    //redis.clearAll()
    redis.insert("1", "EMPRESTIMO 3","PROV") match  {

      case lineToAdd =>

        logger.debug("Name:"+lineToAdd.get.name)
        //val all = redis.getAll

        redis.pushUser( key="1", userChannel = "CARTAO", userId = "000000089", userMetadata = "", priority= 10, atEnd = false) match {

          case  ( Some(value) ) =>  logger.debug("Id usuário"+value.id)
          case  None => logger.debug("Não conseguiu incluir i usuário")
        }
        redis.pushUser( key="1", userChannel = "CARTAO", userId = "000000090", userMetadata = "", priority= 100, atEnd = false) match {

          case  ( Some(value) ) =>  logger.debug("Id usuário"+value.id)
          case  None => logger.debug("Não conseguiu incluir i usuário")
        }
        redis.pushUser( key="1", userChannel = "CARTAO", userId = "000000091", userMetadata = "", priority= 1, atEnd = false) match {

          case  ( Some(value) ) =>  logger.debug("Id usuário"+value.id)
          case  None => logger.debug("Não conseguiu incluir i usuário")
        }

      case _ => logger.debug( " Não inclui a linha.")
    }

    //LineDAO.listUsers("1")
  }


}
