package br.com.bb.horus.microservice.lines.executors

import java.util.SimpleTimeZone

import br.com.bb.horus.core.functions.Producer
import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.{Executor, ExecutorResult, JSON}
import br.com.bb.horus.core.model.operation.Type.PullInternalOperationType
import br.com.bb.horus.core.model.payload.{FailPayload, InputPayload, OutputPayload}
import br.com.bb.horus.core.util.SerializationUtil
import br.com.bb.horus.microservice.lines.dao.LineDAO
import br.com.bb.horus.microservice.lines.executors.LinesUniformizeExecutor.LinesUniformizeInputPayload
import br.com.bb.horus.microservice.lines.models.{Line, User}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

/** LinesUniformizeExecutor companion object.
  *
  * This object provide access to the LinesUniformizeOperation, input and output payloads, and other stuffs.
  *
  *
  */
object LinesUniformizeExecutor {
  lazy val linesUniformizePullOperation: PullInternalOperationType[LinesUniformizeInputPayload] =
    new PullInternalOperationType[LinesUniformizeInputPayload](
      operationName = "uniformize",
      serviceName = None,
      typeDirection = None,
      dataType = JSON,
      executor = LinesUniformizeExecutor()
    )


  final case class LinesUniformizeInputPayload(key: String,
                                               priority: String) extends InputPayload

  final case class LinesUniformizeOutputPayload(key: String,
                                                priority: String) extends OutputPayload


  def apply(): LinesUniformizeExecutor = new LinesUniformizeExecutor()


  private implicit class ThrowableExtension(t: Throwable) {

    import java.io.{PrintWriter, StringWriter}

    def stackAsString: String = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }

}

/** LinesUniformizeExecutor class.
  *
  * This class provide business logic for LinesUniformizeOperation.
  *
  *
  */
class LinesUniformizeExecutor extends Executor[LinesUniformizeInputPayload] with LazyLogging {

  import LinesUniformizeExecutor._

  override def run(wire: Wire, payload: LinesUniformizeInputPayload): Future[ExecutorResult] = {

    import br.com.bb.horus.core.functions.Producer
    val timestamp: String = System.currentTimeMillis.formatted(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC").toString)

    Future {
      try {

        val redis = LineDAO.getInstance()

        val debugUnif = false
        if ( debugUnif ) loadQueue( redis )

        val usersQueue: Seq[User] = redis.listUsers(payload.key)

        usersQueue match {
          case retrieveLine: Seq[User] =>

            if (retrieveLine.length > 0) {

              val actualLine = redis.retrieve(payload.key)

              actualLine match {

                case Some(line) =>


                  if  ( !debugUnif && uniformizeLine(redis, line, retrieveLine.toArray, payload.priority.toInt) ) {

                    val retPayload: LinesUniformizeOutputPayload = LinesUniformizeOutputPayload(key = payload.key,
                      priority = payload.priority)

                    SerializationUtil.toJSON[LinesUniformizeOutputPayload](retPayload) match {
                      case Success(des) => Producer.doPushInternalToInstance(wire, des.getBytes())
                        ExecutorResult(success = true, taskDescription = Some("Uniformize: Prioridade dos usuáros atualizada com sucesso"))

                      case Failure(error) => Producer.doFail(wire, FailPayload(-302, error.getMessage))
                        ExecutorResult(success = false, taskDescription = Some(error.getMessage))
                    }
                  }
                  else {
                    Producer.doFail(wire, FailPayload(-302, "Uniformize: Não foi possivel atualizar as prioridades dos usuários na fila."))
                    ExecutorResult(success = false, taskDescription = Some("Uniformize: Não foi possivel atualizar as prioridades dos usuários na fila."))
                  }
                case _ => Producer.doFail(wire, FailPayload(-302, "Uniformize: Não foi possivel encontrar a fila."))
                          ExecutorResult(success = false, taskDescription = Some("Uniformize: Não foi possivel encontrar a fila."))
              }
            }
            else {
              Producer.doFail(wire, FailPayload(-302, "Uniformize: Não existem usuários na fila."))
              ExecutorResult(success = false, taskDescription = Some("Uniformize: Não existem usuários na fila."))
            }

          case _ => Producer.doFail(wire, FailPayload(-302, "Uniformize: Não existem usuários na fila"))
            ExecutorResult(success = false, taskDescription = Some("Uniformize: Não existem usuários na fila"))
        }
      } catch {
        case e: Exception => Producer.doFail(wire, FailPayload(-302, e.getMessage))
          ExecutorResult(success = false, taskDescription = Some(e.getMessage))
      }
    }
  }

  def uniformizeLine(redis:LineDAO, line:Line, users:Array[User], priority:Int ): Boolean = {

      if ( redis.remove(line.key) ) {
        redis.insert(line.key, line.name, line.metadata) match {

          case Some(lineAdd) =>   var resultInsert = true
                                  users.foreach( usr =>
                                    redis.pushUser(key = line.key, userChannel = usr.channel, userId = usr.id, userMetadata = usr.metadata, priority, atEnd = false)
                                    match {
                                      case _ =>
                                      case None =>  logger.debug("Não conseguiu incluir o usuário na fila.")
                                                    resultInsert = false
                                    })
                                    resultInsert
          case  None => false
        }
      }
      else  false
    }

  def loadQueue(redis:LineDAO): Unit = {

    redis.clearAll()
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

