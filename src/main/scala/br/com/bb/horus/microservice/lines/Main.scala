package br.com.bb.horus.microservice.lines

import br.com.bb.horus.core.HorusCore
import br.com.bb.horus.core.functions.Producer
import br.com.bb.horus.core.model.Wire
import br.com.bb.horus.core.model.operation.ExecutorResult
import br.com.bb.horus.core.util.SerializationUtil
import br.com.bb.horus.microservice.lines.dao.LineDAO
import br.com.bb.horus.microservice.lines.executors.LinesCreateExecutor.LinesCreateOutputPayload
import br.com.bb.horus.microservice.lines.executors._
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success}


object Main extends App with LazyLogging {

  HorusCore() match {
    case Success(core) =>

    //Create Operations

    //Add Operations
      core.addOperation(LinesCreateExecutor.linesCreatePullOperation)
      core.addOperation(LinesDeleteExecutor.linesDeletePullOperation)
      core.addOperation(LinesEnrichUserExecutor.linesEnrichUserPullOperation)
      core.addOperation(LinesListExecutor.linesListPullOperation)
      core.addOperation(LinesListUsersExecutor.linesListUsersPullOperation)
      core.addOperation(LinesPopUserExecutor.linesPopUserPullOperation)
      core.addOperation(LinesPriorizeUserExecutor.linesPriorizeUserPullOperation)
      core.addOperation(LinesPurgeUserExecutor.linesPurgeUserPullOperation)
      core.addOperation(LinesPushUserExecutor.linesPushUserPullOperation)
      core.addOperation(LinesRemoveUserExecutor.linesRemoveUserPullOperation)
      core.addOperation(LinesRetrieveExecutor.linesRetrievePullOperation)
      core.addOperation(LinesUniformizeExecutor.linesUniformizePullOperation)
      core.addOperation(LinesUpdateExecutor.linesUpdatePullOperation)
      core.addOperation(LinesWipeExecutor.linesWipePullOperation)

      val dao = LineDAO.getInstance()

      //dao.clearAll()

      /*dao.insert("3", "emprestimo", "teste")
      dao.insert("4", "credito", "teste")

      dao.pushUser("3", "APF", "000000199", "", 4, true)
      Thread.sleep(100)
      dao.pushUser("3", "APF", "000000099", "", 5, true)
      Thread.sleep(100)
      dao.pushUser("3", "APF", "000000106", "", 5, false)
      Thread.sleep(100)
      dao.pushUser("3", "APF", "000000080", "", 5, false)
      Thread.sleep(100)
      dao.pushUser("3", "APF", "000000083", "", 3, true)
      Thread.sleep(100)
      dao.pushUser("3", "APF", "000000087", "", 4, true)
      Thread.sleep(100)
      dao.pushUser("3", "APF", "000000082", "", 10,true)

      dao.pushUser("4", "APF", "000000199", "", 4, true)
      Thread.sleep(100)

      dao.getAll.foreach(l => println(l.key + "-" + l.name + "-" + l.metadata))

      dao.purgeUser("APF","000000199")
      dao.listUsers("3").foreach(user => println(user.id + "-" + user.channel))
      dao.popUser("3", false) match {
        case Some(user) => println(user.id + "-" + user.channel)
        case None => println("Não há usuários na fila!")
      }*/
    case Failure(e) => logger.error("ERROR: " + e.getMessage)
  }
}
