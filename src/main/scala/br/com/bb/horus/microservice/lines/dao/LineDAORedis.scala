package br.com.bb.horus.microservice.lines.dao


import br.com.bb.horus.core.util.ConfigUtil
import br.com.bb.horus.microservice.lines.models.{Line, User}
import com.redis.RedisClient

import scala.collection.mutable.ListBuffer
import scala.util.Success

object LineDAORedis{
  val LINES_INFO_STRUCT = "lines"

  val redisHost = ConfigUtil.getFromDB[String]("redis.host").get
  val redisPort = ConfigUtil.getFromDB[Int]("redis.port").get
  val redisPasswd = ConfigUtil.getFromDB[String]("redis.password").get

  val redis = new RedisClient(redisHost, redisPort, secret = Some(redisPasswd))

  //val redis = new RedisClient("172.18.15.154", 6379)

  val orderHelper = 86400000



  def resultToUser(entry: String): Option[User] ={
    val parts = entry.split(":")

    if(parts.length != 5)
      None
    else {
      // Pega somentar o ID, Channel e Metadata
      Some(User(parts(1), parts(2), parts(3)))
    }
  }

  def resultToLine(entry: String): Option[Line] ={
    val parts = entry.split(":")

    if(parts.length != 5)
      None
    else
      Some(Line(parts(0), parts(1), parts(2), parts(3).toLong, parts(4).toLong))
  }

  def queryAllLines(): String = "*:*:*:*:*"
  def queryLinesByKey(term: String): String = s"$term:*:*:*:*"
  def queryLinesByMetadata(term: String): String = s"*:*:$term:*:*"
  def queryLinesByName(term: String): String = s"*:$term:*:*:*"

  def queryUsers(term: String): String = "*:*:*:*:*"
  def queryUsersByID(id: String): String = s"*:$id:*:*:*"
  def queryUsersByIDAndChannel(id: String, ch: String): String = s"*:$id:$ch:*:*"

  def lineObjectToEntry(line: Line): String ={
    s"${line.key}:${line.name}:${line.metadata}:${line.createdAt}:${line.updatedAt}"
  }
}

class LineDAORedis extends LineDAO {
  import LineDAORedis._

  override def insert(key: String, name: String, metadata: String): Option[Line] = {
    println("INSERT!!!!")
    //TODO: fazer em transaction
    val ts = System.currentTimeMillis()
    val lineToAdd = Line(key, name, metadata, ts, ts)

    redis.sadd(LINES_INFO_STRUCT, lineObjectToEntry(lineToAdd)) match {
      case Some(ret) if ret > 0 => Some(lineToAdd)
      case None => None
    }
  }

  override def update(key: String, line: Line): Boolean = {
    redis.sscan(LINES_INFO_STRUCT, 0, queryLinesByKey(key)) match {
      case Some(t) =>
        t._2 match {
          case Some(listRet) if listRet.nonEmpty =>
            listRet.head match {
              case Some(entry) =>
                //val line = resultToLine(entry).get

                val transactionRemAndAdd = redis.pipeline(pipelineRedis => {
                  //pipelineRedis.srem(key, entry)
                  //pipelineRedis.sadd(key, line.copy(updatedAt = System.currentTimeMillis()))
                  pipelineRedis.srem(LINES_INFO_STRUCT, entry)
                  pipelineRedis.sadd(LINES_INFO_STRUCT, lineObjectToEntry ( line.copy(updatedAt = System.currentTimeMillis())) )
                })

                transactionRemAndAdd match {
                  case Some(listTransactionRet) if listTransactionRet.size == 2 =>
                    val flatRet = listTransactionRet.map(s => s.asInstanceOf[Option[Long]].get)
                    flatRet.head == 1 && flatRet.last == 1
                  case _ =>
                    false
                }
            }
          case None => false
        }
      case None => false
    }
  }

  override def get(key: String): Option[Line] = {
    redis.sscan(LINES_INFO_STRUCT,0, queryLinesByKey(key)) match {
      case Some(t) =>
        t._2 match {
          case Some(listRet) if listRet.nonEmpty =>
            listRet.head match {
              case Some(entry) => resultToLine(entry)
              case None => None
            }

          case _ => None
        }
      case None => None
    }
  }

  override def getAll: Array[Line] = {
    redis.sscan(LINES_INFO_STRUCT, 0, queryAllLines()) match {
      case Some(t) =>
        t._2 match {
          case Some(listRet) => listRet.map {
            case Some(line) =>
              resultToLine(line) match {
                case Some(l) => l
                case None => null
              }
            case None => null
          }.filter(lOpt => lOpt != null).toArray
        }
    }
  }

  override def remove(key: String): Boolean = {
    val remLineInfo = redis.sscan(LINES_INFO_STRUCT, 0, queryLinesByKey(key)) match {
      case Some(t) =>
        t._2 match {
          case Some(listRet) if listRet.nonEmpty =>
            listRet.head match {
              case Some(entry) =>
                 redis.srem(LINES_INFO_STRUCT, entry) match {
                  case Some(ret) if ret > 0 => true
                  case _ => false
                }
              case None => false
            }

          case _ => false
        }
      case _ => false
    }

    //A remoção de uma fila implica na remoção de suas informações e de todos os usuário que ela tinha
    //Por isso é preciso também fazer um wipe
    remLineInfo && wipe(key)
  }

  override def wipe(key: String): Boolean = {
    redis.del(key) match {
      case Some(ret) if ret > 0 => true
      case _ => false
    }
  }
/*
  override def retrieve(key: String): Option[String] = {
    redis.sscan(LINES_INFO_STRUCT, 0, queryLinesByKey(key)) match {
      case Some(t) =>
        t._2 match {
          case Some(listRet) if listRet.nonEmpty =>
           listRet.head match {
             case Some(entry) => Some(resultToLine(entry).get.metadata)
             case None => None
           }
          case _ => None
        }
      case _ => None
    }
  }
*/
  override def retrieve(key: String): Option[Line] = {
    redis.sscan(LINES_INFO_STRUCT, 0, queryLinesByKey(key)) match {
      case Some(t) =>
        t._2 match {
          case Some(listRet) if listRet.nonEmpty =>
            listRet.head match {
              case Some(entry) => resultToLine(entry)
              case None => None
            }
          case _ => None
        }
      case _ => None
    }
  }

  override def list(metadata: String): Array[Line] = {
    redis.sscan(LINES_INFO_STRUCT, 0, queryAllLines()) match {
      case Some(t) =>
        t._2 match {
          case Some(listRet) if listRet.nonEmpty =>
            listRet.flatten.flatMap(entry => resultToLine(entry)).toArray

          case _ => Array()
        }
      case _ => Array()
    }
  }

  override def pushUser(key: String, userChannel: String, userId: String, userMetadata: String, priority: Int, atEnd: Boolean): Option[User] = {
      val ts = {
        if (atEnd)
          "%020d".format(System.currentTimeMillis() - Long.MaxValue)
        else
          "%020d".format(System.currentTimeMillis() + orderHelper)
      }

      if (lineContainsUser(key, userChannel, userId).isEmpty) {
        redis.zadd(key, priority, ts + ":" + userId + ":" + userChannel + ":" + userMetadata + ":" + key) match {
          case Some(ret) if ret > 0 =>
            Some(User(userId, userChannel, userMetadata))
          case Some(ret) if ret == 0 =>
            None
          case None =>
            None
        }
      } else {
        None
      }
  }

  /*
   * fromEnd == true -> lowest priority
   * fromEnd == false -> highest priority
   */
  override def popUser(key: String, fromEnd: Boolean): Option[User] = {
      val ret = redis.pipeline(pipelineCli => {
        val sortAs = {if (fromEnd) RedisClient.ASC else RedisClient.DESC}

        pipelineCli.zrange(key, 0, 0, sortAs)

        if (fromEnd)
          pipelineCli.zremrangebyrank(key, 0, 0)
        else
          pipelineCli.zremrangebyrank(key, -1, -1)
      })

      ret match {
        case Some(listRet) =>
          listRet.head match {
            case Some(listUser) =>
              resultToUser(listUser.asInstanceOf[List[Any]].head.asInstanceOf[String])
            case None =>
              None
          }
        case None => None
      }
  }

  override def removeUser(key: String, userChannel: String, userId: String): Boolean = {
    val userEntry = lineContainsUser(key, userChannel, userId)

    redis.zrem(key, userEntry) match{
      case Some(ret) if ret == 1 => true
      case _ => false
    }
  }

  override def purgeUser(userChannel: String, userId: String): Boolean = {
    val allLines = getAll
    val toRemList: ListBuffer[String] = new ListBuffer[String]

    val pipeRet = redis.pipeline(pipelineRedis => {
      allLines.foreach(line => pipelineRedis.zscan(line.key,0, queryUsersByIDAndChannel(userId, userChannel),1))
    })

    pipeRet match {
      case Some(listRet) =>
        val realListRet = listRet.asInstanceOf[List[Option[(Option[Int], Option[List[Option[String]]])]]]

        realListRet.foreach {
          case Some(t2) =>
            t2._2 match {
              case Some(scanRet) if scanRet.nonEmpty =>
                scanRet.head match {
                  case Some(userEntry) => toRemList.append(userEntry.asInstanceOf[String])
                }

              case _ =>
            }
        }
    }

    if(toRemList.nonEmpty) {
      redis.pipeline(pipelineRedis => {
        toRemList.foreach(rem => {
          val line = rem.split(":")(3)
          pipelineRedis.zrem(line, rem)
        })
      })

      true
    }else{
      false
    }
  }

  override def listUsers(key: String): Seq[User] = {
    redis.zrangebyscore(key, Double.MinValue, minInclusive = true, Double.MaxValue, maxInclusive = true, None) match {
      case Some(listRet) =>
        listRet.flatMap(entry => resultToUser(entry))
      case None => Nil
    }
  }

  override def enrichUser(key: String, userChannel: String, userId: String, userMetadata: String): Boolean = false

  override def uniformize(key: String, priority: String): Boolean = false

  override def clearAll(): Unit = redis.flushall

  def lineContainsUser(key: String, userChannel: String, userId: String): Option[String] ={
    //TODO: testar mais vezes! Verificar os significado da tupla de retorno do zscan!
    redis.zscan(key,0, queryUsersByIDAndChannel(userId, userChannel),1) match {
      case Some(t) =>
        t._2 match {
          case Some(scanRet) if scanRet.nonEmpty => scanRet.head
          case _ =>  None
        }
    }
  }

  override def getUser(key: String, userChannel: String, userId: String): Option[User] ={
    //TODO: testar mais vezes! Verificar os significado da tupla de retorno do zscan!
    redis.zscan(key,0, queryUsersByIDAndChannel(userId, userChannel),1) match {
      case Some(t) =>

        println(">>>>>>** T:"+t._2,t._2.nonEmpty)

        t._2 match {
          case Some(scanRet) if scanRet.nonEmpty =>
            scanRet.head match {
              case Some(userEntry) => resultToUser(userEntry.asInstanceOf[String])
              case None => None
            }

          case _ =>
            None
        }
    }
  }

}
