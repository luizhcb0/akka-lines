package br.com.bb.horus.microservice.lines.dao

import br.com.bb.horus.microservice.lines.models.{Line, User}

object LineDAO{
  private val instance: LineDAO = new LineDAORedis

  def getInstance(): LineDAO = instance
}

trait LineDAO {

  def clearAll(): Unit

  def insert(key: String, name: String, metadata: String): Option[Line]

  def update(key: String, line: Line): Boolean

  def get(key: String): Option[Line]

  def getAll: Array[Line]

  def getUser(key: String, userChannel: String, userId: String): Option[User]

  def remove(key: String): Boolean

  def wipe(key: String): Boolean

  def retrieve(key: String): Option[Line]

  def list(metadata: String): Array[Line]

  def pushUser(key: String, userChannel: String, userId: String, userMetadata: String, priority: Int, atEnd: Boolean): Option[User]

  def popUser(key: String, fromEnd: Boolean): Option[User]

  def removeUser(key: String, userChannel: String, userId: String): Boolean

  def purgeUser(userChannel: String, userId: String): Boolean

  def enrichUser(key: String, userChannel: String, userId: String, userMetadata: String): Boolean

  def uniformize(key: String, priority: String): Boolean

  def listUsers(key: String): Seq[User]
}
