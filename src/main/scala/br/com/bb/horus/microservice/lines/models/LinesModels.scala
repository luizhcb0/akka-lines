package br.com.bb.horus.microservice.lines.models

import scala.collection.concurrent.TrieMap

case class User(id: String, channel: String, metadata: String)
case class Line(key: String, name: String, metadata: String, createdAt: Long, updatedAt: Long)