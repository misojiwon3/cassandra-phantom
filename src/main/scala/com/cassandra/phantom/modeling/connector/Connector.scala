package com.cassandra.phantom.modeling.connector

import com.outworkers.phantom.connectors.{CassandraConnection, ContactPoint, ContactPoints}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object Connector {
  val log = LoggerFactory.getLogger(getClass)
  private val config = ConfigFactory.load()

  private val hosts = config.getStringList("cassandra.host")
  private val keyspace = config.getString("cassandra.keyspace")
  private val username = config.getString("cassandra.username")
  private val password = config.getString("cassandra.password")

  /**
    * Create a connector with the ability to connects to
    * multiple hosts in a secured cluster
    */

  lazy val connector: CassandraConnection = ContactPoints(hosts)
    .withClusterBuilder(_.withCredentials(username, password))
    .keySpace(keyspace)

  /**
    * Create an embedded connector, testing purposes only
    */
  lazy val testConnector: CassandraConnection = ContactPoint.local.noHeartbeat().keySpace("test") // local : 9042, embedded : 9142
}
