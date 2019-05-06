package es.hablapps.export.rdbms

import java.sql._
import java.util.concurrent.locks.{Lock, ReentrantLock}

import es.hablapps.export.arg.Parameters.OracleConfig
import es.hablapps.export.security.Kerberos.getRealUser
import es.hablapps.export.syntax.{InvalidStateException, ThrowableOrValue}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, TaskCounter}
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.Token
import org.apache.hive.jdbc.HiveConnection
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.log4j.Logger
import scalaz.Scalaz._
import scalaz._

import scala.collection.immutable.IntMap

trait Rdbms { self =>

  private[this] val readLock :Lock = new ReentrantLock()
  private[this] val writeLock :Lock = new ReentrantLock()
  val driver: Class[_]
  val connectionString: String
  def setDelegationToken(creds: Credentials): ThrowableOrValue[Unit]

  private val logger: Logger = Logger.getLogger("es.hablapps.export.rdbms.Rdbms")

  def readLock[A](f: => A): A ={
    try{
      readLock.lock()
      f
    } finally readLock.unlock()
  }


  def writeLock[A](f: => A): A ={
    try{
      writeLock.lock()
      f
    } finally writeLock.unlock()
  }

  private[this] var lastConnection: ThrowableOrValue[Connection] =
    InvalidStateException("Connection hasn't been initialized").left

  def getConnection(url: String = connectionString): ThrowableOrValue[Connection] = lastConnection match {
    case \/-(c) if !c.isClosed => c.right
    case _ =>
      try lastConnection = DriverManager.getConnection(url).right
      catch { case e: SQLException => lastConnection = e.left }
      lastConnection
  }

  def closeConnection(url: String = connectionString): ThrowableOrValue[Unit] =
      for {connection <- getConnection(url)} yield connection.close()


  def query(query: String, strMap: IntMap[AnyRef]): ThrowableOrValue[ResultSet] = {
    for {
      sqlQuery <- self.createStatement(query, strMap)
      rs <- self.execute(sqlQuery)
    } yield rs
  }

  private[this] def execute(pStm: PreparedStatement): ThrowableOrValue[ResultSet] =
    try {
      pStm.executeQuery().right
    } catch {
      case e: java.lang.Exception => e.left
    }


  def batch(query: String, batchSize: Int, strMap: IntMap[AnyRef] = IntMap.empty[AnyRef], rs:ResultSet)(context:Mapper[Object, Text, Text, IntWritable]#Context): ThrowableOrValue[Int] = {
    for {
      pStm <- self.createEmptyStatement(query)
      accumulator <- self.executeBatch(batchSize = batchSize, rs = rs, pStm = pStm)(context=context)
    } yield accumulator
  }

  private[this] def executeBatch(batchSize: Int, rs: ResultSet, pStm: PreparedStatement)(context:Mapper[Object, Text, Text, IntWritable]#Context): ThrowableOrValue[Int] = {
    for {
      conn <- getConnection()
    } yield {
      conn.setAutoCommit(false)
      var rowCount: Long = 0L

      while (rs.next()) {

        for {i <- 1 to rs.getMetaData.getColumnCount} {
          pStm.setObject(i, rs.getObject(i))
        }

        pStm.addBatch()
        rowCount += 1
        if (rowCount % batchSize == 0) {
          //logger.info(s"Executing batch with rowcount: ${rowCount.toString}" )

          pStm.executeBatch()
          conn.commit()

          // Need to commit because if not done, we will get the "ORA-12838: cannot read/modify an object after modifying it in parallel" error in the next batch
          context.getCounter(TaskCounter.MAP_OUTPUT_RECORDS).increment(batchSize)
          context.progress()
          //logger.info(s"Updated counter: ${context.getCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue}")

        }
      }
      pStm.executeBatch()
      rowCount.toInt
    }
  }

  private[this] def createEmptyStatement: ThrowableOrValue[Statement] =
    for {connection <- getConnection() } yield connection.createStatement()

  private[this] def createStatement(sql: String, placeholderMap: IntMap[AnyRef]): ThrowableOrValue[PreparedStatement] =
    self.createEmptyStatement(sql).map { ps: PreparedStatement =>
      placeholderMap.foreach {
        case (index, value) => ps.setObject(index, value)
      }
      ps
    }

  private def createEmptyStatement(sql: String): ThrowableOrValue[PreparedStatement] =
    for { connection <- getConnection() } yield connection.prepareStatement(sql)

  def printConfiguration: ThrowableOrValue[String] = {
    for {
      conn <- getConnection()
    } yield s"""Configuration:
       | $driver
       | Url:                       ${conn.getMetaData.getURL}
       | Database Product Name:     ${conn.getMetaData.getDatabaseProductName}
       | Database Product Version:  ${conn.getMetaData.getDatabaseProductVersion}
       | Logged User:               ${conn.getMetaData.getUserName}
       | JDBC Driver:               ${conn.getMetaData.getDriverName}
       | Driver Version:            ${conn.getMetaData.getDriverVersion}""".stripMargin
  }

}

object Rdbms {

  case class OracleDb(
                       config: OracleConfig
                     )extends Rdbms {
    override val driver: Class[_] = Class.forName("oracle.jdbc.driver.OracleDriver")
    override val connectionString: String = s"""jdbc:oracle:thin:${config.`oracle.user`}/${config.`oracle.password`}@//${config.`oracle.server`}:${config.`oracle.port`}/${config.`oracle.serviceName`}"""

    override def setDelegationToken(creds: Credentials): ThrowableOrValue[Unit] = ???
  }

  case class HiveDb(
                     host: String,
                     port: Int,
                     principal: String,
                     security: String
                  )extends Rdbms {
    override val driver: Class[_] = Class.forName("org.apache.hive.jdbc.HiveDriver")

    val `hive.url`: String = s"""jdbc:hive2://$host:$port/"""

    override val connectionString: String = {
      if(null == security || StringUtils.isEmpty(security))
        s"""${`hive.url`}default"""
      else
        s"""${`hive.url`};$security"""
    }

    override def setDelegationToken(creds: Credentials):ThrowableOrValue[Unit] = \/ fromTryCatchNonFatal {
      for{
        connStr <- \/ fromTryCatchNonFatal {s"${`hive.url`};principal=$principal"}
        conn <- getConnection(connStr)
        tokenStr <- \/ fromTryCatchNonFatal {
          val shortUserName: String = getRealUser.getShortUserName
          val tknStr: String = conn.asInstanceOf[HiveConnection].getDelegationToken(shortUserName, principal)
          logger.info(s"Getting this token $tknStr from this user $shortUserName and this principal $principal")
          tknStr
        }
        _ <- closeConnection(connStr)
      } yield {
        val hive2Token = new Token()
        hive2Token.decodeFromUrlString(tokenStr)
        creds.addToken(new Text("hive.server2.delegation.token"), hive2Token)
        creds.addToken(new Text(HiveAuthFactory.HS2_CLIENT_TOKEN), hive2Token)
      }
    }
  }

}
