package es.hablapps.export.rdbms

import java.sql._
import java.util.concurrent.locks.{Lock, ReentrantLock}

import cats.effect.internals.IOAppPlatform
import cats.effect.{ContextShift, ExitCode, IO, IOApp, Resource}
import es.hablapps.export.security.Kerberos.getRealUser
import es.hablapps.export.syntax.{InvalidStateException, ThrowableOrValue}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, TaskCounter}
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.Token
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.log4j.Logger
import cats.implicits._
import es.hablapps.export.arg.OracleConfig

import scala.collection.immutable.IntMap
import doobie._
import doobie.implicits._

import scala.concurrent.ExecutionContext

trait Rdbms { self =>

  private[this] val readLock :Lock = new ReentrantLock()
  private[this] val writeLock :Lock = new ReentrantLock()
  val driver: String
  val connectionString: String
  def setDelegationToken(creds: Credentials): ThrowableOrValue[Unit]
  type Headers = List[String]
  type Data    = List[List[Object]]

  private val logger: Logger = Logger.getLogger("es.hablapps.export.rdbms.Rdbms")
  implicit def contextShift: ContextShift[IO] = ???

  lazy val xa = Transactor.fromDriverManager[IO](
    self.driver,
    self.connectionString
  )

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
    Left(InvalidStateException("Connection hasn't been initialized"))

  def getConnection(url: String = connectionString): ThrowableOrValue[Connection] = lastConnection match {
    case Right(c) if !c.isClosed => Right(c)
    case _ =>
      try lastConnection = Right(DriverManager.getConnection(url))
      catch { case e: SQLException => lastConnection = Left(e) }
      lastConnection
  }

  def closeConnection(url: String = connectionString): ThrowableOrValue[Unit] =
      for {connection <- getConnection(url)} yield connection.close()


  def query(query: String, strMap: IntMap[AnyRef]) = {
    Either.catchNonFatal(
      sql"select * from students"
        .execWith(execute)
        .transact(xa)
        .flatMap { case (headers, data) =>
      for {
        _ <- IO(println(headers))
        _ <- data.traverse(d => IO(println(d)))
      } yield ExitCode.Success
    })
  }

  private[this] def execute: PreparedStatementIO[(Headers, Data)] =
    for {
      md   <- HPS.getMetaData // lots of useful info here
      cols  = (1 to md.getColumnCount).toList
      data <- HPS.executeQuery(readAll(cols))
    } yield (cols.map(md.getColumnName), data)


  // Read the specified columns from the resultset.
  def readAll(cols: List[Int]): ResultSetIO[Data] =
    readOne(cols).whileM[List](HRS.next)

  // Take a list of column offsets and read a parallel list of values.
  def readOne(cols: List[Int]): ResultSetIO[List[Object]] =
    cols.traverse(FRS.getObject) // always works


  def batch(query: String, batchSize: Int, strMap: IntMap[AnyRef] = IntMap.empty[AnyRef], rs:ResultSet)
           (context:Mapper[Object, Text, Text, IntWritable]#Context)
    : ThrowableOrValue[Int] = {
      for {
        pStm <- self.createEmptyStatement(query)
        accumulator <- self.executeBatch(batchSize = batchSize, rs = rs, pStm = pStm)(context=context)
      } yield accumulator
  }

  private[this] def executeBatch(batchSize: Int, rs: ResultSet, pStm: PreparedStatement)
                                (context:Mapper[Object, Text, Text, IntWritable]#Context): ThrowableOrValue[Int] = {
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
          logger.info(s"Executing batch with rowcount: ${rowCount.toString}" )

          pStm.executeBatch()
          conn.commit()

          // Need to commit because if not done, we will get the "ORA-12838: cannot read/modify an object after modifying it in parallel" error in the next batch
          //context.getCounter(TaskCounter.MAP_OUTPUT_RECORDS).increment(batchSize)
          context.progress()
          logger.info(s"Updated counter: ${context.getCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue}")

        }
      }
      pStm.executeBatch()
      rowCount.toInt
    }
  }

//  private[this] def createEmptyStatement: ThrowableOrValue[Statement] =
//    for {connection <- getConnection() } yield connection.createStatement()


  //    self.createEmptyStatement(sql).map { ps: PreparedStatement =>
//      placeholderMap.foreach {
//        case (index, value) => ps.setObject(index, value)
//      }
//      ps
//    }

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
    override val driver: String = "oracle.jdbc.driver.OracleDriver"
    override val connectionString: String = s"""jdbc:oracle:thin:${config.user}/${config.password}@//${config.server}:${config.port}/${config.serviceName}"""

    override def setDelegationToken(creds: Credentials): ThrowableOrValue[Unit] = ???
  }

  case class HiveDb(
                     host: String,
                     port: Int,
                     principal: String,
                     security: String
                  )extends Rdbms {
    override val driver: String = "org.apache.hive.jdbc.HiveDriver"

    val `hive.url`: String = s"""jdbc:hive2://$host:$port/"""

    override val connectionString: String = {
      if(null == security || StringUtils.isEmpty(security))
        s"""${`hive.url`}default"""
      else
        s"""${`hive.url`};$security"""
    }

    override def setDelegationToken(creds: Credentials):ThrowableOrValue[Unit] = ???
//    Either.catchNonFatal {
//      for{
//        connStr <- Either.catchNonFatal{s"${`hive.url`};principal=$principal"}
//        conn <- getConnection(connStr)
//        tokenStr <- Either.catchNonFatal{
//          val shortUserName: String = getRealUser.getShortUserName
//          conn.asInstanceOf[HiveConnection].getDelegationToken(shortUserName, principal)
//        }
//        _ <- closeConnection(connStr)
//      } yield {
//        val hive2Token = new Token()
//        hive2Token.decodeFromUrlString(tokenStr)
//        creds.addToken(new Text("hive.server2.delegation.token"), hive2Token)
//        creds.addToken(new Text(HiveAuthFactory.HS2_CLIENT_TOKEN), hive2Token)
//      }
//    }
  }

}
