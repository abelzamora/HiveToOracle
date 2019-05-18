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

import cats.effect.{ExitCode, IO, IOApp}

import scala.util.control.NonFatal
import cats.data._
import cats.effect._
import cats.effect.syntax.bracket._
import cats.implicits._
import doobie._
import doobie.implicits._
import fs2.Stream
import fs2.Stream.{bracket, eval}
import doobie.implicits.{AsyncConnectionIO, AsyncPreparedStatementIO}

import scala.collection.immutable.IntMap
import doobie._
import doobie.implicits._
import fs2.Stream
import fs2.Stream.{bracket, eval}

import scala.concurrent.ExecutionContext

trait Rdbms { self =>

  val driver: String
  val connectionString: String
  def setDelegationToken(creds: Credentials): ThrowableOrValue[Unit]
  type Headers = List[String]
  type Data    = List[List[Object]]

//  private val logger: Logger = Logger.getLogger("es.hablapps.export.rdbms.Rdbms")
  implicit val cs = IO.contextShift(ExecutionContext.global)

  lazy val xa = Transactor.fromDriverManager[IO](
    self.driver,
    self.connectionString
  ).copy(strategy0 = doobie.util.transactor.Strategy.default)

  private[this] def liftProcessGeneric(
                          chunkSize: Int,
                          create: ConnectionIO[PreparedStatement],
                          prep:   PreparedStatementIO[Unit],
                          exec:   PreparedStatementIO[ResultSet]): Stream[ConnectionIO, ResultSet] = {

    def prepared(ps: PreparedStatement): Stream[ConnectionIO, PreparedStatement] =
      eval[ConnectionIO, PreparedStatement] {
        val fs = FPS.setFetchSize(chunkSize)
        FC.embed(ps, fs *> prep).map(_ => ps)
      }

    val preparedStatement: Stream[ConnectionIO, PreparedStatement] =
      bracket(create)(FC.embed(_, FPS.close)).flatMap(prepared)

    def results(ps: PreparedStatement): Stream[ConnectionIO, ResultSet] =
      bracket(FC.embed(ps, exec))(FC.embed(_, FRS.close))

    preparedStatement.flatMap(results)

  }

  // A producer of cities, to be run on database 1
  def query(strMap: IntMap[AnyRef] = IntMap.empty[AnyRef]): String => Stream[ConnectionIO, ResultSet] = sql => {

    val res = liftProcessGeneric(512, FC.prepareStatement(sql), HPS.set(1, "students_1"), FPS.executeQuery)
    res.map(rs => {
      while(rs.next()) {
        println(rs.getObject(1))
        (rs.getObject(1))
      }
    })
    res
  }

  /** Prepend a ConnectionIO program with a log message. */
  def printBefore(tag: String, s: String): ConnectionIO[Unit] => ConnectionIO[Unit] =
    HC.delay(Console.println(s"$tag: $s")) <* _


  def write(chunkSize: Int): ResultSet => ConnectionIO[Unit] = rs =>
    printBefore("write", rs.toString){

      FC.prepareStatement("INSERT /*+ APPEND_VALUES */ INTO students2 VALUES (?,?,?)").bracket { ps =>
        FC.embed(ps, writerPS(rs, ps, 2))
      }(FC.embed(_, FPS.close))
    }

  def writerPS(rs: ResultSet, ps:PreparedStatement, chunkSize: Int): PreparedStatementIO[Unit] = {
    @annotation.tailrec
    def writerPS0(rs: ResultSet, ps:PreparedStatement, rowCount: Int): PreparedStatementIO[Unit] = {
      rs.next() match {
        case false =>
          FPS.executeBatch.map(_ => ())
        case true => {
          for {i <- 1 to rs.getMetaData.getColumnCount} ps.setObject(i, rs.getObject(i))
          ps.addBatch()
          if(rowCount % chunkSize == 0 ){
            FPS.executeBatch.map(_ => ())
          }
          writerPS0(rs, ps, rowCount + 1)
        }
      }
    }
    writerPS0(rs, ps, 1)
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
