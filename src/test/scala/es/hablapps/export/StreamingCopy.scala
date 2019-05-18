package es.hablapps.export

import java.sql.{Connection, PreparedStatement, ResultSet}

import cats.effect.{ExitCode, IO, IOApp}

import scala.util.control.NonFatal
import cats.data._
import cats.effect._
import cats.effect.syntax.bracket._
import cats.implicits._
import doobie._
import doobie.implicits._
import fs2.Stream
import es.hablapps.export.DatabaseConfig._
import fs2.Stream.{bracket, eval}
import doobie.implicits.{AsyncConnectionIO, AsyncPreparedStatementIO}
/**
  * Example of resource-safe transactional database-to-database copy with fs2. If you induce failures
  * on either side (by putting a typo in the `read` or `write` statements) both transactions will
  * roll back.
  */
object StreamingCopy extends IOApp {
  implicit val han = LogHandler.jdkLogHandler

  /**
    * Stream from `source` through `sink`, where source and sink run on distinct transactors. To do
    * this we have to wrap one transactor around the other. Thanks to @wedens for
    */
  def fuseMap[F[_]: Effect, A, B](
                                   source: Stream[ConnectionIO, A],
                                   sink:   A => ConnectionIO[B]
                                 )(
                                   sourceXA: Transactor[F],
                                   sinkXA:   Transactor[F]
                                 ): Stream[F, B] = {

    // Interpret a ConnectionIO into a Kleisli arrow for F via the sink interpreter.
    def interpS[T](f: ConnectionIO[T]): Connection => F[T] =
      f.foldMap(sinkXA.interpret).run

    // Open a connection in `F` via the sink transactor. Need patmat due to the existential.
    val conn: Resource[F, Connection] =
      sinkXA match { case xa => xa.connect(xa.kernel) }

    // Given a Connection we can construct the stream we want.
    def mkStream(c: Connection): Stream[F, B] = {

      // Now we can interpret a ConnectionIO into a Stream of F via the sink interpreter.
      def evalS(f: ConnectionIO[_]): Stream[F, Nothing] =
        Stream.eval_(interpS(f)(c))

      // And can thus lift all the sink operations into Stream of F
      lazy val sinkʹ  = (a: A) => evalS(sink(a))
      lazy val before = evalS(sinkXA.strategy.before)
      lazy val after  = evalS(sinkXA.strategy.after )
      def oops(t: Throwable) = evalS(sinkXA.strategy.oops <* FC.raiseError(t))

      // And construct our final stream.
      (before ++ source.transact(sourceXA).flatMap(sinkʹ) ++ after).onError {
        case NonFatal(e) => oops(e)
      }

    }

    // And we're done!
    Stream.resource(conn).flatMap(mkStream)

  }

  // Everything below is code to demonstrate the combinator above.

  /** Prepend a ConnectionIO program with a log message. */
  def printBefore(tag: String, s: String): ConnectionIO[Unit] => ConnectionIO[Unit] =
    HC.delay(Console.println(s"$tag: $s")) <* _

  /** Derive a new transactor that logs stuff. */
  def addLogging[F[_], A](name: String)(xa: Transactor[F]): Transactor[F] = {
    import Transactor._ // bring the lenses into scope
    val update: State[Transactor[F], Unit] =
      for {
        _ <- before %= printBefore(name, "before - setting up the connection")
        _ <- after  %= printBefore(name, "after - committing")
        _ <- oops   %= printBefore(name, "oops - rolling back")
        _ <- always %= printBefore(name, "always - closing")
      } yield ()
    update.runS(xa).value
  }

  def liftProcessGeneric(
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
  def read: Stream[ConnectionIO, ResultSet] = {
    val query: Query[Unit, Unit] = Query(s"""
      SELECT *
      FROM students
      where name = ?
    """)

    liftProcessGeneric(512, FC.prepareStatement(query.sql), HPS.set(1, "students_1"), FPS.executeQuery)
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  def write(rs: ResultSet): ConnectionIO[Unit] =
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

  /** Table creation for our destination DB. We assume the source is populated. */
  val ddl: ConnectionIO[Unit] =
    sql"""
      CREATE TABLE students2
       (
       	name         VARCHAR(20),
                    age NUMBER,
                    gpa NUMBER
       )
    """.update.run.void


  // A postges transactor for our source. We assume the WORLD database is set up already.
  val hive = addLogging("From"){
    val s = doobie.util.transactor.Strategy(FC.unit, FC.unit, FC.unit, FC.unit)
    Transactor.fromDriverManager[IO](
      driver,
      url,
      user,
      password
    ).copy(strategy0 = s)
  }


  // An h2 transactor for our sink.
  val oracle = addLogging("To") {
    val s = doobie.util.transactor.Strategy.default
    val xa = Transactor.fromDriverManager[IO](
      "oracle.jdbc.driver.OracleDriver",
      "jdbc:oracle:thin:@127.0.0.1:1521:xe",
      "system",
      "oracle"
    ).copy(strategy0 = s)
//    Transactor.before.modify(xa, _ *> ddl) // Run our DDL on every connection
    xa
  }

  // Our main program
  def run(args: List[String]): IO[ExitCode] =
    for {
      _ <- fuseMap(read, write)(hive, oracle).compile.drain // do the copy with fuseMap
      n <- sql"select count(*) from students2".query[Int].unique.transact(oracle)
      _ <- IO(Console.println(s"Copied $n students!"))
    } yield ExitCode.Success

}


object DatabaseConfig {
                           val driver: String = "org.apache.hive.jdbc.HiveDriver"
                           val url: String = "jdbc:hive2://localhost:10000/default"
                           val user: String = ""
                           val password: String = ""
}
