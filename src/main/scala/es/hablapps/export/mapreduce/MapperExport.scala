package es.hablapps.export.mapreduce

import java.sql.Connection

import es.hablapps.export.rdbms.Rdbms
import es.hablapps.export.security.Kerberos.doAsRealUser
import es.hablapps.export.syntax.ThrowableOrValue
import es.hablapps.export.utils.Utils
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.log4j.Logger
import doobie._
import doobie.implicits._
import cats.effect.{Effect, ExitCode, IO, Resource}
import cats.implicits._
import fs2.Stream

import scala.collection.immutable.IntMap
import scala.util.control.NonFatal

case class MapperExport(dates: Vector[String]) {

  private val logger: Logger = Logger.getLogger(classOf[MapperExport])

  def run(context: Mapper[Object, Text, Text, IntWritable]#Context,
          `from.query`: String,
          `from.params`: Option[List[String]],
          `to.query`: String,
          `to.batchsize`: Int
         )(fromRdbms: Rdbms, toRdbms: Rdbms): Unit = {

      //var total: Int = 0
      dates.foreach(date => {
        logger.info(s"Processing date $date")
        //TODO include these two methods into the companion object of the business program
        val hiveP: IntMap[AnyRef] = Utils.bindParametersReplacement(`from.params`, date)
        //val toQuery: String = Utils.bindReplacement(`to.query`, s"${StringUtils.remove(date, '-')}")

        //logger.info(s"ToQuery= $toQuery")

        for {
          _ <- fuseMap(fromRdbms.query(hiveP)(`from.query`), toRdbms.write(2))(fromRdbms.xa, toRdbms.xa).compile.drain // do the copy with fuseMap
          n <- sql"select count(*) from students2".query[Int].unique.transact(toRdbms.xa)
          _ <- IO(Console.println(s"Copied $n students!"))
        } yield ExitCode.Success


//        for {
//          res <- doAsRealUser(fromRdbms.query(`from.query`, strMap = hiveP))
//        } yield res.flatMap(r => IO(logger.info(r.toString)))


        //        val batch: ThrowableOrValue[Int] = for {
//          rs <- doAsRealUser(fromRdbms.query(query = `from.query`, strMap = hiveP))
//          accumulator <- toRdbms.batch(query = toQuery, batchSize = `to.batchsize`, rs = rs)(context = context)
//        } yield accumulator
//
//        batch match {
//          case Left(l) => logger.error(l.getMessage, l); System.exit(1)
//          case Right(_) => logger.info(s"Exported ${batch.getOrElse(0)} for date $date"); total += batch.getOrElse(0)
//        }

      })
//      logger.info(s"Total number of rows exported for this values: $total | ${dates.toString}")
  }


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

}
