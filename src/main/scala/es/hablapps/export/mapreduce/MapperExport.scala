package es.hablapps.export.mapreduce

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
import cats.effect.{IO}
import cats.implicits._

import scala.collection.immutable.IntMap

case class MapperExport(dates: Vector[String]) {

  private val logger: Logger = Logger.getLogger(classOf[MapperExport])

  def run(context: Mapper[Object, Text, Text, IntWritable]#Context,
          `from.query`: String,
          `from.params`: Option[List[String]],
          `to.query`: String,
          `to.batchsize`: Int
         )(fromRdbms: Rdbms, toRdbms: Rdbms): Unit = {

    try {
      //var total: Int = 0
      dates.foreach(date => {
        logger.info(s"Processing date $date")
        //TODO include these two methods into the companion object of the business program
        val hiveP: IntMap[AnyRef] = Utils.bindParametersReplacement(`from.params`, date)
        //val toQuery: String = Utils.bindReplacement(`to.query`, s"${StringUtils.remove(date, '-')}")

        //logger.info(s"ToQuery= $toQuery")

        for {
          res <- doAsRealUser(fromRdbms.query(`from.query`, strMap = hiveP))
        } yield res.flatMap(r => IO(logger.info(r.toString)))


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
    } catch {
        case e:Throwable => logger.error("Error exporting the data", e)
      }finally {
          fromRdbms.closeConnection()
          val _ = toRdbms.closeConnection()
        }
  }

}
