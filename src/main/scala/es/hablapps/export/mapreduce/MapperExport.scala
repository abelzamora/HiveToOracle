package es.hablapps.export.mapreduce

import es.hablapps.export.rdbms.Rdbms
import es.hablapps.export.security.Kerberos.doAsRealUser
import es.hablapps.export.syntax.ThrowableOrValue
import es.hablapps.export.utils.Utils
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.log4j.Logger

import scala.collection.immutable.IntMap

case class MapperExport(fechas: Vector[String]) {

  private val logger: Logger = Logger.getLogger(classOf[MapperExport])

  def run(context: Mapper[Object, Text, Text, IntWritable]#Context,
          `from.query`: String,
          `from.params`: String,
          `to.query`: String,
          `to.batchsize`: Int
         )(fromRdbms: Rdbms, toRdbms: Rdbms): Unit = {

    try {
      var total: Int = 0
      fechas.foreach(fecha => {
        logger.info(s"Processing date $fecha")
        //TODO include these two methods into the companion object of the business program
        val hiveP: IntMap[AnyRef] = Utils.bindParametersReplacement(`from.params`, fecha)
        val toQuery: String = Utils.bindReplacement(`to.query`, s"${StringUtils.remove(fecha, '-')}")

        logger.info(s"ToQuery= $toQuery")

        val batch: ThrowableOrValue[Int] = for {
          rs <- doAsRealUser(fromRdbms.query(query = `from.query`, strMap = hiveP))
          accumulator <- toRdbms.batch(query = toQuery, batchSize = `to.batchsize`, rs = rs)(context = context)
        } yield accumulator

        batch match {
          case Left(l) => logger.error(l.getMessage, l); System.exit(1)
          case Right(_) => logger.info(s"Exported ${batch.getOrElse(0)} for date $fecha"); total += batch.getOrElse(0)
        }

      })
      logger.info(s"Total number of rows exported for this values: $total | ${fechas.toString}")
    } catch {
        case e:Throwable => logger.error("Error exporting the data", e)
      }finally {
          for {
            _ <- fromRdbms.closeConnection()
            _ <- toRdbms.closeConnection()
          } yield ()
        }
  }

}
