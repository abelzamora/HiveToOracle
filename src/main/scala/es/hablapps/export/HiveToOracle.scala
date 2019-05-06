package es.hablapps.export

import java.io.IOException

import es.hablapps.export.arg.Parameters.Arguments
import es.hablapps.export.arg.{Parameters, Yaml}
import es.hablapps.export.mapreduce.{MapReduceSpec, MapperExport}
import es.hablapps.export.rdbms.Procedure
import es.hablapps.export.rdbms.Rdbms.{HiveDb, OracleDb}
import es.hablapps.export.syntax.ThrowableOrValue
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Mapper
import org.apache.log4j.Logger
import scalaz.{Failure, NonEmptyList, Success, Validation, \/}

object Export {
  private val logger: Logger = Logger.getLogger("es.hablapps.export.ExportMapper")

  class ExportMapper extends Mapper[Object, Text, Text, IntWritable]{

    @throws(classOf[IOException])
    @throws(classOf[InterruptedException])
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val args: Arguments = Parameters.getArguments(context.getConfiguration.asInstanceOf[JobConf])
      val oracleDb: OracleDb = OracleDb(args.oracleConfig)
      val hiveDb: HiveDb = HiveDb(args.hiveConfig.`hive.server`, args.hiveConfig.`hive.port`, args.hiveConfig.`hive.principal`, args.hiveConfig.`hive.auth`)

      //FIXME be careful with this: ParVector is for running in parallel the array of exportation, sometimes it is returning this error: java.sql.SQLException: org.apache.thrift.transport.TTransportException
//      val fechas: ParVector[String] = value.toString.split(" ").toVector.par
      //TODO put this deserialization in a separate class
      val fechas: Vector[String] = value.toString.split(" ").toVector

      MapperExport(fechas)
        .run(context,
          args.hiveConfig.`hive.query`,
          args.hiveConfig.`hive.params`,
          args.oracleConfig.`oracle.query`,
          args.oracleConfig.`oracle.batchsize`)(hiveDb, oracleDb)

    }


     //FIXME This is the piece of the code which contronls the concurrent executions for the task within the mappers
    /*
    @throws(classOf[IOException])
    @throws(classOf[InterruptedException])
    override def run(context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      import scala.concurrent.ExecutionContext.Implicits.global
      type Ctx = Mapper[Object, Text, Text, IntWritable]#Context
      setup(context)
      try {

        implicit class ForeachAsync[T](ctx: Ctx) {
          def foreachAsync[U](implicit ec: ExecutionContext): Unit = {
            def next(i: Ctx): Unit = if (i.nextKeyValue()) Future(map(i.getCurrentKey, i.getCurrentValue, i)) onComplete (_ => next(i))
            next(ctx)
          }
        }

        context foreachAsync

      } finally {
        cleanup(context)
      }
    }
    */
  }

  @throws[Exception]
  def main(args: Array[String]) {

    implicit lazy val conf: Configuration = new Configuration

    val ret: Validation[NonEmptyList[Throwable], ThrowableOrValue[Int]] = for {
      arg <- Yaml(args).run
    } yield {
      for {
        _ <-  \/ fromTryCatchNonFatal {
          if(!arg.procedureConfig.`procedure.query`.isEmpty) {
            implicit val oracleDb: OracleDb = OracleDb(arg.oracleConfig)
            Procedure(arg.startDate, arg.endDate, arg.procedureConfig.`procedure.query`, arg.procedureConfig.`procedure.params`).run
          }
        }

        exit <- MapReduceSpec(arg.startDate, arg.endDate, arg.inputPath, arg.outputPath, arg.maps, arg.mapReduceParams)(arg).run
      } yield exit
    }

    ret match {
      case Failure(e) => e.foreach(e => logger.error(e)); System.exit(1)
      case Success(a) => a match {
        case t if t.isLeft => t.leftMap(c => logger.error(c.getMessage, c)); System.exit(1)
        case t if t.isRight => logger.info(s"OK! ${t.getOrElse(1)}")//System.exit(t.getOrElse(1))
      }
    }

  }
}

