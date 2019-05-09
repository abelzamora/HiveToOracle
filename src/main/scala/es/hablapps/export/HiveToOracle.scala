package es.hablapps.export

import java.io.{File, IOException}

import es.hablapps.export.arg.{AppConfig, Arguments, Parameters, Yaml}
import es.hablapps.export.mapreduce.{MapReduceSpec, MapperExport}
import es.hablapps.export.rdbms.Procedure
import es.hablapps.export.rdbms.Rdbms.{HiveDb, OracleDb}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Mapper
import org.apache.log4j.Logger
import cats.implicits._
import pureconfig.Derivation._
import pureconfig.generic.auto._

object Export {
  private val logger: Logger = Logger.getLogger("es.hablapps.export.ExportMapper")

  class ExportMapper extends Mapper[Object, Text, Text, IntWritable]{

    @throws(classOf[IOException])
    @throws(classOf[InterruptedException])
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val args: Arguments = Parameters.getArguments(context.getConfiguration.asInstanceOf[JobConf])
      val oracleDb: OracleDb = OracleDb(args.oracleConfig)
      val hiveDb: HiveDb = HiveDb(args.hiveConfig.server, args.hiveConfig.port, args.hiveConfig.principal, args.hiveConfig.auth)

      //TODO put this deserialization in a separate class
      val fechas: Vector[String] = value.toString.split(" ").toVector

      MapperExport(fechas)
        .run(context,
          args.hiveConfig.query,
          args.hiveConfig.params,
          args.oracleConfig.query,
          args.oracleConfig.batchsize)(hiveDb, oracleDb)

    }
  }

  @throws[Exception]
  def main(args: Array[String]) {
    implicit lazy val conf: Configuration = new Configuration

    val yamlConfig = Yaml.parse(args)
    val arg: Arguments = AppConfig.read[Arguments](new File(yamlConfig.configFile).toPath, "hiveToOracle")


    val ret = for {
      _ <-  Either.catchNonFatal{
        if(!arg.procedureConfig.query.isEmpty) {
          implicit val oracleDb: OracleDb = OracleDb(arg.oracleConfig)
          Procedure(yamlConfig.startDate, yamlConfig.endDate, arg.procedureConfig.query, arg.procedureConfig.params).run
        }
      }
      exit <- MapReduceSpec(yamlConfig.startDate, yamlConfig.endDate, arg.inputPath, arg.outputPath, arg.maps, yamlConfig.mapReduce)(arg).run
    } yield exit

    ret match {
      case Left(l) => logger.error(l.getMessage, l); System.exit(1)
      case Right(_) => logger.info(s"OK!")
    }

  }
}

