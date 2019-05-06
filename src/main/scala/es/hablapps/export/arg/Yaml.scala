package es.hablapps.export.arg

import java.io.{File, FileInputStream, InputStream}
import java.util

import es.hablapps.export.arg.Parameters.{Arguments, HiveConfig, OracleConfig, ProcedureConfig}
import es.hablapps.export.syntax.{InvalidStateException, nonNull}
import es.hablapps.export.utils.Utils.{datetime_format, parsingParameterList}
import org.clapper.argot.ArgotConverters._
import org.clapper.argot.ArgotParser
import org.yaml.snakeyaml.{Yaml => YamlT}
import scalaz.syntax.apply._
import scalaz.{Failure, Success, Validation, ValidationNel}

import scala.collection.JavaConverters._
import scala.collection.mutable

case class Yaml(
                 args: Array[String]
               ) { self =>
  private[Yaml] implicit def toInt(input: String):Int = new Integer(input)

  //TODO return an Either, use toEither method of Validation.scala
  def run: ValidationNel[Throwable, Arguments] = {
    lazy val usage = """
                  Usage: yarn jar export-<Version>-jar-with-dependencies.jar es.hablapps.export.Export configFile <startDate> <endDate> [-Dmareduce parameters]
                """
    lazy val parameterMap: mutable.Map[String, Any] = mutable.Map[String, Any]()
    args match {
      case Array(c:String, startDate: String, endDate: String,  _*) if c.endsWith(".yaml") => {
        val mapReduceConf: Array[String] = util.Arrays.copyOfRange(args, 3, args.length)

        val parser = new ArgotParser(s"${this.getClass}", preUsage=Some("Hive To Orcle"))
        val mappingFile = parser.multiOption[String](List("D", "mareduce"), "parameter", "Parameters for mapreduce")

        parser.parse(mapReduceConf)

        mappingFile.value.foreach(p => {
          parameterMap += p.split("=")(0) -> p.split("=")(1)
        })
        self.buildYaml(c, startDate, endDate, parameterMap.toMap)
      }
      case Array(c:String, startDate: String, endDate: String) if c.endsWith(".yaml")=> {
        self.buildYaml(c, startDate, endDate, parameterMap.toMap)
      }
      case _ => Validation.failureNel(InvalidStateException(s"Incorrect number of elements. $usage"))
    }
  }


  private[this] def buildYaml(configPath: String, startDate: String, endDate: String, mapReduceParams: Map[String, Any]): ValidationNel[Throwable, Arguments] ={
    val inFile: InputStream = new FileInputStream (new File (configPath))
    val config: mutable.Map[String, Any] = new YamlT().load(inFile).asInstanceOf[java.util.Map[String, Any]].asScala

    val mapreduce: mutable.Map[String, Any] = config("mapreduce").asInstanceOf[java.util.Map[String, Any]].asScala
    val mapreducePath: mutable.Map[String, Any] = mapreduce("path").asInstanceOf[java.util.Map[String, Any]].asScala
    val hive: mutable.Map[String, Any] = config("hive").asInstanceOf[java.util.Map[String, Any]].asScala
    val oracle: mutable.Map[String, Any] = config("oracle").asInstanceOf[java.util.Map[String, Any]].asScala
    val procedure: mutable.Map[String, Any] = config("procedure").asInstanceOf[java.util.Map[String, Any]].asScala


    val hiveConfig: ValidationNel[Throwable, HiveConfig] = (
      nonNull[Any, String](hive("server"), "Hive server is mandatory")(_.toString)
        |@| nonNull[Any, Int](hive("port"), "Hive port is mandatory")(_.toString)
        |@| nonNull[Any, String](hive("principal"), "There is an error parsing hive principal argument")(s => if(s == null) "" else s.toString)
        |@| nonNull[Any, String](hive("auth"), "There is an error parsing hive authentication argument")(s => if(s == null) "" else s.toString)
        |@| nonNull[Any, String](hive("query"), "Hive query is mandatory")(_.toString)
        |@| nonNull[Any, String](hive("params"), "There is a problem parsing hive parameters query")(a => parsingParameterList(a, ","))
      )(HiveConfig.apply)

    val oracleConfig: ValidationNel[Throwable, OracleConfig] = (
      nonNull[Any, String](oracle("server"), "Oracle server is mandatory")(_.toString)
        |@| nonNull[Any, Int](oracle("port"), "Oracle port is mandatory")(_.toString)
        |@| nonNull[Any, String](oracle("user"), "Oracle user is mandatory")(_.toString)
        |@| nonNull[Any, String](oracle("password"), "Oracle password is mandatory")(_.toString)
        |@| nonNull[Any, String](oracle("serviceName"), "Oracle serviceName is mandatory")(_.toString)
        |@| nonNull[Any, String](oracle("query"), "Oracle query is mandatory")(_.toString)
        |@| nonNull[Any, Int](oracle("batchsize"), "Oracle batch size is mandatory")(_.toString)
      )(OracleConfig.apply)

    val procedureConfig: ValidationNel[Throwable, ProcedureConfig] = (
      nonNull[Any, String](procedure("query"), "There is a problem parsing the procedure query")(s => if(s == null) "" else s.toString)
        |@| nonNull[Any, String](procedure("params"), "There is a problem parsing procedure parameters query")(a => parsingParameterList(a, "@"))
      )(ProcedureConfig.apply)


    (hiveConfig, oracleConfig, procedureConfig) match {
      case (Success(hc), Success(oc), Success(pc)) =>  {
        self.buildArguments(
          mapreducePath("input"),
          mapreducePath("output"),
          mapreduce("maps"),
          startDate,
          endDate,
          hc,
          oc,
          pc,
          mapReduceParams
        )
      }

      case (Failure(hc), Failure(oc), Failure(pc)) => Validation.failure(hc.append(oc).append(pc))
      case (Failure(hc), _, _) => Validation.failure(hc)
      case (_, Failure(oc), _) => Validation.failure(oc)
      case (_, _, Failure(pc)) => Validation.failure(pc)
    }
  }

  private def buildArguments(inputPath: Any,
                             outputPath: Any,
                             maps: Any,
                             startDate: String,
                             endDate: String,
                             hiveConfig: HiveConfig,
                             oracleConfig: OracleConfig,
                             procedureConfig: ProcedureConfig,
                             mapReduceParams: Map[String, Any]
                            ): ValidationNel[Throwable, Arguments] = (
    nonNull[Any, String](inputPath, "InputPath is mandatory")(_.toString)
      |@| nonNull[Any, String](outputPath, "OutputPath is mandatory")(_.toString)
      |@| nonNull[Any, Int](maps, "Number of mappers is mandatory")(_.toString)
      |@| nonNull[String, String](startDate, "Start date is mandatory")(s => datetime_format.format(datetime_format.parse(s)))
      |@| nonNull[String, String](endDate, "End date is mandatory")(s => datetime_format.format(datetime_format.parse(s)))
      |@| nonNull[HiveConfig, HiveConfig](hiveConfig, "There is a problem building Hive Configuration")(identity)
      |@| nonNull[OracleConfig, OracleConfig](oracleConfig, "There is a problem building Oracle Configuration")(identity)
      |@| nonNull[ProcedureConfig, ProcedureConfig](procedureConfig, "There is a problem building Procedure Configuration")(identity)
      |@| nonNull[Map[String, Any], Map[String, Any]](mapReduceParams, "Mapreduce params contain an error")(identity)
    )(Arguments)


}
