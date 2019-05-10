package es.hablapps.export.arg

import java.nio.file.Path

import pureconfig._
import pureconfig.Derivation._
import pureconfig.generic.auto._

import scala.reflect.ClassTag

//TODO create new class with the configuration for mapReduce
case class Arguments(
                      inputPath: String,
                      outputPath: String,
                      maps: Int,
                      hiveConfig: HiveConfig,
                      oracleConfig: OracleConfig,
                      procedureConfig: Option[ProcedureConfig]
                    )
//TODO create a new class with the program parameters (startDate, endDate, queries, batchSize and procedureConfig)
case class HiveConfig (
                        server: String,
                        port: Int,
                        principal: String,
                        auth: String,
                        query: String,
                        params: Option[List[String]]
                      )

case class OracleConfig (
                          server:String,
                          port:Int,
                          user:String,
                          password:String,
                          serviceName:String,
                          query: String,
                          batchSize: Int
                        )


case class ProcedureConfig (
                             query: String,
                             params: String
                           )

object AppConfig {
  def read[A](path: Path, namespace: String)(implicit ca: ClassTag[A], reader: Derivation[ConfigReader[A]]): A = pureconfig.loadConfigOrThrow[A](path, namespace)
}

