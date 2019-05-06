package es.hablapps.export.arg

import es.hablapps.export.utils.Constants.{`hive.auth`, `hive.params`, `hive.port`, `hive.principal`, `hive.query`, `hive.server`, `oracle.batchsize`, `oracle.password`, `oracle.port`, `oracle.query`, `oracle.server`, `oracle.serviceName`, `oracle.user`, `procedure.params`, `procedure.query`, _}
import org.apache.hadoop.conf.Configuration

object Parameters {

  //TODO create new class with the configuration for mapReduce
  case class Arguments(
                        inputPath: String,
                        outputPath: String,
                        maps: Int,
                        startDate: String,
                        endDate: String,
                        hiveConfig: HiveConfig,
                        oracleConfig: OracleConfig,
                        procedureConfig: ProcedureConfig,
                        mapReduceParams: Map[String, Any]
                      )
  //TODO create a new class with the program parameters (startDate, endDate, queries, batchSize and procedureConfig)
  case class HiveConfig (
                          `hive.server`: String,
                          `hive.port`: Int,
                          `hive.principal`: String,
                          `hive.auth`: String,
                          `hive.query`: String,
                          `hive.params`: String
                        )

  case class OracleConfig (
                            `oracle.server`:String,
                            `oracle.port`:Int,
                            `oracle.user`:String,
                            `oracle.password`:String,
                            `oracle.serviceName`:String,
                            `oracle.query`: String,
                            `oracle.batchsize`: Int
                          )


  case class ProcedureConfig (
                               `procedure.query`: String,
                               `procedure.params`: String
                             )


  def setArguments[A <: Configuration](args: Arguments)(jobConf: A): A = {

    jobConf.set(`export.inputpath`, args.inputPath)
    jobConf.set(`export.outputpath`, args.outputPath)
    jobConf.setInt(`export.maps`, args.maps)
    jobConf.set(`export.date.start`, args.startDate)
    jobConf.set(`export.date.end`, args.endDate)

    jobConf.set(`hive.server`, args.hiveConfig.`hive.server`)
    jobConf.setInt(`hive.port`, args.hiveConfig.`hive.port`)
    jobConf.set(`hive.principal`, args.hiveConfig.`hive.principal`)
    jobConf.set(`hive.query`, args.hiveConfig.`hive.query`)
    jobConf.set(`hive.auth`, args.hiveConfig.`hive.auth`)
    jobConf.set(`hive.params`, args.hiveConfig.`hive.params`)

    jobConf.set(`oracle.server`, args.oracleConfig.`oracle.server`)
    jobConf.setInt(`oracle.port`, args.oracleConfig.`oracle.port`)
    jobConf.set(`oracle.user`, args.oracleConfig.`oracle.user`)
    jobConf.set(`oracle.password`, args.oracleConfig.`oracle.password`)
    jobConf.set(`oracle.serviceName`, args.oracleConfig.`oracle.serviceName`)
    jobConf.set(`oracle.query`, args.oracleConfig.`oracle.query`)
    jobConf.setInt(`oracle.batchsize`, args.oracleConfig.`oracle.batchsize`)

    jobConf.set(`procedure.query`, args.procedureConfig.`procedure.query`)
    jobConf.set(`procedure.params`, args.procedureConfig.`procedure.params`)

    jobConf
  }

  def getArguments[A <: Configuration](jobConf: A): Arguments = {

    val hiveConfig = HiveConfig(
      `hive.server` = jobConf.get(`hive.server`),
      `hive.port` = jobConf.getInt(`hive.port`, 0),
      `hive.principal` = jobConf.get(`hive.principal`),
      `hive.auth` = jobConf.get(`hive.auth`),
      `hive.query` = jobConf.get(`hive.query`),
      `hive.params` = jobConf.get(`hive.params`)
    )

    val oracleConfig = OracleConfig(
      `oracle.server` = jobConf.get(`oracle.server`),
      `oracle.port` = jobConf.getInt(`oracle.port`, 0),
      `oracle.user` = jobConf.get(`oracle.user`),
      `oracle.password` = jobConf.get(`oracle.password`),
      `oracle.serviceName` = jobConf.get(`oracle.serviceName`),
      `oracle.query` = jobConf.get(`oracle.query`),
      `oracle.batchsize` = jobConf.getInt(`oracle.batchsize`, 0)
    )

    val procedureConfig = ProcedureConfig(
      `procedure.query` = jobConf.get(`procedure.query`),
      `procedure.params`  = jobConf.get(`procedure.params`)
    )

    Arguments(
      jobConf.get(`export.inputpath`),
      jobConf.get(`export.outputpath`),
      jobConf.getInt(`export.maps`, 0),
      jobConf.get(`export.date.start`),
      jobConf.get(`export.date.end`),
      hiveConfig,
      oracleConfig,
      procedureConfig,
      Map[String, Any]()
    )
  }
}
