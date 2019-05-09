package es.hablapps.export.arg

import es.hablapps.export.utils.Constants.{`hive.auth`, `hive.params`, `hive.port`, `hive.principal`, `hive.query`, `hive.server`, `oracle.batchsize`, `oracle.password`, `oracle.port`, `oracle.query`, `oracle.server`, `oracle.serviceName`, `oracle.user`, `procedure.params`, `procedure.query`, _}
import org.apache.hadoop.conf.Configuration

object Parameters {

  def setArguments[A <: Configuration](args: Arguments)(jobConf: A): A = {

    jobConf.set(`export.inputpath`, args.inputPath)
    jobConf.set(`export.outputpath`, args.outputPath)
    jobConf.setInt(`export.maps`, args.maps)

    jobConf.set(`hive.server`, args.hiveConfig.server)
    jobConf.setInt(`hive.port`, args.hiveConfig.port)
    jobConf.set(`hive.principal`, args.hiveConfig.principal)
    jobConf.set(`hive.query`, args.hiveConfig.query)
    jobConf.set(`hive.auth`, args.hiveConfig.auth)
    jobConf.set(`hive.params`, args.hiveConfig.params)

    jobConf.set(`oracle.server`, args.oracleConfig.server)
    jobConf.setInt(`oracle.port`, args.oracleConfig.port)
    jobConf.set(`oracle.user`, args.oracleConfig.user)
    jobConf.set(`oracle.password`, args.oracleConfig.password)
    jobConf.set(`oracle.serviceName`, args.oracleConfig.serviceName)
    jobConf.set(`oracle.query`, args.oracleConfig.query)
    jobConf.setInt(`oracle.batchsize`, args.oracleConfig.batchsize)

    jobConf.set(`procedure.query`, args.procedureConfig.query)
    jobConf.set(`procedure.params`, args.procedureConfig.params)

    jobConf
  }

  def getArguments[A <: Configuration](jobConf: A): Arguments = {

    val hiveConfig = HiveConfig(
      server = jobConf.get(`hive.server`),
      port = jobConf.getInt(`hive.port`, 0),
      principal = jobConf.get(`hive.principal`),
      auth = jobConf.get(`hive.auth`),
      query = jobConf.get(`hive.query`),
      params = jobConf.get(`hive.params`)
    )

    val oracleConfig = OracleConfig(
      server = jobConf.get(`oracle.server`),
      port = jobConf.getInt(`oracle.port`, 0),
      user = jobConf.get(`oracle.user`),
      password = jobConf.get(`oracle.password`),
      serviceName = jobConf.get(`oracle.serviceName`),
      query = jobConf.get(`oracle.query`),
      batchsize = jobConf.getInt(`oracle.batchsize`, 0)
    )

    val procedureConfig = ProcedureConfig(
      query = jobConf.get(`procedure.query`),
      params  = jobConf.get(`procedure.params`)
    )

    Arguments(
      jobConf.get(`export.inputpath`),
      jobConf.get(`export.outputpath`),
      jobConf.getInt(`export.maps`, 0),
      hiveConfig,
      oracleConfig,
      procedureConfig
    )
  }
}
